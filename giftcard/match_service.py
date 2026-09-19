"""
match_service.py -- Motor de reclamacion "la maquina no despacho".

Verifica contra ePay que exista una VENTA real (detalle de ventas por
transaccion) que coincida con lo que el usuario reclama: misma maquina,
mismo producto (MDB de la seleccion), dentro de una ventana de tiempo, y
con claim unico (una venta solo se canjea UNA vez).

Una venta se considera valida solo si:
  * su codigo MDB de producto coincide con el reclamado,
  * su timestamp cae dentro de [ahora - ventana, ahora],
  * aun no ha sido reclamada por otro claim (tx_key UNIQUE),
  * si el usuario confirmo un monto, el monto real de la venta coincide.

Almacen: SQLite local por defecto (prototipo). Para produccion, la
migracion `supabase/10_giftcard_claims.sql` replica esta tabla en `vendu`;
el conector se puede cambiar sin tocar la logica.
"""

from __future__ import annotations

import os
import secrets
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

# ------------------------------------------------------------------ config

DB_PATH = os.getenv("CLAIMS_DB", "giftcard_claims.db")
VENTANA_MIN = int(os.getenv("RECLAMO_VENTANA_MIN", "10"))
POLL_S = int(os.getenv("RECLAMO_POLL_S", "10"))
TIMEOUT_S = int(os.getenv("RECLAMO_TIMEOUT_S", "720"))
MAX_POR_DISPOSITIVO = int(os.getenv("RECLAMOS_POR_DISPOSITIVO", "3"))
MAX_POR_IP = int(os.getenv("RECLAMOS_POR_IP", "5"))
GIFTCARD_MAX = int(os.getenv("GIFTCARD_MAX_BS", "1000000"))

TERMINALES = {"EMITIDO", "NO_ENCONTRADO", "ERROR_GIFTCARD", "AMBIGUO", "BLOQUEADO"}

_DB_LOCK = threading.Lock()

_SCHEMA = """
CREATE TABLE IF NOT EXISTS claims (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    maquina_id  INTEGER NOT NULL,
    mdb         TEXT    NOT NULL,
    producto    TEXT    NOT NULL,
    monto       REAL,
    monto_real  REAL,
    device_id   TEXT    NOT NULL,
    ip          TEXT,
    tx_key      TEXT UNIQUE,
    state       TEXT    NOT NULL DEFAULT 'PENDIENTE',
    gift_code   TEXT,
    mensaje     TEXT,
    created_at  TEXT    NOT NULL,
    updated_at  TEXT
);
CREATE INDEX IF NOT EXISTS idx_claims_maquina   ON claims(maquina_id, created_at);
CREATE INDEX IF NOT EXISTS idx_claims_estado    ON claims(state);
CREATE TABLE IF NOT EXISTS daily_limits (
    scope   TEXT NOT NULL,
    dia     TEXT NOT NULL,
    count   INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (scope, dia)
);
"""


def _con() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def init_db() -> None:
    with _DB_LOCK:
        con = _con()
        con.executescript(_SCHEMA)
        con.commit()
        con.close()


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ------------------------------------------------------------------ limites

def _cuenta(scope: str, dia: str) -> int:
    con = _con()
    row = con.execute(
        "SELECT count FROM daily_limits WHERE scope=? AND dia=?",
        (scope, dia),
    ).fetchone()
    con.close()
    return row["count"] if row else 0


def check_limites(device_id: str, ip: str) -> Optional[str]:
    dia = _now().strftime("%Y-%m-%d")
    d = _cuenta(f"dev:{device_id}", dia)
    if d >= MAX_POR_DISPOSITIVO:
        return f"Limite diario alcanzado para este dispositivo ({MAX_POR_DISPOSITIVO}/dia)."
    if ip:
        i = _cuenta(f"ip:{ip}", dia)
        if i >= MAX_POR_IP:
            return f"Demasiados reclamos desde esta conexion ({MAX_POR_IP}/dia)."
    con = _con()
    active = con.execute(
        "SELECT 1 FROM claims WHERE device_id=? AND state='PENDIENTE' LIMIT 1",
        (device_id,),
    ).fetchone()
    con.close()
    if active:
        return "Ya tienes un reclamo en curso. Termina o espera el resultado."
    return None


def _incr(scope: str, dia: str) -> None:
    con = _con()
    con.execute(
        "INSERT INTO daily_limits(scope, dia, count) VALUES(?,?,1) "
        "ON CONFLICT(scope, dia) DO UPDATE SET count = count + 1",
        (scope, dia),
    )
    con.commit()
    con.close()


# ------------------------------------------------------------------ claims

def crear_reclamo(
    maquina_id: int,
    mdb: str,
    producto: str,
    monto: Optional[float],
    device_id: str,
    ip: str,
) -> Dict[str, Any]:
    bloqueo = check_limites(device_id, ip)
    if bloqueo:
        return {"ok": False, "error": bloqueo}
    with _DB_LOCK:
        con = _con()
        cur = con.execute(
            "INSERT INTO claims(maquina_id, mdb, producto, monto, device_id, ip, created_at) "
            "VALUES(?,?,?,?,?,?,?)",
            (maquina_id, mdb, producto, monto, device_id, ip, _now().isoformat()),
        )
        claim_id = cur.lastrowid
        con.commit()
        con.close()
    dia = _now().strftime("%Y-%m-%d")
    _incr(f"dev:{device_id}", dia)
    if ip:
        _incr(f"ip:{ip}", dia)
    return {"ok": True, "claim_id": claim_id}


def estado_reclamo(claim_id: int) -> Optional[Dict[str, Any]]:
    con = _con()
    row = con.execute("SELECT * FROM claims WHERE id=?", (claim_id,)).fetchone()
    con.close()
    if not row:
        return None
    d = dict(row)
    d["terminal"] = d["state"] in TERMINALES
    return d


def cancelar_pendiente_tardio(claim_id: int) -> bool:
    """Marca NO_ENCONTRADO si el claim sigue PENDIENTE y vencio el limite."""
    con = _con()
    cur = con.execute(
        "UPDATE claims SET state='NO_ENCONTRADO', mensaje='No se encontro la venta en "
        "este periodo. Intenta de nuevo o habla con un operador.', updated_at=? "
        "WHERE id=? AND state='PENDIENTE'",
        (_now().isoformat(), claim_id),
    )
    con.commit()
    ok = cur.rowcount == 1
    con.close()
    return ok


# ------------------------------------------------------------------ epay

class MCPSync:
    """Singleton EpayMCP con locks (el cliente MCP no es thread-safe)."""

    _init_lock = threading.Lock()
    _call_lock = threading.Lock()
    _mcp: Optional[Any] = None
    _cached_planos: Dict[int, tuple] = {}

    PLANO_TTL_S = 60

    @classmethod
    def _conectar(cls):
        with cls._init_lock:
            if cls._mcp is None:
                import sys
                import logging

                logging.disable(logging.WARNING)
                root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                if root not in sys.path:
                    sys.path.insert(0, root)
                from epay_mcp import EpayMCP

                cls._mcp = EpayMCP().connect()
            return cls._mcp

    @classmethod
    def call(cls, tool: str, args: Dict[str, Any]):
        mcp = cls._conectar()
        with cls._call_lock:
            return mcp.call_tool(tool, args)

    @classmethod
    def planograma(cls, maquina_id: int):
        clave = maquina_id
        ahora = _now().timestamp()
        entrada = cls._cached_planos.get(clave)
        if entrada and entrada[0] + cls.PLANO_TTL_S > ahora:
            return entrada[1]
        mcp = cls._conectar()
        with cls._call_lock:
            pg = mcp.planograma(maquina_id)
        cls._cached_planos[clave] = (_now().timestamp(), pg)
        return pg


def _mdb_de_referencia(ref: str) -> str:
    return (ref or "").split()[0].strip().lower() if (ref or "").split() else ""


def _ventas_ventana(maquina_id: int, desde: datetime, hasta: datetime) -> List[Dict[str, Any]]:
    """Detalle de ventas (transacciones) en la ventana, de ePay."""
    salida: List[Dict[str, Any]] = []
    dia = desde
    while dia.date() <= hasta.date():
        f = dia.date().isoformat()
        try:
            r = MCPSync.call(
                "ventas_x_rango",
                {"fecha_desde": f, "fecha_hasta": f, "maquina_id": maquina_id,
                 "incluir_detalle": True},
            )
        except Exception:
            r = {}
        for row in (r or {}).get("detalle") or []:
            crudo = (row.get("Fecha / hora") or "").strip()
            try:
                dt = datetime.strptime(crudo, "%d-%m-%Y %H:%M").replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            if dt < desde or dt > hasta:
                continue
            monto = _float(row.get("Monto"))
            salida.append({
                "dt": dt,
                "mdb": _mdb_de_referencia(row.get("Referencia") or ""),
                "producto": " ".join((row.get("Referencia") or "").split()[1:]),
                "monto": monto,
            })
        dia += timedelta(days=1)
    salida.sort(key=lambda v: v["dt"])
    return salida


def _float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(str(v).strip().replace(",", ""))
    except ValueError:
        return None


def candidatas(claim_id: int) -> List[Dict[str, Any]]:
    """Ventas de ePay sin reclamar que encajan con el claim (producto + ventana)."""
    con = _con()
    row = con.execute("SELECT * FROM claims WHERE id=?", (claim_id,)).fetchone()
    con.close()
    if not row or row["state"] != "PENDIENTE":
        return []
    mdb = (row["mdb"] or "").strip().lower()
    monto_obj = _float(row["monto"])
    ahora = _now()
    desde = ahora - timedelta(minutes=VENTANA_MIN)
    ventas = [v for v in _ventas_ventana(row["maquina_id"], desde, ahora)
              if v["mdb"] == mdb]
    reservadas = set()
    con = _con()
    for r in con.execute("SELECT tx_key FROM claims WHERE tx_key IS NOT NULL"):
        reservadas.add(r["tx_key"])
    con.close()
    cand = [v for v in ventas if _txkey(row["maquina_id"], v) not in reservadas]
    if monto_obj is not None:
        cand = [v for v in cand if v["monto"] is not None and abs(v["monto"] - monto_obj) < 0.01]
    return cand


def _txkey(maquina_id: int, venta: Dict[str, Any]) -> str:
    return f"{maquina_id}|{venta['dt']:%Y-%m-%d %H:%M}|{venta['mdb']}|{venta['monto']:.2f}"


def estado_ambiguo(claim_id: int) -> None:
    con = _con()
    con.execute(
        "UPDATE claims SET state='AMBIGUO', mensaje='Hay varias compras del mismo producto "
        "en este periodo y no se pudo confirmar cual es la tuya. Habla con un operador.', "
        "updated_at=? WHERE id=?",
        (_now().isoformat(), claim_id),
    )
    con.commit()
    con.close()


def intento_match(claim_id: int) -> str:
    """Un paso de polling: intenta reservar una venta exacta y emitir la gift card.

    Devuelve el estado resultante: PENDIENTE | EMITIDO | NO_ENCONTRADO |
    ERROR_GIFTCARD | AMBIGUO.
    """
    con = _con()
    row = con.execute("SELECT * FROM claims WHERE id=?", (claim_id,)).fetchone()
    con.close()
    if not row or row["state"] != "PENDIENTE":
        return (row["state"] if row else "NO_ENCONTRADO")

    ahora = _now()
    if (ahora - datetime.fromisoformat(row["created_at"])).total_seconds() > TIMEOUT_S:
        cancelar_pendiente_tardio(claim_id)
        return "NO_ENCONTRADO"

    ventas = candidatas(claim_id)
    if len(ventas) > 1:
        estado_ambiguo(claim_id)
        return "AMBIGUO"
    if not ventas:
        return "PENDIENTE"

    v = ventas[0]
    key = _txkey(row["maquina_id"], v)

    from .giftcard_client import emitir_giftcard

    # Reservar la venta con un UPDATE atomico; si otra claim gano la carrera,
    # sqlite eleva IntegrityError por tx_key UNIQUE y se intenta la siguiente.
    con = _con()
    cur = con.execute(
        "UPDATE claims SET tx_key=?, updated_at=? WHERE id=? AND tx_key IS NULL",
        (key, _now().isoformat(), claim_id),
    )
    con.commit()
    reservo = cur.rowcount == 1
    if not reservo:
        return "PENDIENTE"

    try:
        codigo = emitir_giftcard({
            "referencia": key,
            "maquina_id": row["maquina_id"],
            "mdb": row["mdb"],
            "producto": v["producto"] or row["producto"],
            "monto": v["monto"],
        })
    except Exception as e:
        con = _con()
        con.execute(
            "UPDATE claims SET state='ERROR_GIFTCARD', mensaje='La compra se confirmo pero "
            "fallo la generacion del codigo. Habla con un operador.', updated_at=? "
            "WHERE id=?",
            (_now().isoformat(), claim_id),
        )
        con.commit()
        con.close()
        return "ERROR_GIFTCARD"

    con = _con()
    con.execute(
        "UPDATE claims SET state='EMITIDO', gift_code=?, monto_real=?, mensaje=?, "
        "updated_at=? WHERE id=?",
        (codigo, v["monto"], "Compra confirmada en la maquina. Presenta este codigo.", _now().isoformat(), claim_id),
    )
    con.commit()
    con.close()
    return "EMITIDO"


init_db()