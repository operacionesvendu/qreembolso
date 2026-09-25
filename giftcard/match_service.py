"""
match_service.py -- Motor de reclamacion "la maquina no despacho".

Verifica contra ePay que exista un COBRO real sin despacho (cobros_maquina
por maquina) que coincida con lo que el usuario reclama: misma maquina,
mismo producto (MDB de la seleccion), dentro de una ventana de tiempo, y
con claim unico (un cobro solo se canjea UNA vez).

Un cobro se considera valido solo si:
  * ePay lo marca con un estado reembolsable (RECLAMO_ESTADOS, por defecto
    solo `sin_despacho`: se cobro y la maquina no entrego nada),
  * su codigo MDB de producto coincide con el reclamado,
  * su timestamp cae dentro de [creacion del reclamo - ventana, ahora],
  * aun no ha sido reclamado por otro claim (tx_key UNIQUE),
  * si el usuario confirmo un monto, el monto real del cobro coincide.

Almacen:
  * DATABASE_URL (postgres://...) -> tablas `vendu.giftcard_*` de
    `supabase/10_giftcard_claims.sql` (PRODUCCION: el disco de Railway es
    efimero y con SQLite se pierden los claims en cada deploy).
  * Sin DATABASE_URL -> SQLite local en CLAIMS_DB (desarrollo/pruebas).
"""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List, Optional

log = logging.getLogger("match_service")

# ------------------------------------------------------------------ config

DB_PATH = os.getenv("CLAIMS_DB", "giftcard_claims.db")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
USE_PG = DATABASE_URL.startswith(("postgres://", "postgresql://"))
PG_SCHEMA = os.getenv("CLAIMS_SCHEMA", "vendu")

VENTANA_MIN = int(os.getenv("RECLAMO_VENTANA_MIN", "10"))
POLL_S = int(os.getenv("RECLAMO_POLL_S", "10"))
TIMEOUT_S = int(os.getenv("RECLAMO_TIMEOUT_S", "720"))
MAX_POR_DISPOSITIVO = int(os.getenv("RECLAMOS_POR_DISPOSITIVO", "3"))
MAX_POR_IP = int(os.getenv("RECLAMOS_POR_IP", "5"))
GIFTCARD_MAX_BS = float(os.getenv("GIFTCARD_MAX_BS", "10000"))
GIFTCARD_MAX_BS_IP = float(os.getenv("GIFTCARD_MAX_BS_IP", "25000"))
ESTADOS_REEMBOLSABLES = {
    e.strip().lower()
    for e in os.getenv("RECLAMO_ESTADOS", "sin_despacho").split(",")
    if e.strip()
}

TERMINALES = {"EMITIDO", "NO_ENCONTRADO", "ERROR_GIFTCARD", "AMBIGUO", "BLOQUEADO", "CANCELADO"}

if USE_PG:
    T_CLAIMS = f"{PG_SCHEMA}.giftcard_claims"
    T_LIMITS = f"{PG_SCHEMA}.giftcard_limits"
    T_EMISIONES = f"{PG_SCHEMA}.giftcard_emisiones"
else:
    T_CLAIMS, T_LIMITS, T_EMISIONES = "claims", "daily_limits", "emisiones"

_DB_LOCK = threading.Lock()

_SCHEMA_SQLITE = """
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
CREATE INDEX IF NOT EXISTS idx_claims_device    ON claims(device_id, state);
CREATE TABLE IF NOT EXISTS daily_limits (
    scope   TEXT NOT NULL,
    dia     TEXT NOT NULL,
    count   INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (scope, dia)
);
CREATE TABLE IF NOT EXISTS emisiones (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    claim_id    INTEGER REFERENCES claims(id),
    codigo      TEXT UNIQUE NOT NULL,
    monto_bs    REAL,
    contexto    TEXT,
    emitida_en  TEXT NOT NULL
);
"""


# ------------------------------------------------------------------ db

if USE_PG:
    import psycopg
    from psycopg.rows import dict_row

    _INTEGRITY: tuple = (psycopg.IntegrityError,)
else:
    _INTEGRITY = (sqlite3.IntegrityError,)


def _q(sql: str) -> str:
    """Las consultas se escriben con `?` (sqlite); psycopg usa `%s`."""
    return sql.replace("?", "%s") if USE_PG else sql


@contextmanager
def _db() -> Iterator[Any]:
    if USE_PG:
        con = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    else:
        con = sqlite3.connect(DB_PATH, timeout=10)
        con.row_factory = sqlite3.Row
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def _one(sql: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
    with _db() as con:
        row = con.execute(_q(sql), params).fetchone()
    return dict(row) if row else None


def _all(sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
    with _db() as con:
        return [dict(r) for r in con.execute(_q(sql), params).fetchall()]


def _run(sql: str, params: tuple = ()) -> int:
    with _db() as con:
        return con.execute(_q(sql), params).rowcount


def init_db() -> None:
    if USE_PG:
        # En Postgres el esquema lo crea la migracion (supabase/10_...sql).
        _one(f"SELECT 1 FROM {T_CLAIMS} LIMIT 1")
        return
    with _DB_LOCK:
        con = sqlite3.connect(DB_PATH)
        con.executescript(_SCHEMA_SQLITE)
        con.commit()
        con.close()


def backend() -> str:
    return "postgres" if USE_PG else "sqlite"


# ------------------------------------------------------------------ tiempo

def _zona_caracas():
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo("America/Caracas")
    except Exception:
        return timezone(-timedelta(hours=4))


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _ts(dt: datetime) -> Any:
    """Valor a guardar: datetime en Postgres (timestamptz), ISO fijo en SQLite."""
    return dt if USE_PG else dt.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _as_dt(v: Any) -> datetime:
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    dt = datetime.fromisoformat(str(v))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _dia() -> str:
    """Dia operativo en hora de Venezuela (los topes se reinician a medianoche local)."""
    return _now().astimezone(_zona_caracas()).strftime("%Y-%m-%d")


def _rango_dia_utc() -> tuple:
    caracas = _zona_caracas()
    local = _now().astimezone(caracas)
    inicio = local.replace(hour=0, minute=0, second=0, microsecond=0)
    return inicio.astimezone(timezone.utc), (inicio + timedelta(days=1)).astimezone(timezone.utc)


# ------------------------------------------------------------------ limites

def _cuenta(scope: str, dia: str) -> int:
    row = _one(f"SELECT count FROM {T_LIMITS} WHERE scope=? AND dia=?", (scope, dia))
    return int(row["count"]) if row else 0


def check_limites(device_id: str, ip: str) -> Optional[str]:
    dia = _dia()
    if _cuenta(f"dev:{device_id}", dia) >= MAX_POR_DISPOSITIVO:
        return f"Limite diario alcanzado para este dispositivo ({MAX_POR_DISPOSITIVO}/dia)."
    if ip and _cuenta(f"ip:{ip}", dia) >= MAX_POR_IP:
        return f"Demasiados reclamos desde esta conexion ({MAX_POR_IP}/dia)."
    activo = _one(
        f"SELECT id FROM {T_CLAIMS} WHERE device_id=? AND state='PENDIENTE' LIMIT 1",
        (device_id,),
    )
    if activo:
        return "Ya tienes un reclamo en curso. Termina o espera el resultado."
    return None


def _incr(scope: str, dia: str) -> None:
    _run(
        f"INSERT INTO {T_LIMITS} AS l (scope, dia, count) VALUES(?,?,1) "
        "ON CONFLICT(scope, dia) DO UPDATE SET count = l.count + 1",
        (scope, dia),
    )


# ------------------------------------------------------------------ tope Bs

def _suma_emitida(scope_col: str, valor: str) -> float:
    assert scope_col in ("device_id", "ip")
    desde, hasta = _rango_dia_utc()
    row = _one(
        f"SELECT COALESCE(SUM(monto_real),0) AS s FROM {T_CLAIMS} "
        f"WHERE state='EMITIDO' AND {scope_col}=? AND created_at >= ? AND created_at < ?",
        (valor, _ts(desde), _ts(hasta)),
    )
    return float(row["s"] or 0) if row else 0.0


def _tope_error(device_id: str, ip: str, monto: Optional[float]) -> Optional[str]:
    """Revisa el tope Bs/dia POR USUARIO y POR IP antes de emitir.

    Un valor <= 0 en la variable desactiva el tope. La tarjeta solo se emite
    si lo ya emitido hoy + este monto no supera el tope.
    """
    monto = monto or 0.0
    if device_id and GIFTCARD_MAX_BS > 0:
        if _suma_emitida("device_id", device_id) + monto > GIFTCARD_MAX_BS:
            return (f"Alcanzaste el limite de reembolsos del dia "
                    f"({GIFTCARD_MAX_BS:.2f} Bs/usuario). Intenta manana.")
    if ip and GIFTCARD_MAX_BS_IP > 0:
        if _suma_emitida("ip", ip) + monto > GIFTCARD_MAX_BS_IP:
            return "Limite de reembolsos del dia alcanzado desde esta conexion."
    return None


# ------------------------------------------------------------------ claims

def crear_reclamo(
    maquina_id: int,
    mdb: str,
    producto: str,
    monto: Optional[float],
    device_id: str,
    ip: str,
) -> Dict[str, Any]:
    with _DB_LOCK:
        bloqueo = check_limites(device_id, ip)
        if bloqueo:
            return {"ok": False, "error": bloqueo}
        row = _one(
            f"INSERT INTO {T_CLAIMS}(maquina_id, mdb, producto, monto, device_id, ip, created_at) "
            "VALUES(?,?,?,?,?,?,?) RETURNING id",
            (maquina_id, mdb.strip().lower(), producto, monto, device_id, ip, _ts(_now())),
        )
        claim_id = int(row["id"])
        dia = _dia()
        _incr(f"dev:{device_id}", dia)
        if ip:
            _incr(f"ip:{ip}", dia)
    return {"ok": True, "claim_id": claim_id}


def _fila(claim_id: int) -> Optional[Dict[str, Any]]:
    return _one(f"SELECT * FROM {T_CLAIMS} WHERE id=?", (claim_id,))


def estado_reclamo(claim_id: int) -> Optional[Dict[str, Any]]:
    d = _fila(claim_id)
    if not d:
        return None
    d["terminal"] = d["state"] in TERMINALES
    if d.get("monto_real") is not None:
        d["monto_real"] = float(d["monto_real"])
    return d


def pendientes() -> List[int]:
    """Ids de claims PENDIENTE (para reanudar el polling tras un reinicio)."""
    return [int(r["id"]) for r in _all(f"SELECT id FROM {T_CLAIMS} WHERE state='PENDIENTE'")]


def cancelar_pendiente_tardio(claim_id: int) -> bool:
    """Marca NO_ENCONTRADO si el claim sigue PENDIENTE (sin venta reservada)."""
    return _run(
        f"UPDATE {T_CLAIMS} SET state='NO_ENCONTRADO', mensaje='No se encontro la compra en "
        "este periodo. Intenta de nuevo o habla con un operador.', updated_at=? "
        "WHERE id=? AND state='PENDIENTE' AND tx_key IS NULL",
        (_ts(_now()), claim_id),
    ) == 1


def cancelar_por_usuario(claim_id: int, device_id: str) -> bool:
    """El usuario abandona la busqueda. Solo si aun no se reservo una venta."""
    return _run(
        f"UPDATE {T_CLAIMS} SET state='CANCELADO', mensaje='Reclamo cancelado.', updated_at=? "
        "WHERE id=? AND device_id=? AND state='PENDIENTE' AND tx_key IS NULL",
        (_ts(_now()), claim_id, device_id),
    ) == 1


def listar(estado: Optional[str] = None, limite: int = 100) -> List[Dict[str, Any]]:
    limite = max(1, min(int(limite), 1000))
    if estado:
        rows = _all(
            f"SELECT * FROM {T_CLAIMS} WHERE state=? ORDER BY id DESC LIMIT {limite}",
            (estado.upper(),),
        )
    else:
        rows = _all(f"SELECT * FROM {T_CLAIMS} ORDER BY id DESC LIMIT {limite}")
    for r in rows:
        for k in ("created_at", "updated_at"):
            if isinstance(r.get(k), datetime):
                r[k] = r[k].isoformat()
        for k in ("monto", "monto_real"):
            if r.get(k) is not None:
                r[k] = float(r[k])
    return rows


def _marcar(claim_id: int, state: str, mensaje: str, **extra: Any) -> None:
    sets = ["state=?", "mensaje=?", "updated_at=?"]
    params: List[Any] = [state, mensaje, _ts(_now())]
    for col, val in extra.items():
        assert col in ("gift_code", "monto_real")
        sets.append(f"{col}=?")
        params.append(val)
    params.append(claim_id)
    _run(f"UPDATE {T_CLAIMS} SET {', '.join(sets)} WHERE id=? AND state='PENDIENTE'", tuple(params))


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


def _parse_ventana_dt(cadena: str) -> Optional[datetime]:
    try:
        local = datetime.strptime(cadena, "%Y-%m-%d %H:%M:%S").replace(tzinfo=_zona_caracas())
        return local.astimezone(timezone.utc)
    except (ValueError, TypeError):
        return None


def _mdb_de_referencia(ref: str) -> str:
    partes = (ref or "").split()
    return partes[0].strip().lower() if partes else ""


def _float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(str(v).strip().replace(",", ""))
    except ValueError:
        return None


def _ventas_ventana(maquina_id: int, desde: datetime, hasta: datetime) -> List[Dict[str, Any]]:
    """Cobros de la maquina en la ventana, via cobros_maquina.

    El MCP reporta los timestamps en hora Venezuela (UTC-4); aqui se convierten
    a UTC para compararlos con el reloj del bot. Cada cobro trae `estado`
    (despachado / parcial / sin_despacho / sin_tid).
    """
    caracas = _zona_caracas()
    salida: List[Dict[str, Any]] = []
    dj = desde.astimezone(caracas).date()
    fin = hasta.astimezone(caracas).date()
    while dj <= fin:
        f = dj.isoformat()
        try:
            r = MCPSync.call(
                "cobros_maquina",
                {"maquina_id": str(maquina_id), "fecha_desde": f, "fecha_hasta": f},
            )
        except Exception as e:
            log.warning("cobros_maquina(%s, %s) fallo: %s", maquina_id, f, e)
            r = {}
        for fila in (r or {}).get("cobros") or []:
            dt = _parse_ventana_dt(fila.get("fecha") or "")
            if not dt or dt < desde or dt > hasta:
                continue
            salida.append({
                "dt": dt,
                "mdb": _mdb_de_referencia(fila.get("respuesta") or fila.get("seleccion") or ""),
                "producto": "",
                "monto": _float(fila.get("monto")),
                "estado": str(fila.get("estado") or "").strip().lower(),
            })
        dj += timedelta(days=1)
    salida.sort(key=lambda v: v["dt"])
    return salida


def _txkey(maquina_id: int, venta: Dict[str, Any]) -> str:
    return f"{maquina_id}|{venta['dt']:%Y-%m-%d %H:%M:%S}|{venta['mdb']}|{venta['monto']:.2f}"


def candidatas(claim_id: int) -> List[Dict[str, Any]]:
    """Cobros sin despacho y sin reclamar que encajan con el claim (producto + ventana)."""
    row = _fila(claim_id)
    if not row or row["state"] != "PENDIENTE":
        return []
    mdb = (row["mdb"] or "").strip().lower()
    monto_obj = _float(row["monto"])
    desde = _as_dt(row["created_at"]) - timedelta(minutes=VENTANA_MIN)
    ventas = [
        v for v in _ventas_ventana(int(row["maquina_id"]), desde, _now())
        if v["mdb"] == mdb and v["estado"] in ESTADOS_REEMBOLSABLES and v["monto"] is not None
    ]
    if monto_obj is not None:
        ventas = [v for v in ventas if abs(v["monto"] - monto_obj) < 0.01]
    if not ventas:
        return []
    claves = [_txkey(int(row["maquina_id"]), v) for v in ventas]
    marcas = ",".join("?" for _ in claves)
    reservadas = {
        r["tx_key"]
        for r in _all(f"SELECT tx_key FROM {T_CLAIMS} WHERE tx_key IN ({marcas})", tuple(claves))
    }
    return [v for v, k in zip(ventas, claves) if k not in reservadas]


def _registrar_emision(claim_id: int, codigo: str, monto: float, contexto: Dict[str, Any]) -> None:
    import json

    try:
        _run(
            f"INSERT INTO {T_EMISIONES}(claim_id, codigo, monto_bs, contexto, emitida_en) "
            "VALUES(?,?,?,?,?)",
            (claim_id, codigo, monto, json.dumps(contexto, ensure_ascii=False, default=str), _ts(_now())),
        )
    except Exception as e:  # la bitacora no debe tumbar una emision ya hecha
        log.error("No se pudo registrar la emision del claim %s: %s", claim_id, e)


def intento_match(claim_id: int) -> str:
    """Un paso de polling: intenta reservar un cobro exacto y emitir la gift card.

    Devuelve el estado resultante: PENDIENTE | EMITIDO | NO_ENCONTRADO |
    ERROR_GIFTCARD | AMBIGUO | BLOQUEADO | CANCELADO.
    """
    row = _fila(claim_id)
    if not row or row["state"] != "PENDIENTE":
        return row["state"] if row else "NO_ENCONTRADO"

    if row["tx_key"]:
        # Se reservo un cobro pero el proceso murio antes de terminar la emision:
        # no se sabe si la tarjeta llego a crearse. Lo revisa un operador.
        _marcar(claim_id, "ERROR_GIFTCARD",
                "La compra se confirmo pero la emision quedo interrumpida. Habla con un operador.")
        return "ERROR_GIFTCARD"

    if (_now() - _as_dt(row["created_at"])).total_seconds() > TIMEOUT_S:
        cancelar_pendiente_tardio(claim_id)
        return "NO_ENCONTRADO"

    ventas = candidatas(claim_id)
    if len(ventas) > 1:
        _marcar(claim_id, "AMBIGUO",
                "Hay varias compras del mismo producto en este periodo y no se pudo "
                "confirmar cual es la tuya. Habla con un operador.")
        return "AMBIGUO"
    if not ventas:
        return "PENDIENTE"

    v = ventas[0]
    key = _txkey(int(row["maquina_id"]), v)

    # Reserva atomica: si otro claim gano la carrera, tx_key UNIQUE lo impide.
    try:
        reservo = _run(
            f"UPDATE {T_CLAIMS} SET tx_key=?, updated_at=? "
            "WHERE id=? AND tx_key IS NULL AND state='PENDIENTE'",
            (key, _ts(_now()), claim_id),
        ) == 1
    except _INTEGRITY:
        reservo = False
    if not reservo:
        return "PENDIENTE"

    err_tope = _tope_error(row["device_id"], row["ip"], v["monto"])
    if err_tope:
        _marcar(claim_id, "BLOQUEADO", err_tope)
        return "BLOQUEADO"

    from .giftcard_client import emitir_giftcard

    contexto = {
        "referencia": key,
        "claim_id": claim_id,
        "maquina_id": row["maquina_id"],
        "mdb": row["mdb"],
        "producto": v["producto"] or row["producto"],
        "monto": v["monto"],
    }
    try:
        codigo = emitir_giftcard(contexto)
    except Exception as e:
        log.error("Emision fallida para claim %s: %s", claim_id, e)
        _marcar(claim_id, "ERROR_GIFTCARD",
                "La compra se confirmo pero fallo la generacion del codigo. Habla con un operador.")
        return "ERROR_GIFTCARD"

    _marcar(claim_id, "EMITIDO", "Compra confirmada en la maquina. Presenta este codigo.",
            gift_code=codigo, monto_real=v["monto"])
    _registrar_emision(claim_id, codigo, v["monto"], contexto)
    log.info("Claim %s EMITIDO (%s, %.2f Bs)", claim_id, key, v["monto"])
    return "EMITIDO"


init_db()
