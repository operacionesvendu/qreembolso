"""
match_service.py -- Motor de reclamacion "la maquina no despacho".

Verifica contra ePay que exista un COBRO real (cobros_maquina por maquina)
que coincida con lo que el usuario reclama: misma maquina, mismo monto que
los productos seleccionados, dentro de una ventana de tiempo, y con claim
unico (un cobro solo se canjea UNA vez).

Un cobro se considera valido solo si:
  * su monto es igual a la suma de precios (planograma) de los productos
    seleccionados (+- RECLAMO_TOLERANCIA_BS),
  * si se reclama UN solo producto, el MDB del cobro es uno de los slots
    donde esta ese producto
    (con varios productos -compra de carrito- solo se exige el monto),
  * su timestamp cae dentro de [creacion del reclamo - ventana, ahora],
  * aun no ha sido reclamado por otro claim (tx_key UNIQUE),
  * su `estado` esta en RECLAMO_ESTADOS. Por defecto `*` = cualquiera
    (despachado incluido): politica temporal para bajar la carga de
    reembolsos manuales. El estado queda guardado en `estado_cobro` para
    auditar despues cuantas tarjetas se dieron por compras despachadas.

Almacen:
  * DATABASE_URL (postgres://...) -> tablas `vendu.giftcard_*` de
    `supabase/10_giftcard_claims.sql` (PRODUCCION: el disco de Railway es
    efimero y con SQLite se pierden los claims en cada deploy).
  * Sin DATABASE_URL -> SQLite local en CLAIMS_DB (desarrollo/pruebas).
"""

from __future__ import annotations

import json
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
    for e in os.getenv("RECLAMO_ESTADOS", "*").split(",")
    if e.strip()
}
TOLERANCIA_BS = float(os.getenv("RECLAMO_TOLERANCIA_BS", "0.01"))
MAX_ITEMS = int(os.getenv("RECLAMO_MAX_ITEMS", "10"))
# Prefijos de código de las máquinas que aparecen en el selector de /reclamo.
PREFIJOS_SELECTOR = [p.strip().upper() for p in os.getenv("RECLAMO_PREFIJOS", "V").split(",") if p.strip()]
# Loguea en cada consulta los cobros vistos (hora, mdb, monto, estado) para
# diagnosticar por que un reclamo no coincide. No incluye datos personales.
LOG_COBROS = os.getenv("RECLAMO_LOG_COBROS", "1").strip().lower() in ("1", "true", "si", "yes")

TERMINALES = {"EMITIDO", "NO_ENCONTRADO", "ERROR_GIFTCARD", "AMBIGUO", "BLOQUEADO", "CANCELADO",
              "ENTREGADO"}
# Reembolsar solo lo que la máquina NO entregó (cobrado - despachado según las
# ventas que el MCP cruza con cada cobro). "0" = reembolsar el cobro completo.
SOLO_FALTANTE = os.getenv("RECLAMO_SOLO_FALTANTE", "1").strip().lower() not in ("0", "false", "no")

if USE_PG:
    T_CLAIMS = f"{PG_SCHEMA}.giftcard_claims"
    T_LIMITS = f"{PG_SCHEMA}.giftcard_limits"
    T_EMISIONES = f"{PG_SCHEMA}.giftcard_emisiones"
else:
    T_CLAIMS, T_LIMITS, T_EMISIONES = "claims", "daily_limits", "emisiones"

_DB_LOCK = threading.Lock()
_EMITIENDO: set = set()           # claims con emision en curso en este proceso
_EMITIENDO_LOCK = threading.Lock()
# Una emision via MCP puede tardar hasta el read-timeout del cliente (300 s);
# pasado esto, un claim con cobro reservado y sin EMITIDO se da por interrumpido.
EMISION_MAX_S = int(os.getenv("GIFTCARD_EMISION_MAX_S", "360"))

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
    estado_cobro TEXT,
    items       TEXT,
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
        for col in ("estado_cobro", "items"):  # bases creadas antes de esas columnas
            try:
                con.execute(f"ALTER TABLE claims ADD COLUMN {col} TEXT")
            except sqlite3.OperationalError:
                pass
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
        return f"Llegaste al límite de reclamos de hoy en este teléfono ({MAX_POR_DISPOSITIVO} por día)."
    if ip and _cuenta(f"ip:{ip}", dia) >= MAX_POR_IP:
        return "Hay demasiados reclamos hoy desde esta conexión. Intenta mañana o habla con una persona."
    activo = _one(
        f"SELECT id FROM {T_CLAIMS} WHERE device_id=? AND state='PENDIENTE' LIMIT 1",
        (device_id,),
    )
    if activo:
        return "Ya tienes un reclamo en curso. Espera su resultado."
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
            return "Alcanzaste el límite de reembolsos de hoy. Intenta mañana o habla con una persona."
    if ip and GIFTCARD_MAX_BS_IP > 0:
        if _suma_emitida("ip", ip) + monto > GIFTCARD_MAX_BS_IP:
            return "Se alcanzó el límite de reembolsos de hoy desde esta conexión. Habla con una persona."
    return None


# ------------------------------------------------------------------ claims

def productos_en_stock(maquina_id: int) -> Dict[str, Dict[str, Any]]:
    """Productos con stock de la maquina segun el planograma.

    Un mismo producto suele estar en varios slots (p. ej. F0, F2, F4, F6):
    se agrupan por nombre + precio en una sola entrada, cuya clave es el MDB
    del primer slot y `mdbs` trae todos. La persona no sabe de que slot salio
    (o no salio) su compra, asi que cualquiera de ellos vale.
    """
    pg = MCPSync.planograma(maquina_id)
    vistos: Dict[str, Dict[str, Any]] = {}
    por_producto: Dict[tuple, str] = {}
    for slot in pg.get("slots", []):
        nombre = (slot.get("nombre") or "").strip()
        mdb = (slot.get("seleccion") or "").strip().lower()
        cant = slot.get("cantidad") or 0
        if not nombre or not mdb or cant <= 0:
            continue
        precio = round(slot.get("precio_bs") or 0, 2)
        clave = (" ".join(nombre.lower().split()), precio)
        pid = str(slot.get("producto_id") or "").strip()
        if clave in por_producto:
            item = vistos[por_producto[clave]]
            if mdb not in item["mdbs"]:
                item["mdbs"].append(mdb)
            if pid and pid not in item["producto_ids"]:
                item["producto_ids"].append(pid)
            item["stock"] += int(cant)
            continue
        por_producto[clave] = mdb
        vistos[mdb] = {"mdb": mdb, "mdbs": [mdb], "producto_ids": [pid] if pid else [],
                       "nombre": nombre, "precio_bs": precio, "stock": int(cant)}
    return vistos


def armar_pedido(maquina_id: int, pedido: List[tuple]) -> Dict[str, Any]:
    """[(mdb, cantidad), ...] -> monto esperado (precios del planograma) + descripcion.

    El monto lo calcula el servidor: el usuario no puede escribir un monto
    arbitrario para calzar con el cobro de otra persona.
    """
    cantidades: Dict[str, int] = {}
    for mdb, cant in pedido:
        clave = (mdb or "").strip().lower()
        cantidades[clave] = cantidades.get(clave, 0) + int(cant)
    total_items = sum(cantidades.values())
    if total_items < 1 or any(c < 1 for c in cantidades.values()):
        return {"ok": False, "status": 400, "error": "Elige al menos un producto."}
    if total_items > MAX_ITEMS:
        return {"ok": False, "status": 400, "error": f"Máximo {MAX_ITEMS} productos por reclamo."}
    try:
        stock = productos_en_stock(maquina_id)
    except Exception as e:
        log.warning("planograma(%s) fallo: %s", maquina_id, e)
        return {"ok": False, "status": 502,
                "error": "No pudimos consultar la máquina. Intenta de nuevo en unos segundos."}
    if any(m not in stock or not stock[m]["precio_bs"] for m in cantidades):
        return {"ok": False, "status": 400,
                "error": "Algún producto ya no está disponible en esta máquina. Vuelve a elegir."}
    monto = round(sum(stock[m]["precio_bs"] * c for m, c in cantidades.items()), 2)
    producto = ", ".join(
        stock[m]["nombre"] + (f" x{c}" if c > 1 else "") for m, c in sorted(cantidades.items())
    )
    # mdb guardado: un grupo por producto (",") con todos sus slots ("|")
    mdb = ",".join("|".join(stock[m]["mdbs"]) for m in sorted(cantidades))
    items = [{"nombre": stock[m]["nombre"], "precio_bs": stock[m]["precio_bs"], "cantidad": c,
              "producto_ids": stock[m].get("producto_ids", [])} for m, c in sorted(cantidades.items())]
    return {"ok": True, "mdb": mdb, "producto": producto[:200], "monto": monto, "items": items}


def crear_reclamo(
    maquina_id: int,
    pedido: List[tuple],
    device_id: str,
    ip: str,
) -> Dict[str, Any]:
    bloqueo = check_limites(device_id, ip)
    if bloqueo:
        return {"ok": False, "status": 429, "error": bloqueo}
    armado = armar_pedido(maquina_id, pedido)
    if not armado["ok"]:
        return armado
    with _DB_LOCK:
        bloqueo = check_limites(device_id, ip)
        if bloqueo:
            return {"ok": False, "status": 429, "error": bloqueo}
        row = _one(
            f"INSERT INTO {T_CLAIMS}(maquina_id, mdb, producto, monto, items, device_id, ip, created_at) "
            "VALUES(?,?,?,?,?,?,?,?) RETURNING id",
            (maquina_id, armado["mdb"], armado["producto"], armado["monto"],
             json.dumps(armado.get("items") or [], ensure_ascii=False), device_id, ip, _ts(_now())),
        )
        claim_id = int(row["id"])
        dia = _dia()
        _incr(f"dev:{device_id}", dia)
        if ip:
            _incr(f"ip:{ip}", dia)
    return {"ok": True, "claim_id": claim_id, "monto": armado["monto"], "producto": armado["producto"]}


def _fila(claim_id: int) -> Optional[Dict[str, Any]]:
    return _one(f"SELECT * FROM {T_CLAIMS} WHERE id=?", (claim_id,))


def estado_reclamo(claim_id: int) -> Optional[Dict[str, Any]]:
    d = _fila(claim_id)
    if not d:
        return None
    d["terminal"] = d["state"] in TERMINALES
    for k in ("monto", "monto_real"):
        if d.get(k) is not None:
            d[k] = float(d[k])
    return d


def pendientes() -> List[int]:
    """Ids de claims PENDIENTE (para reanudar el polling tras un reinicio)."""
    return [int(r["id"]) for r in _all(f"SELECT id FROM {T_CLAIMS} WHERE state='PENDIENTE'")]


def cancelar_pendiente_tardio(claim_id: int) -> bool:
    """Marca NO_ENCONTRADO si el claim sigue PENDIENTE (sin venta reservada)."""
    return _run(
        f"UPDATE {T_CLAIMS} SET state='NO_ENCONTRADO', mensaje='No encontramos un pago que "
        "coincida en los últimos minutos.', updated_at=? "
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

                # MCP_TOKEN_GIFTCARD (del servicio Epay.Uno) es el token acotado del
                # bot: lee máquinas/planograma/cobros y es el único que emite gift
                # cards. Si está, tiene prioridad sobre EPAY_MCP_TOKEN.
                cls._mcp = EpayMCP(token=os.getenv("MCP_TOKEN_GIFTCARD", "").strip() or None).connect()
            return cls._mcp

    @classmethod
    def call(cls, tool: str, args: Dict[str, Any]):
        mcp = cls._conectar()
        with cls._call_lock:
            return mcp.call_tool(tool, args)

    _emision: Optional[tuple] = None  # (timestamp, bool)

    @classmethod
    def puede_emitir(cls) -> Optional[bool]:
        """True si el token conectado ve crear_gift_card (cache 5 min); None si no se pudo saber."""
        ahora = _now().timestamp()
        if cls._emision and cls._emision[0] + 300 > ahora:
            return cls._emision[1]
        try:
            mcp = cls._conectar()
            with cls._call_lock:
                ok = "crear_gift_card" in mcp.list_tools()
        except Exception as e:
            log.warning("No se pudo verificar el permiso de emision: %s", e)
            return None
        cls._emision = (ahora, ok)
        if not ok:
            log.error("El token MCP configurado NO puede crear gift cards: usa MCP_TOKEN_GIFTCARD")
        return ok

    _cache_maquinas: Optional[tuple] = None  # (timestamp, lista)
    MAQUINAS_TTL_S = 600

    @classmethod
    def maquinas_definidas(cls) -> List[Dict[str, Any]]:
        """Catálogo de máquinas del portal (cache 10 min)."""
        ahora = _now().timestamp()
        if cls._cache_maquinas and cls._cache_maquinas[0] + cls.MAQUINAS_TTL_S > ahora:
            return cls._cache_maquinas[1]
        mcp = cls._conectar()
        with cls._call_lock:
            lista = mcp.maquinas_definidas()
        cls._cache_maquinas = (ahora, lista)
        return lista

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


def _nombre_publico(codigo: str, nombre: str) -> str:
    """'V11 - Oficentro Los Ruices - U1023033' -> 'Oficentro Los Ruices'."""
    import re

    n = re.sub(r"\s+-\s+U\d+.*$", "", nombre or "").strip()
    n = re.sub(r"\s+-\s+K$", "", n).strip()
    if codigo and n.upper().startswith(codigo.upper()):
        n = n[len(codigo):].lstrip(" -–").strip()
    return n or codigo


def maquinas_activas() -> List[Dict[str, Any]]:
    """Máquinas que el cliente puede elegir en /reclamo (snacks activas, por prefijo)."""
    import sys

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if root not in sys.path:
        sys.path.insert(0, root)
    from epay_mcp import es_snack

    salida = []
    for m in MCPSync.maquinas_definidas():
        codigo = (m.get("codigo") or "").strip().upper()
        nombre = (m.get("nombre") or "").strip()
        if not codigo or m.get("maquina_id") is None:
            continue
        if nombre.upper().startswith("INACTIVO"):
            continue
        if PREFIJOS_SELECTOR and not any(codigo.startswith(p) for p in PREFIJOS_SELECTOR):
            continue
        if not es_snack(codigo, nombre):
            continue
        salida.append({"maquina_id": int(m["maquina_id"]), "codigo": codigo.split("-")[0],
                       "nombre": _nombre_publico(codigo.split("-")[0], nombre)})
    import unicodedata

    def _orden(x):  # alfabético ignorando acentos ("Clínica Ávila" junto a "Clínica Aa...")
        return unicodedata.normalize("NFKD", x["nombre"]).encode("ascii", "ignore").decode().lower()

    salida.sort(key=_orden)
    return salida


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
                "monto_despachado": _float(fila.get("monto_despachado")),
                "entregados": [str(x.get("producto") or "") for x in (fila.get("ventas") or [])],
            })
        dj += timedelta(days=1)
    salida.sort(key=lambda v: v["dt"])
    return salida


def _estado_ok(estado: str) -> bool:
    return "*" in ESTADOS_REEMBOLSABLES or estado in ESTADOS_REEMBOLSABLES


def _txkey(maquina_id: int, venta: Dict[str, Any]) -> str:
    return f"{maquina_id}|{venta['dt']:%Y-%m-%d %H:%M:%S}|{venta['mdb']}|{venta['monto']:.2f}"


def candidatas(claim_id: int) -> List[Dict[str, Any]]:
    """Cobros sin reclamar que encajan con el claim (monto + producto + ventana)."""
    row = _fila(claim_id)
    if not row or row["state"] != "PENDIENTE":
        return []
    grupos = [set(g.split("|")) for g in (row["mdb"] or "").strip().lower().split(",") if g]
    monto_obj = _float(row["monto"])
    if monto_obj is None:
        return []
    desde = _as_dt(row["created_at"]) - timedelta(minutes=VENTANA_MIN)
    vistas = _ventas_ventana(int(row["maquina_id"]), desde, _now())
    if LOG_COBROS:
        caracas = _zona_caracas()
        log.info(
            "claim %s maq %s busca %.2f Bs mdb=%s | cobros en ventana: %s",
            claim_id, row["maquina_id"], monto_obj, row["mdb"],
            [(f"{v['dt'].astimezone(caracas):%H:%M:%S}", v["mdb"], v["monto"], v["estado"]) for v in vistas] or "ninguno",
        )
    ventas = [
        v for v in vistas
        if v["monto"] is not None
        and abs(v["monto"] - monto_obj) <= TOLERANCIA_BS
        and _estado_ok(v["estado"])
        and (len(grupos) != 1 or v["mdb"] in grupos[0])
    ]
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
        with _EMITIENDO_LOCK:
            en_curso = claim_id in _EMITIENDO
        reservado = _as_dt(row["updated_at"] or row["created_at"])
        if en_curso or (_now() - reservado).total_seconds() < EMISION_MAX_S:
            return "PENDIENTE"  # otra consulta esta emitiendo esta tarjeta
        # Se reservo un cobro pero el proceso murio antes de terminar la emision:
        # no se sabe si la tarjeta llego a crearse. Lo revisa un operador.
        _marcar(claim_id, "ERROR_GIFTCARD",
                "Encontramos tu pago, pero la emisión del código se interrumpió. "
                "Una persona lo revisará contigo.")
        return "ERROR_GIFTCARD"

    if (_now() - _as_dt(row["created_at"])).total_seconds() > TIMEOUT_S:
        cancelar_pendiente_tardio(claim_id)
        return "NO_ENCONTRADO"

    ventas = candidatas(claim_id)
    if len(ventas) > 1:
        _marcar(claim_id, "AMBIGUO",
                "Hubo varias compras iguales en este momento y no pudimos confirmar "
                "cuál es la tuya. Una persona te ayudará.")
        return "AMBIGUO"
    if not ventas:
        return "PENDIENTE"

    v = ventas[0]
    key = _txkey(int(row["maquina_id"]), v)

    # Reserva atomica: si otro claim gano la carrera, tx_key UNIQUE lo impide.
    try:
        reservo = _run(
            f"UPDATE {T_CLAIMS} SET tx_key=?, estado_cobro=?, updated_at=? "
            "WHERE id=? AND tx_key IS NULL AND state='PENDIENTE'",
            (key, v["estado"] or None, _ts(_now()), claim_id),
        ) == 1
    except _INTEGRITY:
        reservo = False
    if not reservo:
        return "PENDIENTE"
    with _EMITIENDO_LOCK:
        _EMITIENDO.add(claim_id)
    try:
        return _emitir_reservado(claim_id, row, v, key)
    finally:
        with _EMITIENDO_LOCK:
            _EMITIENDO.discard(claim_id)


def faltantes(items: List[Dict[str, Any]], entregados: List[str]) -> List[Dict[str, Any]]:
    """Productos del reclamo que la máquina no registró como vendidos en ese cobro."""
    quedan = list(entregados)
    salida = []
    for it in items:
        pids = set(it.get("producto_ids") or [])
        faltan = 0
        for _ in range(int(it.get("cantidad") or 0)):
            hit = next((p for p in quedan if p in pids), None)
            if hit is None:
                faltan += 1
            else:
                quedan.remove(hit)
        if faltan:
            salida.append({**it, "cantidad": faltan})
    return salida


def _texto_items(items: List[Dict[str, Any]]) -> str:
    return ", ".join(f"{i['nombre']}" + (f" ×{i['cantidad']}" if i["cantidad"] > 1 else "") for i in items)


def monto_a_reembolsar(row: Dict[str, Any], v: Dict[str, Any]) -> tuple:
    """(monto Bs, mensaje) del reembolso para el cobro v del reclamo row.

    Manda el dinero: cobrado - despachado (las ventas que el MCP asocia al
    cobro). Los productos faltantes se usan para explicarle a la persona qué
    se le reembolsa.
    """
    cobrado = float(v["monto"])
    if not SOLO_FALTANTE:
        return cobrado, "Compra confirmada."
    despachado = v.get("monto_despachado") or 0.0
    monto = round(max(0.0, cobrado - despachado), 2)
    try:
        items = json.loads(row.get("items") or "[]")
    except (TypeError, ValueError):
        items = []
    falta = faltantes(items, v.get("entregados") or []) if items else []
    if monto <= TOLERANCIA_BS:
        return 0.0, ("La máquina registra que entregó todos los productos de esta compra. "
                     "Si no recibiste alguno, habla con una persona.")
    if despachado <= TOLERANCIA_BS:
        return monto, "La máquina no entregó tu compra. Te reembolsamos el total."
    detalle = f": {_texto_items(falta)}" if falta else ""
    return monto, f"La máquina entregó parte de tu compra. Te reembolsamos lo que faltó{detalle}."


def _emitir_reservado(claim_id: int, row: Dict[str, Any], v: Dict[str, Any], key: str) -> str:
    monto, mensaje = monto_a_reembolsar(row, v)
    if monto <= 0:
        _marcar(claim_id, "ENTREGADO", mensaje, monto_real=0.0)
        log.info("Claim %s ENTREGADO: la máquina despachó todo (%s)", claim_id, key)
        return "ENTREGADO"
    err_tope = _tope_error(row["device_id"], row["ip"], monto)
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
        "monto": monto,
        "monto_cobrado": v["monto"],
        "monto_despachado": v.get("monto_despachado"),
        "estado_cobro": v["estado"],
    }
    try:
        codigo = emitir_giftcard(contexto)
    except Exception as e:
        log.error("Emision fallida para claim %s: %s", claim_id, e)
        _marcar(claim_id, "ERROR_GIFTCARD",
                "Encontramos tu pago, pero no pudimos generar el código. "
                "Una persona lo revisará contigo.")
        return "ERROR_GIFTCARD"

    _marcar(claim_id, "EMITIDO", mensaje, gift_code=codigo, monto_real=monto)
    _registrar_emision(claim_id, codigo, monto, contexto)
    log.info("Claim %s EMITIDO (%s, %.2f de %.2f Bs, cobro %s)", claim_id, key, monto, v["monto"], v["estado"] or "?")
    return "EMITIDO"


init_db()
