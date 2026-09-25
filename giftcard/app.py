"""
app.py -- Micrositio "La maquina no despacho" (QR por maquina).

Flujo:
  1. La persona escanea el QR -> GET /q/{maquina_id} (pagina movil).
  2. Elige "La maquina no despacho mi producto".
  3. El sistema muestra los productos en stock (planograma ePay) de ESA maquina.
  4. La persona confirma producto + monto -> POST /api/reclamo.
  5. Un fondo (asyncio) busca en ePay un cobro sin despacho de ese producto en
     los ultimos minutos (polling); si coincide, emite la gift card y la muestra.

Configuracion (env):
  EPAY_MCP_TOKEN      token del MCP ePay.uno (obligatorio)
  DATABASE_URL        Postgres de produccion (sin ella: SQLite local)
  GIFTCARD_URL        endpoint de gift cards (opcional)
  GIFTCARD_TOKEN      token del endpoint (opcional)
  RECLAMO_*           ver match_service.py
  WHATSAPP_NUMBER     numero para el boton "hablar con un humano"
  ADMIN_TOKEN         habilita GET /api/admin/reclamos (Authorization: Bearer ...)
  TRUSTED_PROXIES     saltos de proxy delante de la app (Railway: 1)
"""

from __future__ import annotations

import asyncio
import hmac
import io
import logging
import os
import sys
import urllib.parse
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from . import match_service as ms

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("giftcard.app")

ROOT = Path(__file__).resolve().parent
STATIC = ROOT / "static"
INDEX = STATIC / "index.html"
WHATSAPP = os.getenv("WHATSAPP_NUMBER", "").strip()
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "").strip()
TRUSTED_PROXIES = int(os.getenv("TRUSTED_PROXIES", "1"))

_reclamos_en_curso: dict[int, asyncio.Task] = {}


def _lanzar_poll(claim_id: int) -> None:
    if claim_id in _reclamos_en_curso and not _reclamos_en_curso[claim_id].done():
        return
    _reclamos_en_curso[claim_id] = asyncio.create_task(_tarea_poll(claim_id))


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Tras un deploy/reinicio los claims PENDIENTE pierden su tarea de polling:
    # se reanudan (o vencen por timeout dentro de intento_match).
    try:
        ids = await asyncio.to_thread(ms.pendientes)
    except Exception as e:
        log.error("No se pudieron reanudar los reclamos pendientes: %s", e)
        ids = []
    for cid in ids:
        _lanzar_poll(cid)
    if ids:
        log.info("Reanudados %d reclamos pendientes", len(ids))
    yield
    for t in list(_reclamos_en_curso.values()):
        t.cancel()


app = FastAPI(title="Vendu - Reclamo de maquina", version="0.2.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")


class ReclamoIn(BaseModel):
    maquina_id: int = Field(gt=0)
    mdb: str = Field(min_length=1, max_length=16)
    producto: str = Field(default="", max_length=200)
    monto: float | None = Field(default=None, gt=0, lt=10_000_000)


def _device_id(req: Request) -> str:
    d = req.headers.get("X-Device-Id", "").strip()
    if not d or len(d) > 64 or not d.isascii() or not d.replace("-", "").isalnum():
        raise HTTPException(400, "Falta X-Device-Id valido")
    return d


def _ip(req: Request) -> str:
    """IP del cliente. X-Forwarded-For la puede falsear el cliente, asi que se
    toma la entrada que agrego el ultimo proxy de confianza (desde la derecha)."""
    fwd = [p.strip() for p in req.headers.get("X-Forwarded-For", "").split(",") if p.strip()]
    if fwd and TRUSTED_PROXIES > 0:
        return fwd[-min(TRUSTED_PROXIES, len(fwd))]
    return req.client.host if req.client else ""


def _reclamo_propio(claim_id: int, req: Request) -> dict:
    """El reclamo (y su codigo) solo lo ve el dispositivo que lo creo."""
    device_id = _device_id(req)
    estado = ms.estado_reclamo(claim_id)
    if not estado or not hmac.compare_digest(estado["device_id"], device_id):
        raise HTTPException(404, "Reclamo inexistente")
    return estado


async def _tarea_poll(claim_id: int) -> None:
    inicio = datetime.now(timezone.utc)
    try:
        while True:
            try:
                state = await asyncio.to_thread(ms.intento_match, claim_id)
            except Exception as e:  # un fallo puntual (red, DB) no mata el polling
                log.exception("intento_match(%s) fallo: %s", claim_id, e)
                state = "PENDIENTE"
            if state in ms.TERMINALES:
                return
            if (datetime.now(timezone.utc) - inicio).total_seconds() > ms.TIMEOUT_S + ms.POLL_S:
                await asyncio.to_thread(ms.cancelar_pendiente_tardio, claim_id)
                return
            await asyncio.sleep(ms.POLL_S)
    finally:
        _reclamos_en_curso.pop(claim_id, None)


@app.get("/")
def home():
    return RedirectResponse("/q/0")


@app.get("/healthz")
def healthz():
    return {"ok": True, "db": ms.backend(), "en_curso": len(_reclamos_en_curso)}


@app.get("/q/{maquina_id}")
def pagina_maquina(maquina_id: int):
    return FileResponse(INDEX, headers={"Cache-Control": "no-cache"})


@app.get("/api/maquina/{maquina_id}")
def info_maquina(maquina_id: int):
    if maquina_id <= 0:
        raise HTTPException(404, "Maquina inexistente")
    try:
        pg = ms.MCPSync.planograma(maquina_id)
    except Exception as e:
        log.warning("planograma(%s) fallo: %s", maquina_id, e)
        raise HTTPException(502, "No pudimos consultar la maquina. Intenta de nuevo en unos segundos.")
    vistos: dict[str, dict] = {}
    for s in pg.get("slots", []):
        nombre = (s.get("nombre") or "").strip()
        mdb = (s.get("seleccion") or "").lower()
        cant = s.get("cantidad") or 0
        if not nombre or not mdb or cant <= 0:
            continue
        vistos[mdb] = {
            "mdb": mdb,
            "nombre": nombre,
            "precio_bs": round(s.get("precio_bs") or 0, 2),
            "stock": int(cant),
        }
    return {
        "maquina_id": pg.get("maquina_id") or maquina_id,
        "nombre": pg.get("nombre") or "",
        "productos": sorted(vistos.values(), key=lambda x: x["nombre"]),
    }


@app.post("/api/reclamo")
async def crear_reclamo(body: ReclamoIn, req: Request):
    device_id = _device_id(req)
    ip = _ip(req)
    res = await asyncio.to_thread(
        ms.crear_reclamo,
        body.maquina_id, body.mdb, body.producto or body.mdb, body.monto, device_id, ip,
    )
    if not res.get("ok"):
        raise HTTPException(429, res.get("error", "No se pudo crear el reclamo"))
    claim_id = res["claim_id"]
    _lanzar_poll(claim_id)
    return {"ok": True, "claim_id": claim_id}


@app.get("/api/reclamo/{claim_id}")
def ver_reclamo(claim_id: int, req: Request):
    estado = _reclamo_propio(claim_id, req)
    return {
        "claim_id": claim_id,
        "maquina_id": estado["maquina_id"],
        "producto": estado["producto"],
        "state": estado["state"],
        "terminal": estado["terminal"],
        "monto_real": estado["monto_real"],
        "mensaje": estado["mensaje"],
        "gift_code": estado["gift_code"] if estado["state"] == "EMITIDO" else None,
    }


@app.post("/api/reclamo/{claim_id}/cancelar")
def cancelar_reclamo(claim_id: int, req: Request):
    _reclamo_propio(claim_id, req)
    ok = ms.cancelar_por_usuario(claim_id, _device_id(req))
    return {"ok": ok}


@app.get("/api/reclamo/{claim_id}/qr")
def codigo_qr(claim_id: int, req: Request):
    estado = _reclamo_propio(claim_id, req)
    if estado["state"] != "EMITIDO" or not estado["gift_code"]:
        raise HTTPException(404, "Sin codigo de gift card")
    import qrcode

    qr = qrcode.make(estado["gift_code"], box_size=10, border=2)
    buf = io.BytesIO()
    qr.save(buf, format="PNG")
    return Response(content=buf.getvalue(), media_type="image/png",
                    headers={"Cache-Control": "private, no-store"})


@app.get("/api/ayuda/{maquina_id}")
def ayuda(maquina_id: int):
    nombre = ""
    try:
        pg = ms.MCPSync.planograma(maquina_id)
        nombre = pg.get("nombre") or ""
    except Exception:
        pass
    link = ""
    if WHATSAPP:
        texto = urllib.parse.quote(f"Maquina {maquina_id} ({nombre}): necesito ayuda con un reclamo.")
        link = f"https://wa.me/{WHATSAPP}?text={texto}"
    return {"maquina": nombre, "whatsapp": link, "telefono": WHATSAPP}


@app.get("/api/admin/reclamos")
def admin_reclamos(req: Request, estado: str | None = None, limite: int = 100):
    """Listado para operadores (ERROR_GIFTCARD, AMBIGUO... se resuelven a mano)."""
    auth = req.headers.get("Authorization", "")
    if not ADMIN_TOKEN or not hmac.compare_digest(auth, f"Bearer {ADMIN_TOKEN}"):
        raise HTTPException(401, "No autorizado")
    return {"reclamos": ms.listar(estado, limite)}
