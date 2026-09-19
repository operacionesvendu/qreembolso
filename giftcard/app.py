"""
app.py -- Micrositio "La maquina no despacho" (QR por maquina).

Flujo:
  1. La persona escanea el QR -> GET /q/{maquina_id} (pagina movil).
  2. Elige "La maquina no despacho mi producto".
  3. El sistema muestra los productos en stock (planograma ePay) de ESA maquina.
  4. La persona confirma producto + monto -> POST /api/reclamo.
  5. Un fondo (asyncio) busca en ePay una venta nueva de ese producto en los
     ultimos minutos (polling); si coincide, emite la gift card y la muestra.

Configuracion (env):
  EPAY_MCP_TOKEN      token del MCP ePay.uno (obligatorio)
  GIFTCARD_URL        endpoint de gift cards (opcional; si falta, codigo STUB)
  GIFTCARD_TOKEN      token del endpoint (opcional)
  RECLAMO_*           ver match_service.py
  WHATSAPP_NUMBER     numero para el boton "hablar con un humano"
"""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from . import match_service as ms

ROOT = Path(__file__).resolve().parent
STATIC = ROOT / "static"
INDEX = STATIC / "index.html"
WHATSAPP = os.getenv("WHATSAPP_NUMBER", "").strip()

app = FastAPI(title="Vendu - Reclamo de maquina", version="0.1.0")
app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")

_reclamos_en_curso: dict[int, asyncio.Task] = {}


class ReclamoIn(BaseModel):
    maquina_id: int
    mdb: str
    producto: str = ""
    monto: float | None = None


def _device_id(req: Request) -> str:
    d = req.headers.get("X-Device-Id", "").strip()
    if not d or not d.replace("-", "").isalnum() or len(d) > 64:
        raise HTTPException(400, "Falta X-Device-Id valido")
    return d


def _ip(req: Request) -> str:
    fwd = req.headers.get("X-Forwarded-For", "")
    return fwd.split(",")[0].strip() if fwd else req.client.host if req.client else ""


async def _tarea_poll(claim_id: int) -> None:
    inicio = datetime.now(timezone.utc)
    while True:
        state = await asyncio.to_thread(ms.intento_match, claim_id)
        if state in ms.TERMINALES:
            return
        if (datetime.now(timezone.utc) - inicio).total_seconds() > ms.TIMEOUT_S + ms.POLL_S:
            ms.cancelar_pendiente_tardio(claim_id)
            return
        await asyncio.sleep(ms.POLL_S)


@app.get("/")
def home():
    return RedirectResponse("/q/0")


@app.get("/q/{maquina_id}")
def pagina_maquina(maquina_id: int):
    return FileResponse(INDEX)


@app.get("/api/maquina/{maquina_id}")
def info_maquina(maquina_id: int):
    try:
        pg = ms.MCPSync.planograma(maquina_id)
    except Exception as e:
        raise HTTPException(502, f"ePay no respondio: {e}")
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
    res = ms.crear_reclamo(
        body.maquina_id, body.mdb, body.producto or body.mdb, body.monto, device_id, ip
    )
    if not res.get("ok"):
        raise HTTPException(400, res.get("error", "No se pudo crear el reclamo"))
    claim_id = res["claim_id"]
    _reclamos_en_curso[claim_id] = asyncio.create_task(_tarea_poll(claim_id))
    return {"ok": True, "claim_id": claim_id}


@app.get("/api/reclamo/{claim_id}")
def ver_reclamo(claim_id: int):
    estado = ms.estado_reclamo(claim_id)
    if not estado:
        raise HTTPException(404, "Reclamo inexistente")
    return {
        "claim_id": claim_id,
        "state": estado["state"],
        "terminal": estado["terminal"],
        "monto_real": estado["monto_real"],
        "mensaje": estado["mensaje"],
        "gift_code": estado["gift_code"] if estado["state"] == "EMITIDO" else None,
    }


@app.get("/api/reclamo/{claim_id}/qr")
def codigo_qr(claim_id: int):
    estado = ms.estado_reclamo(claim_id)
    if not estado or estado["state"] != "EMITIDO" or not estado["gift_code"]:
        raise HTTPException(404, "Sin codigo de gift card")
    try:
        import qrcode
        import io

        qr = qrcode.make(estado["gift_code"], box_size=10, border=2)
        buf = io.BytesIO()
        qr.save(buf, format="PNG")
        from fastapi.responses import Response

        return Response(content=buf.getvalue(), media_type="image/png")
    except ImportError:
        raise HTTPException(500, "Falta la libreria qrcode")


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
        import urllib.parse

        texto = urllib.parse.quote(f"Maquina {maquina_id} ({nombre}): necesito ayuda con un reclamo.")
        link = f"https://wa.me/{WHATSAPP}?text={texto}"
    return {"maquina": nombre, "whatsapp": link, "telefono": WHATSAPP}