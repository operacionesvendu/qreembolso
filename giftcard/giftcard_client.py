"""
giftcard_client.py -- Emision de la gift card de la reclamacion.

Orden de prioridad:
  1. GIFTCARD_URL (endpoint propio del negocio) si esta configurado.
  2. Tool MCP `crear_gift_card` (reportegift.php) con confirmar=true; genera
     la tarjeta directamente en el portal y devuelve el codigo nuevo.
  3. Codigo local de emergencia STUB-... (marcado como stub, SOLO pruebas).

La emision via MCP usa la misma sesion/credenciales que el resto (MCPSync
de match_service), asi que no se configura nada extra: solo EPAY_MCP_TOKEN.
"""

from __future__ import annotations

import os
import secrets
from datetime import date, timedelta
from typing import Any, Dict

import requests

GIFTCARD_URL = os.getenv("GIFTCARD_URL", "").strip()
GIFTCARD_TOKEN = os.getenv("GIFTCARD_TOKEN", "").strip()
GIFTCARD_VENCE = os.getenv("GIFTCARD_VENCE", "").strip()


def _stub(contexto: Dict[str, Any]) -> str:
    return "STUB-" + secrets.token_hex(4).upper()


def _fecha_vence() -> str:
    if GIFTCARD_VENCE:
        return GIFTCARD_VENCE[:10]
    return (date.today() + timedelta(days=365)).isoformat()


def _emitir_via_mcp(contexto: Dict[str, Any]) -> str:
    """Crea la gift card en el portal ePay a traves de la tool `crear_gift_card`."""
    from .match_service import MCPSync

    monto = contexto.get("monto")
    if not monto:
        raise RuntimeError("Monto de la venta no disponible para emitir gift card")
    res = MCPSync.call("crear_gift_card", {
        "descripcion": f"Reembolso {contexto.get('producto') or 'QR'}",
        "monto": round(float(monto), 2),
        "vence": _fecha_vence(),
        "unico": True,
        "confirmar": True,
    })
    nuevas = (res or {}).get("gift_cards_nuevas") or []
    if not nuevas:
        raise RuntimeError(f"crear_gift_card no devolvio tarjeta: {res}")
    codigo = str((nuevas[0].get("col_0") or "").strip())
    if not codigo:
        raise RuntimeError(f"Gift card sin codigo: {nuevas[0]}")
    return codigo


def emitir_giftcard(contexto: Dict[str, Any]) -> str:
    if GIFTCARD_URL:
        headers = {"Content-Type": "application/json"}
        if GIFTCARD_TOKEN:
            headers["Authorization"] = f"Bearer {GIFTCARD_TOKEN}"
        r = requests.post(GIFTCARD_URL, json=contexto, headers=headers, timeout=20)
        r.raise_for_status()
        data = r.json()
        codigo = data.get("codigo") or data.get("code") or data.get("gift_code")
        if not codigo:
            raise RuntimeError(f"Endpoint giftcard no devolvio codigo: {data}")
        return str(codigo)
    if os.getenv("EPAY_MCP_TOKEN"):
        return _emitir_via_mcp(contexto)
    return _stub(contexto)