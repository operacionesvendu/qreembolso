"""
giftcard_client.py -- Emision de la gift card de la reclamacion.

Orden de prioridad:
  1. GIFTCARD_URL (endpoint propio del negocio) si esta configurado.
  2. Tool MCP `crear_gift_card` (reportegift.php) con confirmar=true; genera
     la tarjeta directamente en el portal y devuelve el codigo nuevo.
  3. Codigo local STUB-... SOLO si GIFTCARD_ALLOW_STUB=1 (pruebas). Sin
     credenciales y sin ese flag, la emision falla (-> ERROR_GIFTCARD) para
     que nunca se entreguen codigos falsos en produccion.

La emision via MCP usa la misma sesion/credenciales que el resto (MCPSync
de match_service), asi que no se configura nada extra: EPAY_MCP_TOKEN (o
EPAY_MCP_USER + EPAY_MCP_PASS).
"""

from __future__ import annotations

import logging
import os
import re
import secrets
from datetime import date, timedelta
from typing import Any, Dict

import requests

GIFTCARD_URL = os.getenv("GIFTCARD_URL", "").strip()
GIFTCARD_TOKEN = os.getenv("GIFTCARD_TOKEN", "").strip()
GIFTCARD_VENCE = os.getenv("GIFTCARD_VENCE", "").strip()
GIFTCARD_ALLOW_STUB = os.getenv("GIFTCARD_ALLOW_STUB", "").strip().lower() in ("1", "true", "si", "yes")


def _hay_credenciales_mcp() -> bool:
    return bool(os.getenv("MCP_TOKEN_GIFTCARD") or os.getenv("EPAY_MCP_TOKEN")
                or (os.getenv("EPAY_MCP_USER") and os.getenv("EPAY_MCP_PASS")))


def _stub(contexto: Dict[str, Any]) -> str:
    return "STUB-" + secrets.token_hex(4).upper()


log = logging.getLogger("giftcard_client")


def _fecha_vence(valor: str | None = None, hoy: date | None = None) -> str:
    """Vencimiento YYYY-MM-DD (el MCP exige ese formato y una fecha futura).

    GIFTCARD_VENCE acepta una fecha fija ("2027-12-31") o un plazo en días
    ("+365d", "365", "+90"). Vacío o inválido -> hoy + 365 días.
    """
    v = (GIFTCARD_VENCE if valor is None else valor).strip()
    hoy = hoy or date.today()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", v):
        try:
            fija = date.fromisoformat(v)
            if fija > hoy:
                return fija.isoformat()
            log.warning("GIFTCARD_VENCE=%s ya pasó; se usa hoy + 365 días", v)
        except ValueError:
            log.warning("GIFTCARD_VENCE=%s no es una fecha válida; se usa hoy + 365 días", v)
    elif v:
        m = re.fullmatch(r"\+?\s*(\d{1,4})\s*d?", v, flags=re.IGNORECASE)
        if m and int(m.group(1)) > 0:
            return (hoy + timedelta(days=int(m.group(1)))).isoformat()
        log.warning("GIFTCARD_VENCE=%r no se entiende; se usa hoy + 365 días", v)
    return (hoy + timedelta(days=365)).isoformat()


def _emitir_via_mcp(contexto: Dict[str, Any]) -> str:
    """Crea la gift card en el portal ePay a traves de la tool `crear_gift_card`."""
    from .match_service import MCPSync

    monto = contexto.get("monto")
    if not monto:
        raise RuntimeError("Monto de la venta no disponible para emitir gift card")
    res = MCPSync.call("crear_gift_card", {
        "descripcion": f"Reembolso QR #{contexto.get('claim_id', '')} "
                       f"maq {contexto.get('maquina_id', '')} {contexto.get('producto') or ''}".strip(),
        "monto": round(float(monto), 2),
        "vence": _fecha_vence(),
        # idempotencia en el MCP: el mismo cobro (tx_key) nunca crea dos tarjetas
        "ref": str(contexto.get("referencia") or ""),
        "unico": True,
        "confirmar": True,
    })
    nuevas = (res or {}).get("gift_cards_nuevas") or []
    if not nuevas:
        raise RuntimeError(f"crear_gift_card no devolvio tarjeta: {res}")
    t = nuevas[0]
    codigo = str(t.get("col_0") or t.get("codigo") or t.get("Codigo") or "").strip()
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
    if _hay_credenciales_mcp():
        return _emitir_via_mcp(contexto)
    if GIFTCARD_ALLOW_STUB:
        return _stub(contexto)
    raise RuntimeError("Sin credenciales para emitir gift cards (EPAY_MCP_TOKEN o GIFTCARD_URL)")