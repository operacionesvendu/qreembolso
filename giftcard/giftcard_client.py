"""
giftcard_client.py -- Emision de la gift card de la reclamacion.

Si existe GIFTCARD_URL (endpoint que el negocio dispone), se POSTea el
contexto de la venta confirmada y se espera {"codigo": "..."} (o "code").
Si NO hay endpoint configurado, emite un codigo local de emergencia
(marcado como stub) para poder probar el flujo completo.
"""

from __future__ import annotations

import os
import secrets
from typing import Any, Dict

import requests

GIFTCARD_URL = os.getenv("GIFTCARD_URL", "").strip()
GIFTCARD_TOKEN = os.getenv("GIFTCARD_TOKEN", "").strip()


def _stub(contexto: Dict[str, Any]) -> str:
    return "STUB-" + secrets.token_hex(4).upper()


def emitir_giftcard(contexto: Dict[str, Any]) -> str:
    if not GIFTCARD_URL:
        return _stub(contexto)
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