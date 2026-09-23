"""
epay_mcp.py -- EpayService: cliente robusto del MCP de ePay.uno.

El MCP (https://mcp-epayuno.tuvendu.com/mcp/epayuno-74f3688d61097548) es la
UNICA fuente de datos de máquinas, productos, planograma (canales), estatus e
inventario bajo.

Protocolo (Streamable HTTP / MCP 2025-03-26):
  1. POST initialize  -> la respuesta trae el header `mcp-session-id`.
  2. Enviar notifications/initialized.
  3. Cada requests posterior lleva ese mismo `mcp-session-id` + el token.

Esta clase encapsula la sesión (con reconexión automática), retries, timeouts,
logging, y la normalización de los datos que devuelve el MCP (números con coma,
texto con mojibake cp1252, máquinas "V46-UCVCOM", etc.).

Configuración (nunca versionar el token):
  * EPAY_MCP_URL   (default https://mcp-epayuno.tuvendu.com/mcp/epayuno-74f3688d61097548)
  * EPAY_MCP_TOKEN (en st.secrets en Streamlit Cloud, o variable de entorno)
  * EPAY_MCP_USER  + EPAY_MCP_PASS  (alternativa: Basic Auth a nuevos MCP)
  * EPAY_MACHINE_FILTER  prefijos de codigo de maquina, separados por coma.
                          Ej: "V46-UCVCOM" -> solo las que empiecen así.
  * EPAY_ONLY_SNACKS     "1"/"true" -> filtra a maquinas de snacks (heurística).

Uso:
  from epay_mcp import EpayMCP, es_snack, filtro_codigos
  mcp = EpayMCP().connect()
  maquinas = mcp.maquinas_definidas()
  planog   = mcp.planograma(10356)
"""

from __future__ import annotations

import html
import json
import logging
import os
import re
import threading
import time
from typing import Any, Dict, List, Optional

import requests

log = logging.getLogger("epay_mcp")

DEFAULT_URL = "https://mcp-epayuno.tuvendu.com/mcp/epayuno-74f3688d61097548"
PROTOCOL_VERSION = "2025-03-26"
CLIENT = {"name": "vendu-dashboard", "version": "1.0"}
TIMEOUT = (30, 300)  # (connect, read)
RETRIES = 3

_RE_NUM_US = re.compile(r"^\s*\$?\s*(\d{1,3}(?:,\d{3})*(?:\.\d+)?|\.\d+)\s*$")
_RE_NUM_EU = re.compile(r"^\s*\$?\s*(\d{1,3}(?:\.\d{3})*(?:,\d+)?|\d+,\d+)\s*$")


def parse_numerico(valor: Any) -> Optional[float]:
    """Convierte '1,015.82', '27,864.53', '0.77', '-149' o None -> float | None."""
    if valor is None:
        return None
    if isinstance(valor, (int, float)):
        return float(valor)
    t = str(valor).strip().replace(" ", "").replace("\xa0", "").replace("$", "")
    if not t or t in ("-", "—"):
        return None
    if _RE_NUM_US.match(t):
        return float(t.replace(",", ""))
    if _RE_NUM_EU.match(t) and "," in t:
        return float(t.replace(".", "").replace(",", "."))
    try:
        return float(t.replace(",", ""))
    except ValueError:
        return None


def parse_entero(valor: Any) -> Optional[int]:
    f = parse_numerico(valor)
    return int(f) if f is not None else None


def limpiar_texto(valor: Any) -> str:
    if valor is None:
        return ""
    t = html.unescape(str(valor)).strip()
    # Corrige mojibake tipico al leer cp1252/español
    t = t.replace("Ã©", "é").replace("Ã\xad", "í").replace("Ã³", "ó").replace("Ã¡", "á") \
         .replace("Ãº", "ú").replace("Ã±", "ñ").replace("â\x80\x93", "–").replace("Ã¼", "ü")
    return t


def es_snack(codigo: str, nombre: str) -> bool:
    """Heurística: una máquina es de snacks si su código no es de café ni
    su nombre lo indica. Los equipos Kikko (- K) y los códigos C* son café."""
    c = (codigo or "").strip().upper()
    n = (nombre or "").lower()
    if c.startswith("C") or "caf" in n or "- K" in n or "\u2013 k" in n:
        return False
    return True


def filtro_codigos(prefijos: Optional[str]) -> Optional[List[str]]:
    """'V46-UCVCOM,V08' -> ['V46-UCVCOM','V08']. None/vacío -> None (sin filtro)."""
    if not prefijos:
        return None
    return [p.strip().upper() for p in prefijos.split(",") if p.strip()]


def _sse_payload(texto: str) -> str:
    return "\n".join(l[5:].strip() for l in texto.splitlines() if l.startswith("data:"))


class ErrorMCP(Exception):
    """Error de negocio reportado por una tool del MCP."""


class EpayMCP:
    """Cliente MCP con sesión reutilizable y reconexión automática."""

    def __init__(
        self,
        url: Optional[str] = None,
        token: Optional[str] = None,
        *,
        user: Optional[str] = None,
        password: Optional[str] = None,
        timeout: tuple = TIMEOUT,
        retries: int = RETRIES,
    ):
        self.url = url or os.getenv("EPAY_MCP_URL") or DEFAULT_URL
        self.token = token or os.getenv("EPAY_MCP_TOKEN") or ""
        self.user = user or os.getenv("EPAY_MCP_USER") or ""
        self.password = password or os.getenv("EPAY_MCP_PASS") or ""
        if not self.token:
            # En Streamlit Cloud el token puede vivir en st.secrets
            try:
                import streamlit as st

                self.token = st.secrets.get("EPAY_MCP_TOKEN", "")
            except Exception:
                pass
        if not self.token and not (self.user and self.password):
            raise ValueError(
                "Falta EPAY_MCP_TOKEN (o EPAY_MCP_USER + EPAY_MCP_PASS para Basic Auth). "
                "La ePay la proporciona el operador; nunca commitearla."
            )
        self.timeout = timeout
        self.retries = retries
        self._session: Optional[requests.Session] = None
        self._sid: Optional[str] = None

    # ------------------------------------------------------------------ sesión

    def _base_headers(self) -> Dict[str, str]:
        h: Dict[str, str] = {
            "Accept": "application/json, text/event-stream",
            "Content-Type": "application/json",
        }
        if self.token:
            h["x-epay-token"] = self.token
        if self.user and self.password:
            import base64

            raw = f"{self.user}:{self.password}".encode("utf-8")
            h["Authorization"] = "Basic " + base64.b64encode(raw).decode("ascii")
        return h

    def _post(self, payload: dict, retries: Optional[int] = None) -> requests.Response:
        retries = self.retries if retries is None else retries
        last: Optional[Exception] = None
        for intento in range(1, retries + 1):
            try:
                h = self._base_headers()
                if self._sid:
                    h["mcp-session-id"] = self._sid
                resp = self._session.post(self.url, headers=h, json=payload, timeout=self.timeout)
                if resp.status_code >= 500:
                    raise requests.ConnectionError(f"HTTP {resp.status_code}")
                return resp
            except requests.RequestException as e:
                last = e
                log.warning("MCP POST falló (intento %s/%s, %s): %s", intento, retries, payload.get("method"), e)
                if intento < retries:
                    self._session.close()
                    self._session = requests.Session()
                    self._sid = None
                    try:
                        self._connect_interno()
                    except Exception:
                        pass
                    time.sleep(0.7 * intento)
        raise RuntimeError(f"MCP inalcanzable tras {retries} intentos: {last}")

    def _connect_interno(self) -> str:
        self._session = requests.Session()
        resp = self._post(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": CLIENT,
                },
            },
            retries=1,
        )
        if resp.status_code >= 400:
            detalle = ""
            try:
                detalle = resp.json().get("error", {}).get("message", "")
            except Exception:
                detalle = resp.text[:200]
            raise RuntimeError(f"MCP respondió HTTP {resp.status_code} en initialize: {detalle}")
        if not resp.headers:
            raise RuntimeError("MCP sin headers en initialize")
        self._sid = resp.headers.get("mcp-session-id")
        if not self._sid:
            raise RuntimeError("MCP no devolvió mcp-session-id (¿token correcto?)")
        try:
            self._session.post(
                self.url,
                headers=self._base_headers() | {"mcp-session-id": self._sid},
                json={"jsonrpc": "2.0", "method": "notifications/initialized"},
                timeout=self.timeout,
            )
        except Exception as e:  # notificación es best-effort
            log.debug("notifications/initialized: %s", e)
        return self._sid

    def connect(self) -> "EpayMCP":
        self._connect_interno()
        log.info("MCP conectado (sesión %s)", self._sid)
        return self

    # ------------------------------------------------------------------- tools

    def call_tool(self, nombre: str, arguments: Optional[Dict[str, Any]] = None) -> Any:
        """Ejecuta una tool y devuelve el JSON (application/json) del resultado."""
        if self._session is None or not self._sid:
            self.connect()
        # El texto puede llegar en varias líneas `data:`; SSE las concatena.
        resp = None
        for intento in range(1, self.retries + 1):
            try:
                resp = self._post(
                    {
                        "jsonrpc": "2.0",
                        "id": 2,
                        "method": "tools/call",
                        "params": {"name": nombre, "arguments": arguments or {}},
                    }
                )
                break
            except RuntimeError:
                if intento == self.retries:
                    raise
                self.connect()
        if resp is None:
            raise RuntimeError("Sin respuesta MCP")
        texto = resp.content.decode("utf-8", errors="replace")
        try:
            payload = json.loads(_sse_payload(texto))
        except json.JSONDecodeError as e:
            log.error("Respuesta MCP corrupta (%s): %s", nombre, texto[:500])
            raise ErrorMCP(f"Respuesta MCP no parseable para {nombre}: {e}")
        result = payload.get("result", {})
        if result.get("isError"):
            detalle = ""
            for c in result.get("content", []):
                detalle += c.get("text", "")
            raise ErrorMCP(detalle or f"Tool {nombre} falló")
        structured = result.get("structuredContent")
        if structured is not None:
            return structured
        # content: lista de {type: text, text: '...'} donde text es JSON
        for c in result.get("content", []):
            if c.get("type") == "text":
                t = c.get("text", "")
                try:
                    return json.loads(t)
                except (json.JSONDecodeError, TypeError):
                    return {"__raw": t}
        return {}

    # ------------------------------------------------------------- alta capa

    def maquinas_definidas(self) -> List[Dict[str, Any]]:
        """Catálogo de máquinas del portal: id, codigo, nombre, serial, uid..."""
        res = self.call_tool("maquinas_definidas", {})
        datos = res.get("datos", [])
        out = []
        for r in datos:
            cod = limpiar_texto(r.get("Codigo(sys)"))
            m = re.search(r"\((\d+)\)", cod)
            maquina_id = int(m.group(1)) if m else None
            codigo = cod.split("(")[0].strip()
            serial_raw = limpiar_texto(r.get("(Version) Serial"))
            st = re.search(r"\((U\d+)\)", serial_raw)
            uid = st.group(1) if st else ""
            out.append({
                "maquina_id": maquina_id,
                "codigo": codigo,
                "nombre": limpiar_texto(r.get("Nombre")),
                "serial": serial_raw,
                "uid": uid,
                "pago_hasta": limpiar_texto(r.get("Pago Hasta")),
                "ruta": limpiar_texto(r.get("Ruta")),
                "inventario": limpiar_texto(r.get("Inventario")),
                "ajustar": limpiar_texto(r.get("Ajustar")),
            })
        return out

    def estatus(self, filtro: str = "offline") -> List[Dict[str, Any]]:
        res = self.call_tool("estatus_maquinas", {"filtro": filtro} if filtro else {})
        return res.get("datos", [])

    def ubicaciones(self, fecha_desde: str = "", fecha_hasta: str = "") -> List[Dict[str, Any]]:
        args = {}
        if fecha_desde:
            args["fecha_desde"] = fecha_desde
        if fecha_hasta:
            args["fecha_hasta"] = fecha_hasta
        return self.call_tool("listar_ubicaciones", args).get("ubicaciones", [])

    def productos_globales(self) -> List[Dict[str, Any]]:
        """Catálogo global de productos (codigo, nombre, categoría, precios, stock)."""
        res = self.call_tool("productos_globales", {})
        out = []
        for r in res.get("datos", []):
            out.append({
                "codigo": limpiar_texto(r.get("Codigo")),
                "nombre": limpiar_texto(r.get("Nombre")),
                "categoria": limpiar_texto(r.get("Categoria")),
                "precio_bs": parse_numerico(r.get("Precio")),
                "precio_usd": parse_numerico(r.get("Precio USD")),
                "cantidad": parse_numerico(r.get("Cantidad")),
                "activo": limpiar_texto(r.get("Activo")).lower() == "activo",
            })
        return out

    def planograma(self, maquina_id: int) -> Dict[str, Any]:
        """Canales (slots) de UNA máquina: producto, seleccion, stock, minimo, maximo.

        El 'seleccion' (hex, ej. 000b) es la llave que une las ventas
        (Referencia) con el slot y su producto.
        """
        res = self.call_tool("planograma_maquina", {"maquina_id": int(maquina_id)})
        slots = []
        for s in res.get("slots", []):
            slots.append({
                "slot": limpiar_texto(s.get("slot")),
                "seleccion": (limpiar_texto(s.get("seleccion")) or "").lower(),
                "producto_id": parse_entero(s.get("producto_id")),
                "nombre": limpiar_texto(s.get("producto")),
                "precio_bs": parse_numerico(s.get("precio_bs")),
                "precio_usd": parse_numerico(s.get("precio_usd")),
                "cantidad": parse_numerico(s.get("cantidad")),
                "minimo": parse_numerico(s.get("minimo")),
                "maximo": parse_numerico(s.get("maximo")),
                "activo": bool(s.get("activo", True)),
                "estado": limpiar_texto(s.get("estado")),
            })
        return {
            "maquina_id": parse_entero(res.get("maquina_id")),
            "nombre": limpiar_texto(res.get("nombre")),
            "total_slots": len(slots),
            "resumen": res.get("resumen", {}),
            "slots": slots,
        }

    def ventas_rango(
        self,
        fecha_desde: str,
        fecha_hasta: str,
        *,
        maquina_id: Optional[int] = None,
        moneda: str = "bs",
        detalle: bool = False,
    ) -> Dict[str, Any]:
        """Resumen por máquina (+ detalle por selección si detalle=True)."""
        tool = "ventas_usd_x_rango" if moneda.lower() == "usd" else "ventas_x_rango"
        args: Dict[str, Any] = {"fecha_desde": fecha_desde, "fecha_hasta": fecha_hasta}
        if maquina_id:
            args["maquina_id"] = int(maquina_id)
        args["incluir_detalle"] = detalle
        res = self.call_tool(tool, args)
        return res

    def ventas_mes(self, anio: int, mes: int, *, maquina_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """Ventas por día de un mes (cantidad + monto_bs), opcional por máquina."""
        args: Dict[str, Any] = {"anio": int(anio), "mes": int(mes)}
        if maquina_id:
            args["maquina_id"] = int(maquina_id)
        res = self.call_tool("ventas_x_mes", args)
        dias = []
        for r in res.get("datos", []):
            dias.append({
                "dia": limpiar_texto(r.get("dia")),
                "cantidad": parse_numerico(r.get("cantidad")) or 0,
                "monto_bs": parse_numerico(r.get("monto_bs")) or 0,
            })
        return dias

    def ventas_de_producto(
        self,
        producto: str,
        fecha_desde: str,
        fecha_hasta: str,
        *,
        maquina_id: Optional[int] = None,
        desglose: Optional[str] = None,
    ) -> Dict[str, Any]:
        args: Dict[str, Any] = {
            "producto": producto,
            "fecha_desde": fecha_desde,
            "fecha_hasta": fecha_hasta,
        }
        if maquina_id:
            args["maquina_id"] = int(maquina_id)
        if desglose:
            args["desglose"] = desglose
        return self.call_tool("ventas_de_producto", args)

    def ventas_por_maquina(
        self,
        fecha_desde: str,
        fecha_hasta: str,
        *,
        maquina_id: Optional[int] = None,
        moneda: str = "bs",
    ) -> Dict[str, Any]:
        args: Dict[str, Any] = {"fecha_desde": fecha_desde, "fecha_hasta": fecha_hasta}
        if maquina_id:
            args["maquina_id"] = int(maquina_id)
        if moneda:
            args["moneda"] = moneda
        return self.call_tool("ventas_por_maquina", args)

    def inventario_bajo(self, *, maquina_id: Optional[int] = None, detalle: bool = True,
                        incluir_ok: bool = False) -> Dict[str, Any]:
        args = {"detalle": detalle, "incluir_ok": incluir_ok}
        if maquina_id:
            args["maquina_id"] = int(maquina_id)
        return self.call_tool("inventario_bajo", args)

    def costo_de_venta(self, fecha_desde: str, fecha_hasta: str, *,
                       desglose: str = "producto") -> Dict[str, Any]:
        return self.call_tool("costo_de_venta", {
            "fecha_desde": fecha_desde, "fecha_hasta": fecha_hasta, "desglose": desglose})

    def alerta_falta_costo(self, fecha_desde: str = "", fecha_hasta: str = "") -> Dict[str, Any]:
        args = {}
        if fecha_desde and fecha_hasta:
            args = {"fecha_desde": fecha_desde, "fecha_hasta": fecha_hasta}
        return self.call_tool("alerta_falta_costo", args)


def obtener_filtro_config() -> Optional[List[str]]:
    return filtro_codigos(os.getenv("EPAY_MACHINE_FILTER"))


def filtrar_maquinas(
    maquinas: List[Dict[str, Any]],
    prefijos: Optional[List[str]] = None,
    solo_snacks: bool = False,
) -> List[Dict[str, Any]]:
    prefijos = prefijos or obtener_filtro_config()
    out = []
    for m in maquinas:
        cod = (m.get("codigo") or "").upper()
        if prefijos and not any(cod.startswith(p) for p in prefijos):
            continue
        if solo_snacks and not es_snack(m.get("codigo"), m.get("nombre")):
            continue
        out.append(m)
    return out


# ------------------------------------------------------------------ CLI demo
if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    mcp = EpayMCP(
        url=os.getenv("EPAY_MCP_URL", None),
        token=os.getenv("EPAY_MCP_TOKEN", ""),
        user=os.getenv("EPAY_MCP_USER", ""),
        password=os.getenv("EPAY_MCP_PASS", ""),
    ).connect()
    opcion = sys.argv[1] if len(sys.argv) > 1 else "maquinas"
    if opcion == "maquinas":
        for m in filtrar_maquinas(mcp.maquinas_definidas()):
            print(json.dumps(m, ensure_ascii=False, default=str))
    elif opcion == "productos":
        for p in mcp.productos_globales():
            print(json.dumps(p, ensure_ascii=False, default=str))
    elif opcion == "estatus":
        for e in mcp.estatus(""):
            print(json.dumps(e, ensure_ascii=False, default=str))
    else:
        print("uso: python epay_mcp.py [maquinas|productos|estatus]")