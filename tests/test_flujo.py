"""Pruebas del flujo de reclamo con un MCP falso y SQLite temporal.

    pip install -r requirements.txt -r requirements-dev.txt
    python -m pytest -q
"""

from __future__ import annotations

import os
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone

_TMP = tempfile.mkdtemp()
os.environ["CLAIMS_DB"] = os.path.join(_TMP, "claims.db")
os.environ.pop("DATABASE_URL", None)
os.environ.pop("EPAY_MCP_TOKEN", None)
os.environ["GIFTCARD_ALLOW_STUB"] = "1"
os.environ["RECLAMO_POLL_S"] = "1"
os.environ["ADMIN_TOKEN"] = "secreto"
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from fastapi.testclient import TestClient

from giftcard import app as app_mod
from giftcard import match_service as ms

CARACAS = timezone(-timedelta(hours=4))
MAQ = 10357


class FakeMCP:
    def __init__(self):
        self.cobros: list[dict] = []
        self.emitidas: list[dict] = []

    def cobro(self, mdb="000b", monto=150.0, estado="sin_despacho", hace_s=30):
        dt = (datetime.now(timezone.utc) - timedelta(seconds=hace_s)).astimezone(CARACAS)
        self.cobros.append({
            "fecha": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "respuesta": f"{mdb} OK",
            "monto": f"{monto:,.2f}",
            "estado": estado,
        })

    def call(self, tool, args):
        if tool == "cobros_maquina":
            assert args["maquina_id"] == str(MAQ)
            return {"cobros": list(self.cobros)}
        if tool == "crear_gift_card":
            self.emitidas.append(args)
            return {"gift_cards_nuevas": [{"col_0": f"GC{len(self.emitidas):04d}"}]}
        raise AssertionError(tool)

    def planograma(self, maquina_id):
        return {
            "maquina_id": maquina_id,
            "nombre": "V01 Prueba",
            "slots": [
                {"nombre": "Doritos", "seleccion": "000b", "cantidad": 3, "precio_bs": 150.0},
                {"nombre": "Agotado", "seleccion": "000c", "cantidad": 0, "precio_bs": 99.0},
                {"nombre": "Coca-Cola", "seleccion": "000d", "cantidad": 5, "precio_bs": 120.5},
                {"nombre": "Doritos", "seleccion": "000e", "cantidad": 2, "precio_bs": 150.0},
            ],
        }


@pytest.fixture()
def fake(monkeypatch):
    f = FakeMCP()
    monkeypatch.setattr(ms.MCPSync, "call", classmethod(lambda cls, t, a: f.call(t, a)))
    monkeypatch.setattr(ms.MCPSync, "planograma", classmethod(lambda cls, m: f.planograma(m)))
    monkeypatch.setattr(ms.MCPSync, "puede_emitir", classmethod(lambda cls: True))
    with ms._db() as con:
        for t in ("emisiones", "claims", "daily_limits"):
            con.execute(f"DELETE FROM {t}")
    return f


_n = 0


def _device():
    global _n
    _n += 1
    return f"dtest{_n:04d}"


def _reclamo(device, pedido=(("000b", 1),), ip="1.1.1.1"):
    r = ms.crear_reclamo(MAQ, list(pedido), device, ip)
    assert r["ok"], r
    return r["claim_id"]


# ------------------------------------------------------------------ motor

def test_emite_con_cobro_sin_despacho(fake):
    fake.cobro()
    cid = _reclamo(_device())
    assert ms.intento_match(cid) == "EMITIDO"
    est = ms.estado_reclamo(cid)
    assert est["gift_code"].startswith("STUB-")
    assert est["monto_real"] == 150.0


def test_por_defecto_emite_aunque_figure_despachado(fake):
    fake.cobro(estado="despachado")
    cid = _reclamo(_device())
    assert ms.intento_match(cid) == "EMITIDO"
    assert ms.estado_reclamo(cid)["estado_cobro"] == "despachado"


def test_estados_restringidos(fake, monkeypatch):
    monkeypatch.setattr(ms, "ESTADOS_REEMBOLSABLES", {"sin_despacho"})
    fake.cobro(estado="despachado")
    cid = _reclamo(_device())
    assert ms.intento_match(cid) == "PENDIENTE"


def test_monto_distinto_no_coincide(fake):
    fake.cobro(monto=200.0)
    cid = _reclamo(_device())
    assert ms.intento_match(cid) == "PENDIENTE"


def test_producto_en_varios_slots_se_agrupa(fake):
    stock = ms.productos_en_stock(MAQ)
    assert list(stock) == ["000b", "000d"]
    assert stock["000b"]["mdbs"] == ["000b", "000e"] and stock["000b"]["stock"] == 5
    fake.cobro(mdb="000e", monto=150.0)  # salio del otro slot de Doritos
    cid = _reclamo(_device())
    assert ms.intento_match(cid) == "EMITIDO"


def test_un_producto_exige_mismo_mdb(fake):
    fake.cobro(mdb="000d", monto=150.0)
    cid = _reclamo(_device())
    assert ms.intento_match(cid) == "PENDIENTE"


def test_varios_productos_suma_del_planograma(fake):
    fake.cobro(mdb="000d", monto=420.5)  # 2 x 150 + 120.50
    cid = _reclamo(_device(), pedido=(("000b", 2), ("000d", 1)))
    est = ms.estado_reclamo(cid)
    assert est["monto"] == 420.5 and est["producto"] == "Doritos x2, Coca-Cola"
    assert ms.intento_match(cid) == "EMITIDO"
    assert ms.estado_reclamo(cid)["monto_real"] == 420.5


def test_varios_productos_monto_distinto(fake):
    fake.cobro(monto=300.0)
    cid = _reclamo(_device(), pedido=(("000b", 2), ("000d", 1)))
    assert ms.intento_match(cid) == "PENDIENTE"


def test_producto_sin_stock_o_inexistente(fake):
    for mdb in ("000c", "ffff"):
        r = ms.crear_reclamo(MAQ, [(mdb, 1)], _device(), "1.1.1.1")
        assert not r["ok"] and r["status"] == 400


def test_maximo_de_productos(fake):
    r = ms.crear_reclamo(MAQ, [("000b", ms.MAX_ITEMS + 1)], _device(), "1.1.1.1")
    assert not r["ok"] and r["status"] == 400


def test_cobro_fuera_de_ventana(fake):
    fake.cobro(hace_s=(ms.VENTANA_MIN + 2) * 60)
    cid = _reclamo(_device())
    assert ms.intento_match(cid) == "PENDIENTE"


def test_cobro_se_canjea_una_sola_vez(fake):
    fake.cobro()
    a = _reclamo(_device(), ip="1.1.1.1")
    b = _reclamo(_device(), ip="2.2.2.2")
    assert ms.intento_match(a) == "EMITIDO"
    assert ms.intento_match(b) == "PENDIENTE"


def test_dos_cobros_identicos_es_ambiguo(fake):
    fake.cobro(hace_s=20)
    fake.cobro(hace_s=40)
    cid = _reclamo(_device())
    assert ms.intento_match(cid) == "AMBIGUO"


def test_tope_bs_por_usuario(fake, monkeypatch):
    monkeypatch.setattr(ms, "GIFTCARD_MAX_BS", 100.0)
    fake.cobro()
    cid = _reclamo(_device())
    assert ms.intento_match(cid) == "BLOQUEADO"


def test_limite_de_reclamos_por_dispositivo(fake):
    d = _device()
    for _ in range(ms.MAX_POR_DISPOSITIVO):
        cid = _reclamo(d)
        ms.cancelar_por_usuario(cid, d)
    r = ms.crear_reclamo(MAQ, [("000b", 1)], d, "9.9.9.9")
    assert not r["ok"] and "límite" in r["error"]


def test_un_reclamo_pendiente_a_la_vez(fake):
    d = _device()
    _reclamo(d)
    r = ms.crear_reclamo(MAQ, [("000b", 1)], d, "1.1.1.1")
    assert not r["ok"] and "en curso" in r["error"]


def test_timeout_marca_no_encontrado(fake, monkeypatch):
    cid = _reclamo(_device())
    monkeypatch.setattr(ms, "TIMEOUT_S", -1)
    assert ms.intento_match(cid) == "NO_ENCONTRADO"


def test_emision_interrumpida_va_a_operador(fake):
    cid = _reclamo(_device())
    viejo = ms._ts(datetime.now(timezone.utc) - timedelta(seconds=ms.EMISION_MAX_S + 5))
    ms._run("UPDATE claims SET tx_key='x', updated_at=? WHERE id=?", (viejo, cid))
    assert ms.intento_match(cid) == "ERROR_GIFTCARD"


def test_emision_reciente_no_se_marca_interrumpida(fake):
    cid = _reclamo(_device())
    ms._run("UPDATE claims SET tx_key='x', updated_at=? WHERE id=?", (ms._ts(ms._now()), cid))
    assert ms.intento_match(cid) == "PENDIENTE"


def test_emision_via_mcp(fake, monkeypatch):
    monkeypatch.setenv("EPAY_MCP_TOKEN", "t")
    fake.cobro()
    cid = _reclamo(_device())
    assert ms.intento_match(cid) == "EMITIDO"
    assert ms.estado_reclamo(cid)["gift_code"] == "GC0001"
    assert fake.emitidas[0]["monto"] == 150.0 and fake.emitidas[0]["confirmar"] is True
    assert fake.emitidas[0]["ref"].startswith(f"{MAQ}|")  # idempotencia por cobro
    fila = ms._one("SELECT * FROM emisiones WHERE claim_id=?", (cid,))
    assert fila and fila["codigo"] == "GC0001"


def test_sin_credenciales_ni_stub_falla(fake, monkeypatch):
    from giftcard import giftcard_client as gc

    monkeypatch.setattr(gc, "GIFTCARD_ALLOW_STUB", False)
    fake.cobro()
    cid = _reclamo(_device())
    assert ms.intento_match(cid) == "ERROR_GIFTCARD"


# ------------------------------------------------------------------ API

def test_api_flujo_completo(fake):
    fake.cobro()
    d = _device()
    with TestClient(app_mod.app) as c:
        h = {"X-Device-Id": d}
        info = c.get(f"/api/maquina/{MAQ}").json()
        assert [p["mdb"] for p in info["productos"]] == ["000d", "000b"]

        r = c.post("/api/reclamo", headers=h,
                   json={"maquina_id": MAQ, "items": [{"mdb": "000b", "cantidad": 1}]})
        assert r.status_code == 200, r.text
        assert r.json()["monto"] == 150.0
        cid = r.json()["claim_id"]

        ms.intento_match(cid)  # puede competir con la tarea de fondo: no debe romperla
        for _ in range(50):
            est = c.get(f"/api/reclamo/{cid}", headers=h).json()
            if est["terminal"]:
                break
            time.sleep(0.1)
        assert est["state"] == "EMITIDO" and est["gift_code"]

        qr = c.get(f"/api/reclamo/{cid}/qr", headers=h)
        assert qr.status_code == 200 and qr.headers["content-type"] == "image/png"

        # Otro dispositivo no puede ver el codigo
        otro = {"X-Device-Id": _device()}
        assert c.get(f"/api/reclamo/{cid}", headers=otro).status_code == 404
        assert c.get(f"/api/reclamo/{cid}/qr", headers=otro).status_code == 404


def test_api_cancelar(fake):
    d = _device()
    with TestClient(app_mod.app) as c:
        h = {"X-Device-Id": d}
        cid = c.post("/api/reclamo", headers=h,
                     json={"maquina_id": MAQ, "items": [{"mdb": "000b"}]}).json()["claim_id"]
        assert c.post(f"/api/reclamo/{cid}/cancelar", headers=h).json()["ok"] is True
        assert c.get(f"/api/reclamo/{cid}", headers=h).json()["state"] == "CANCELADO"


def test_api_valida_entrada(fake):
    with TestClient(app_mod.app) as c:
        cuerpo = {"maquina_id": MAQ, "items": [{"mdb": "000b"}]}
        assert c.post("/api/reclamo", json=cuerpo).status_code == 400  # sin X-Device-Id
        h = {"X-Device-Id": _device()}
        assert c.post("/api/reclamo", headers=h,
                      json={"maquina_id": MAQ, "items": []}).status_code == 422
        assert c.post("/api/reclamo", headers=h,
                      json={"maquina_id": MAQ, "items": [{"mdb": "000c"}]}).status_code == 400
        # un monto enviado por el cliente se ignora: manda el planograma
        r = c.post("/api/reclamo", headers=h, json={**cuerpo, "monto": 1})
        assert r.json()["monto"] == 150.0


def test_api_admin(fake):
    _reclamo(_device())
    with TestClient(app_mod.app) as c:
        assert c.get("/api/admin/reclamos").status_code == 401
        r = c.get("/api/admin/reclamos?estado=pendiente",
                  headers={"Authorization": "Bearer secreto"})
        assert r.status_code == 200 and len(r.json()["reclamos"]) == 1


def test_healthz_reporta_emision(fake):
    with TestClient(app_mod.app) as c:
        assert c.get("/healthz").json()["emision"] is True


def test_ip_usa_proxy_de_confianza():
    class R:
        headers = {"X-Forwarded-For": "6.6.6.6, 203.0.113.9"}
        client = None
    assert app_mod._ip(R()) == "203.0.113.9"
