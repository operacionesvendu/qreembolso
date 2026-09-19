"""
emitir_qrs.py -- Genera un QR por cada maquina de Vendu (serie V) con el URL
del micrositio de reclamo:  {QR_BASE_URL}/q/{maquina_id}

Uso:
    $env:QR_BASE_URL='https://reclamos.vendutest.com'   # URL publica del micrositio
    $env:QR_OUT_DIR='qrs'                                # carpeta de salida (default: ./qrs)
    $env:EPAY_MACHINE_FILTER='V01,V46'                   # opcional: solo esos prefijos
    python -m giftcard.emitir_qrs   (o: python giftcard/emitir_qrs.py)

Salida:
    qrs/qr-<codigo>-<maquina_id>.png   (QR + etiqueta con codigo y nombre)
    qrs/manifesto_qrs.csv              (codigo, maquina_id, nombre, url, archivo)

El MCP necesita EPAY_MCP_TOKEN (variable de entorno) para listar las maquinas.
"""

from __future__ import annotations

import csv
import os
import sys
from pathlib import Path

ROOT = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import qrcode

BASE_URL = os.getenv("QR_BASE_URL", "").strip() or "http://localhost:8502"
OUT_DIR = Path(os.getenv("QR_OUT_DIR", "qrs")).resolve()
PREFIJOS = [p.strip().upper() for p in
            os.getenv("EPAY_MACHINE_FILTER", "").split(",") if p.strip()]


def _fuente(size: int):
    try:
        return __import__("PIL").ImageFont.truetype("arial.ttf", size)
    except Exception:
        return __import__("PIL").ImageFont.load_default()


def _texto_en_lineas(draw, texto: str, fuente, ancho_max: int):
    lineas, linea = [], ""
    for palabra in texto.split():
        candidata = f"{linea} {palabra}".strip()
        if draw.textlength(candidata, font=fuente) <= ancho_max:
            linea = candidata
        else:
            if linea:
                lineas.append(linea)
            linea = palabra
    if linea:
        lineas.append(linea)
    return lineas


def emitir_qr(codigo: str, maquina_id: int, nombre: str, url: str, out_dir: Path) -> Path:
    from PIL import Image, ImageDraw

    qr = qrcode.make(url, box_size=10, border=3).get_image()
    ancho_qr, alto_qr = qr.size
    etiqueta = 88
    img = Image.new("RGB", (ancho_qr, alto_qr + etiqueta), (255, 255, 255))
    img.paste(qr, (0, 0))
    draw = ImageDraw.Draw(img)
    f1 = _fuente(24)
    f2 = _fuente(20)
    y = alto_qr + 10
    draw.text((12, y), codigo, fill=(0, 0, 0), font=f1)
    y += 34
    lineas = _texto_en_lineas(draw, nombre, f2, ancho_qr - 24)
    for ln in lineas[:2]:
        draw.text((12, y), ln, fill=(70, 70, 70), font=f2)
        y += 26

    archivo = out_dir / f"qr-{codigo}-{maquina_id}.png"
    img.save(archivo, "PNG")
    return archivo


def generar_qrs(
    base_url: str = BASE_URL,
    out_dir: Path = OUT_DIR,
    prefijos: list[str] | None = None,
    solo_snacks: bool = True,
) -> list[dict]:
    prefijos = prefijos or PREFIJOS
    from epay_mcp import EpayMCP, es_snack

    mcp = EpayMCP(token=os.getenv("EPAY_MCP_TOKEN") or None).connect()
    maquinas = mcp.maquinas_definidas()
    liberadas = []
    for m in maquinas:
        codigo = (m.get("codigo") or "").strip().upper()
        mid = m.get("maquina_id")
        if not codigo or mid is None:
            continue
        if not codigo.startswith("V"):
            continue
        if prefijos and not any(codigo.startswith(p) for p in prefijos):
            continue
        if solo_snacks and not es_snack(codigo, m.get("nombre")):
            continue
        liberadas.append(m)

    out_dir.mkdir(parents=True, exist_ok=True)
    filas = []
    for m in sorted(liberadas, key=lambda x: (x.get("codigo") or "")):
        codigo = (m.get("codigo") or "").strip()
        mid = m["maquina_id"]
        nombre = m.get("nombre") or ""
        url = f"{base_url.rstrip('/')}/q/{mid}"
        archivo = emitir_qr(codigo, mid, nombre, url, out_dir)
        filas.append({
            "codigo": codigo,
            "maquina_id": mid,
            "nombre": nombre,
            "url": url,
            "archivo": archivo.name,
        })
        print(f"  QR {archivo.name:28s} {codigo:<12} id={mid:<6} {url}")

    manifiesto = out_dir / "manifesto_qrs.csv"
    with open(manifiesto, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["codigo", "maquina_id", "nombre", "url", "archivo"])
        w.writeheader()
        w.writerows(filas)
    print(f"\n{len(filas)} QRs en {out_dir}  |  manifiesto: {manifiesto}")
    return filas


if __name__ == "__main__":
    if BASE_URL == "http://localhost:8502":
        print("AVISO: QR_BASE_URL no esta definido; se usa http://localhost:8502 "
              "(los QRs solo serviran localmente).")
    generar_qrs()