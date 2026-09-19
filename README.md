# qreembolso · Vendu "La máquina no despachó"

Micrositio (mobile-first) que la persona abre al escanear el **QR pegado en una
máquina de Vendu**. Al confirmar su compra, un bot verifica contra ePay.uno
que exista la **venta real** en los últimos minutos y, si coincide, emite una
**gift card** mostrada en pantalla como código + QR.

## Estructura

```
├── app.py                  # FastAPI: micrositio + API de reclamos
├── match_service.py        # bot de verificación contra ePay (claim único)
├── giftcard_client.py      # emisión de gift cards (GIFTCARD_URL o stub)
├── emitir_qrs.py           # genera los QRs por máquina (PNG + manifiesto CSV)
├── epay_mcp.py             # cliente MCP de ePay.uno (sesión, retries, normalización)
├── static/index.html       # página móvil single-file (2 botones, tutorial, QR result)
└── supabase/10_giftcard_claims.sql  # migración de producción (schema vendu)
```

## Cómo correr

```powershell
pip install -r requirements.txt

$env:EPAY_MCP_TOKEN='...'            # token del MCP ePay.uno (obligatorio)
# $env:WHATSAPP_NUMBER='584241234567'   # opcional: botón "hablar con humano"
# $env:GIFTCARD_URL='https://api.example.com/giftcard'   # opcional
# $env:GIFTCARD_TOKEN='...'

python -m uvicorn giftcard.app:app --host 0.0.0.0 --port 8502
```

Abrir `http://localhost:8502/q/10357` (el URL del QR apunta a `/q/{maquina_id}`).

## Variables (todas en `match_service.py`)

| Variable                  | Defecto | Qué hace |
|---|---|---|
| `EPAY_MCP_TOKEN`          | —       | token del MCP ePay.uno (requerido) |
| `CLAIMS_DB`               | `giftcard_claims.db` | archivo SQLite local (prototipo) |
| `RECLAMO_VENTANA_MIN`     | `10`    | ventana en minutos para buscar la venta |
| `RECLAMO_POLL_S`          | `10`    | cada cuánto se consulta ePay |
| `RECLAMO_TIMEOUT_S`       | `720`   | tiempo total de búsqueda antes de dar NO_ENCONTRADO |
| `RECLAMOS_POR_DISPOSITIVO`| `3`     | tope de gift cards/día por dispositivo |
| `RECLAMOS_POR_IP`         | `5`     | tope de gift cards/día por IP |
| `GIFTCARD_URL`/`GIFTCARD_TOKEN` | — | endpoint propio de gift cards (si existe) |
| `GIFTCARD_VENCE`        | `+365d` | vencimiento YYYY-MM-DD de las tarjetas emitidas por MCP |
| `WHATSAPP_NUMBER`         | —       | número para el botón de soporte |

La gift card se genera de verdad en el portal ePay mediante la tool MCP
`crear_gift_card` (usa `EPAY_MCP_TOKEN`, no requiere configurar nada extra).
Solo si el token no está presente se emite un código `STUB-XXXX` para pruebas.

## Generar los QRs por máquina

```powershell
$env:QR_BASE_URL='https://reclamos.ejemplo.com'   # URL pública del micrositio
$env:QR_OUT_DIR='qrs'                             # salida (default ./qrs)
# $env:EPAY_MACHINE_FILTER='V01,V64'              # opcional: solo esos prefijos
python -m giftcard.emitir_qrs
```

Crea `qrs/qr-<codigo>-<maquina_id>.png` (QR + etiqueta con el código y nombre)
y `qrs/manifesto_qrs.csv` (codigo, maquina_id, nombre, url, archivo).

## Flujo

1. Se escanea el QR → `GET /q/{maquina_id}`.
2. La persona elige **"La máquina no despachó mi producto"**.
3. Ve los productos **en stock** de ESA máquina (planograma ePay, cache 60 s).
4. Confirma producto + monto (prellenado con el precio del planograma, editable).
5. `POST /api/reclamo` → un fondo consulta `ventas_x_rango` (detalle) cada
   `RECLAMO_POLL_S` segundos.
6. **Match** = venta con misma máquina, mismo producto (MDB), monto real,
   minuto dentro de la ventana y **sin reclamar** (claim único por `tx_key` UNIQUE).
7. `EMITIDO` → se muestra el código + QR. Si la venta ya fue reclamada → no emite.
8. Sin match en `RECLAMO_TIMEOUT_S` → `NO_ENCONTRADO` (reintenta o habla con soporte).

## Anti-fraude

- id de dispositivo (`localStorage`) + límite diario; tope por IP.
- Venta reclamable **una sola vez** (columna `tx_key` UNIQUE, reserva atómica).
- Varias compras idénticas en el mismo minuto → lanza `AMBIGUO` (no emite);
  con el monto confirmado por el usuario se desambigua.
- Sin token MCP (o sin `GIFTCARD_URL`) → código `STUB-` (NO usar en producción).

## Producción

La tabla `claims` y los límites viven en SQLite aquí; al pasar al entorno real
utilizar la migración `supabase/10_giftcard_claims.sql` (schema `vendu`) y
conectar `match_service` a esa tabla sin cambiar la lógica.