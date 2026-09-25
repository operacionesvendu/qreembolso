# qreembolso · Vendu "La máquina no despachó"

Micrositio (mobile-first) que la persona abre al escanear el **QR pegado en una
máquina de Vendu**. Al confirmar su compra, un bot verifica contra ePay.uno
que exista la **venta real** en los últimos minutos y, si coincide, emite una
**gift card** mostrada en pantalla como código + QR.

## Estructura

```
├── epay_mcp.py                     # cliente MCP de ePay.uno (sesión, retries, normalización)
├── giftcard/
│   ├── app.py                      # FastAPI: micrositio + API de reclamos + admin
│   ├── match_service.py            # bot de verificación contra ePay (claim único)
│   ├── giftcard_client.py          # emisión de gift cards (MCP crear_gift_card / GIFTCARD_URL)
│   ├── emitir_qrs.py               # genera los QRs por máquina (PNG + manifiesto CSV)
│   └── static/index.html           # página móvil single-file
├── supabase/10_giftcard_claims.sql # migración de producción (schema vendu)
└── tests/test_flujo.py             # pruebas con MCP falso + SQLite temporal
```

## Cómo correr (local)

```powershell
pip install -r requirements.txt

$env:EPAY_MCP_TOKEN='...'            # token del MCP ePay.uno (obligatorio)
# $env:WHATSAPP_NUMBER='584241234567'   # opcional: botón "hablar con humano"

python -m uvicorn giftcard.app:app --host 0.0.0.0 --port 8502
```

Abrir `http://localhost:8502/q/10357` (el URL del QR apunta a `/q/{maquina_id}`).
Sin `DATABASE_URL` los reclamos se guardan en SQLite (`giftcard_claims.db`).

## Pruebas

```powershell
pip install -r requirements.txt -r requirements-dev.txt
python -m pytest -q
```

## Producción (Railway + Supabase)

1. Correr `supabase/10_giftcard_claims.sql` en el proyecto Supabase (es idempotente).
2. En el servicio de Railway definir:
   - `EPAY_MCP_TOKEN` (o `EPAY_MCP_USER` + `EPAY_MCP_PASS`)
   - `DATABASE_URL` = cadena de conexión Postgres de Supabase (rol `postgres`).
     **Obligatorio en Railway**: el disco es efímero; con SQLite cada deploy
     borra los reclamos, los topes diarios y el registro de cobros ya canjeados.
   - `WHATSAPP_NUMBER`, `ADMIN_TOKEN` (recomendados)
3. Healthcheck: `GET /healthz` (responde `{"ok":true,"db":"postgres",...}`).
4. Generar los QRs con `QR_BASE_URL` = dominio público del servicio.

Correr **una sola réplica/worker**: el polling vive en tareas asyncio del proceso.
(Con más réplicas no se duplican tarjetas —la reserva por `tx_key` es atómica—,
pero se multiplican las consultas a ePay.) Tras un reinicio, los reclamos
`PENDIENTE` se reanudan solos.

## Variables

| Variable                  | Defecto | Qué hace |
|---|---|---|
| `EPAY_MCP_TOKEN`          | —       | token del MCP ePay.uno (requerido) |
| `EPAY_MCP_USER`/`EPAY_MCP_PASS` | — | alternativa: Basic Auth al MCP |
| `EPAY_MCP_URL`            | MCP epayuno | URL del MCP |
| `DATABASE_URL`            | —       | Postgres de producción (sin ella: SQLite) |
| `CLAIMS_SCHEMA`           | `vendu` | schema de las tablas en Postgres |
| `CLAIMS_DB`               | `giftcard_claims.db` | archivo SQLite (solo desarrollo) |
| `RECLAMO_VENTANA_MIN`     | `10`    | minutos antes del reclamo en los que se busca el cobro |
| `RECLAMO_POLL_S`          | `10`    | cada cuánto se consulta ePay |
| `RECLAMO_TIMEOUT_S`       | `720`   | tiempo total de búsqueda antes de dar NO_ENCONTRADO |
| `RECLAMO_ESTADOS`         | `sin_despacho` | estados de `cobros_maquina` que se reembolsan (coma) |
| `RECLAMOS_POR_DISPOSITIVO`| `3`     | tope de reclamos/día por dispositivo |
| `RECLAMOS_POR_IP`         | `5`     | tope de reclamos/día por IP |
| `GIFTCARD_MAX_BS`         | `10000` | tope Bs/día reembolsados **por usuario** (device-id) |
| `GIFTCARD_MAX_BS_IP`      | `25000` | tope Bs/día reembolsados **por IP** |
| `GIFTCARD_URL`/`GIFTCARD_TOKEN` | — | endpoint propio de gift cards (tiene prioridad sobre el MCP) |
| `GIFTCARD_VENCE`          | `+365d` | vencimiento YYYY-MM-DD de las tarjetas emitidas por MCP |
| `GIFTCARD_ALLOW_STUB`     | —       | `1` = sin credenciales emite `STUB-XXXX` (solo pruebas) |
| `WHATSAPP_NUMBER`         | —       | número para el botón de soporte |
| `ADMIN_TOKEN`             | —       | habilita `GET /api/admin/reclamos` |
| `TRUSTED_PROXIES`         | `1`     | proxies delante de la app (para leer la IP real de `X-Forwarded-For`) |

La gift card se genera en el portal ePay mediante la tool MCP `crear_gift_card`
(`confirmar=true`), con la nota `Reembolso QR #<reclamo> maq <id> <producto>`
para poder rastrearla en `gift_cards_api`. Sin credenciales la emisión falla
(`ERROR_GIFTCARD`) salvo que se active `GIFTCARD_ALLOW_STUB`.

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
5. `POST /api/reclamo` → un fondo consulta `cobros_maquina` cada `RECLAMO_POLL_S` s.
6. **Match** = cobro con estado reembolsable (`sin_despacho`), misma máquina,
   mismo producto (MDB), mismo monto, dentro de la ventana y **sin reclamar**
   (claim único por `tx_key` UNIQUE).
7. `EMITIDO` → se muestra el código + QR (sobrevive a recargar la página).
8. Sin match en `RECLAMO_TIMEOUT_S` → `NO_ENCONTRADO` (reintenta o habla con soporte).

Estados: `PENDIENTE`, `EMITIDO`, `NO_ENCONTRADO`, `AMBIGUO`, `BLOQUEADO`,
`ERROR_GIFTCARD`, `CANCELADO`.

## API

| Método | Ruta | Uso |
|---|---|---|
| GET  | `/q/{maquina_id}` | página móvil |
| GET  | `/api/maquina/{id}` | productos en stock |
| POST | `/api/reclamo` | crea el reclamo (header `X-Device-Id`) |
| GET  | `/api/reclamo/{id}` | estado (solo el dispositivo que lo creó) |
| GET  | `/api/reclamo/{id}/qr` | PNG del código (solo el dispositivo que lo creó) |
| POST | `/api/reclamo/{id}/cancelar` | cancela si aún no se reservó un cobro |
| GET  | `/api/ayuda/{id}` | link de WhatsApp |
| GET  | `/api/admin/reclamos?estado=ERROR_GIFTCARD` | listado para operadores (`Authorization: Bearer $ADMIN_TOKEN`) |
| GET  | `/healthz` | healthcheck |

## Anti-fraude

- Solo se reembolsan cobros que ePay marca `sin_despacho`: una compra que sí
  salió de la máquina **no** genera gift card.
- id de dispositivo (`localStorage`) + límite diario; tope por IP (tomada del
  proxy de confianza, no del valor que manda el cliente).
- Tope monetario: Bs reembolsados/día por usuario y por IP; al superarlo el
  reclamo se marca `BLOQUEADO` sin emitir. El día se cuenta en hora de Venezuela.
- Cobro reclamable **una sola vez** (`tx_key` UNIQUE, reserva atómica).
- Varias compras idénticas en la ventana → `AMBIGUO` (no emite, pasa a soporte).
- El código de la gift card solo lo ve el dispositivo que hizo el reclamo.
- Si el proceso muere a mitad de una emisión, el reclamo pasa a
  `ERROR_GIFTCARD` para revisión manual (nunca se reintenta solo, para no
  emitir dos tarjetas).
