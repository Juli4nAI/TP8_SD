# POC Activación de Servicios con Kafka

TP 8 de Sistemas Distribuidos 2026 (UTN-FRCU). Un cliente contrata un plan desde una UI en React. Eso publica **un único evento** en Kafka, y facturación, aprovisionamiento, notificación y CRM reaccionan solos, sin que ningún servicio llame directamente a otro.

Stack: Kafka 3.9 (KRaft, 1 broker), NestJS 11 con `@nestjs/microservices`, React + Vite + socket.io, MongoDB 7 (replica set de un nodo), Kafka UI (kafbat) y Mailhog.

## Levantarlo

Necesitás **Docker Desktop** corriendo. En Windows, con el backend de WSL 2 y al menos 4 GB de RAM asignados.

```powershell
docker compose up -d --build
```

La primera vez tarda unos minutos porque descarga las imágenes y compila el monorepo. Después:

| Qué | Dónde |
|---|---|
| Demo UI | http://localhost:5173 |
| activation-api | http://localhost:3000/activations |
| Kafka UI | http://localhost:8080 |
| Mailhog (bandeja de emails) | http://localhost:8025 |
| Kafka desde tu máquina | `localhost:9092` |
| MongoDB desde tu máquina | `mongodb://localhost:27017/?directConnection=true` |

Para ver qué pasa adentro: `docker compose logs -f billing-service provisioning-service activation-api`.

Para apagar todo y borrar los datos: `docker compose down -v`.

## Guion de la demo (sección 10)

Pantalla dividida: Demo UI a la izquierda, Kafka UI a la derecha. El paso a paso, con qué decir en cada escenario, está en [`docs/demo-script.md`](docs/demo-script.md).

| # | Escenario | Cómo |
|---|---|---|
| 1 | Camino feliz | UI → *Sin fallo* → **Contratar**. Aparecen 4 eventos en verde y llega un email a Mailhog. |
| 2 | Fallo y compensación | UI → *Falla provisioning* → **Contratar**. Se ven `ProvisioningFailed`, `ActivationFailed`, `BillingAccountCancelled` y el email de error. |
| 3 | Servicio caído | `docker compose stop notification-service`, después UI → **Contratar 3 seguidas**. Kafka UI muestra lag 3 en `notification-svc`. Al hacer `docker compose start notification-service` llegan los 3 emails. |
| 4 | Escalar | `docker compose up -d --scale billing-service=2`. Kafka UI → Consumers → `billing-svc` reparte las 3 particiones entre 2 miembros. |
| 5 | Nuevo consumidor | `docker compose --profile loyalty up -d loyalty-service`. El grupo nuevo `loyalty` procesa todo el historial (`docker compose logs loyalty-service`). |

Extras para los criterios de aceptación:

- **Idempotencia.** En la UI, sobre una activación terminada, **Reenviar eventos** vuelve a publicar sus eventos con el *mismo* `eventId`. Los logs muestran `ya procesado → descartado` y no aparecen cuentas ni emails nuevos.
- **DLQ.** Ejecutá `scripts/send-invalid.ps1` (o `.sh`), que publica un mensaje que no es JSON, o elegí *Error técnico en billing* en la UI. Se reintenta a 1 s, 2 s y 4 s y termina en `<topic>.dlq` con el motivo en los headers (`x-error`). El resto del flujo sigue.
- **RF-10, reprocesar CRM.** `scripts/replay-crm.ps1` resetea los offsets del grupo `crm-analytics` al inicio y reconstruye su base.
- **Rastreo por correlationId.** Ver todos los eventos de una activación:
  `docker compose exec mongo mongosh crm --quiet --eval "db.event_log.find({correlationId:'act-xxxx'}).sort({occurredAt:1})"`

## Estructura

```
apps/
  activation-api/         REST + WebSocket + agregador de la saga + timeout
  billing-service/        crea y anula (compensación) cuentas
  provisioning-service/   aprovisiona la línea (1 a 3 s)
  notification-service/   emails a Mailhog
  crm-analytics-service/  registra todos los eventos, no publica nada
  loyalty-service/        escenario 5 (profile "loyalty")
  demo-ui/                React + Vite + socket.io-client
libs/
  contracts/              eventos, sobre común, topics (única fuente de verdad)
  kafka-toolkit/          idempotencia, reintentos, DLQ, outbox, commit manual
docker/create-topics.sh   4 topics + 4 DLQ, 3 particiones, retención 7 días
docs/                     arquitectura y guion de la demo
scripts/                  replay de CRM y mensaje inválido para la DLQ
```

Las decisiones de diseño y en qué se aparta o completa el documento del TP están en [`docs/architecture.md`](docs/architecture.md).

## Desarrollo local (opcional)

No hace falta para la demo, porque Docker compila todo. Si querés compilar en tu máquina:

```powershell
npm install
npm run build
```

> **Ojo con OneDrive.** La carpeta está dentro de OneDrive, y `node_modules` tiene miles de archivos que OneDrive intenta sincronizar. Docker no lo necesita. Si lo instalás localmente, pausá la sincronización o mové el repo fuera de OneDrive.

## Problemas comunes

- **`port is already allocated`.** Otro programa usa el 3000, 5173, 8080, 8025, 9092 o 27017. Cerralo o cambiá el puerto de la izquierda en `docker-compose.yml`.
- **Las apps se reinician al arrancar.** Esperá unos segundos: dependen de `kafka-init` y de que Mongo sea PRIMARY. `docker compose ps` muestra el estado.
- **`create-topics.sh: not found` o errores con `\r`.** El script quedó con finales de línea de Windows. El repo trae `.gitattributes` para evitarlo. Si pasa igual, convertí el archivo a LF desde VS Code (abajo a la derecha, "CRLF" → "LF").
