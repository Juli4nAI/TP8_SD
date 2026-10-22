# POC: Activación de servicios con Kafka

TP 8 de Sistemas Distribuidos.

La idea es simple: un cliente contrata un plan desde una web hecha en React, y eso publica **un solo evento** en Kafka. A partir de ahí cada servicio hace lo suyo por su cuenta (facturación, aprovisionamiento, el mail y el CRM), sin que ninguno llame directamente a otro.

## Integrantes

- Carmona Julián
- Flegler Mateo
- Joannas Damián

## Cómo levantarlo

Con Docker Desktop abierto, desde la carpeta del proyecto:

```bash
docker compose up -d --build
```

La primera vez tarda un rato porque baja las imágenes y compila todo. Para apagarlo: `docker compose down -v`.

## Dónde ver cada cosa

| Qué | Dónde |
|---|---|
| La demo | http://localhost:5173 |
| La API | http://localhost:3000/activations |
| Kafka UI | http://localhost:8080 |
| Los mails (Mailhog) | http://localhost:8026 |

## Casos que probamos

| # | Caso | Qué hicimos |
|---|---|---|
| 1 | Todo sale bien | En la demo elegimos *Sin fallo* y le damos a **Contratar**. Aparecen los 4 eventos en verde y llega el mail de bienvenida. |
| 2 | Algo falla | Elegimos *Falla provisioning*. Se ve `ProvisioningFailed`, después `ActivationFailed`, y billing anula la cuenta solo (`BillingAccountCancelled`). Llega un mail avisando del error. |
| 3 | Se cae un servicio | Apagamos el de mails con `docker compose stop notification-service` y le damos a **Contratar 3 seguidas**. En Kafka UI se ve que quedaron 3 mensajes pendientes (lag 3). Lo volvemos a prender con `docker compose start notification-service` y llegan los 3 mails. No se pierde nada. |
| 4 | Más instancias | Levantamos otra copia de billing con `docker compose up -d --scale billing-service=2`. En Kafka UI → Consumers → `billing-svc` se ve cómo reparte las 3 particiones entre las 2. |
| 5 | Un servicio nuevo | Prendemos `loyalty` con `docker compose --profile loyalty up -d loyalty-service`. Como es un grupo nuevo, lee todo desde el principio y procesa activaciones que pasaron antes de que existiera. |

## Otras cosas que se pueden ver

- **Mensajes repetidos.** En una activación terminada está el botón **Reenviar eventos**, que vuelve a mandar los mismos eventos con el mismo id. Los servicios se dan cuenta de que ya los procesaron y los ignoran: no se crean cuentas ni mails de más. En los logs aparece `ya procesado → descartado`.
- **Mensajes que fallan siempre.** Con *Error técnico en billing*, el mensaje se reintenta 3 veces y después va a parar a `activation.requested.dlq`, así no traba al resto. A los 30 segundos la activación se da por fallida.
- **Reprocesar el CRM desde cero.** `scripts/replay-crm.ps1` lo resetea al principio y vuelve a leer todo el historial.
