# Arquitectura

activation-api es el único punto de entrada. Todo lo demás reacciona a eventos en Kafka.

```
Demo UI ──POST /activations──▶ activation-api ──(outbox)──▶ activation.requested
   ▲                               │  ▲                          │
   └──────── WebSocket ────────────┘  │              ┌───────────┼─────────────┐
                                      │              ▼           ▼             ▼
                    billing.events ◀── billing   provisioning   crm-analytics
               provisioning.events ◀────────────── provisioning
                                      │
          activation-api (agregador) ─┴─▶ activation.events ──▶ notification, billing (compensación), crm, loyalty
```

## Topics

| Topic | Eventos | Productor | Consumer groups |
|---|---|---|---|
| `activation.requested` | ActivationRequested | activation-api | billing-svc, provisioning-svc, crm-analytics |
| `billing.events` | BillingAccountCreated, BillingFailed, BillingAccountCancelled | billing | activation-api, crm-analytics |
| `provisioning.events` | ProvisioningCompleted, ProvisioningFailed | provisioning | activation-api, crm-analytics |
| `activation.events` | ActivationCompleted, ActivationFailed | activation-api | notification-svc, billing-svc, crm-analytics, loyalty |
| `<topic>.dlq` | mensajes que fallaron 4 intentos | el consumidor que falló | inspección manual en Kafka UI |

Todos tienen 3 particiones, retención de 7 días y `customerId` como clave.

## Cómo se cumple cada requerimiento no funcional

| RNF | Dónde |
|---|---|
| 01 `docker compose up` | `docker-compose.yml`. Usa healthchecks y `depends_on` con `service_healthy` / `service_completed_successfully`. Mongo inicializa su replica set en el propio healthcheck. |
| 02 orden por cliente | `OutboxService.add` usa `key = customerId`, y el relay publica en orden de `createdAt`. |
| 03 idempotencia | `ReliableConsumer`: inserta `eventId` en `processed_events` (índice único) en la misma transacción que el efecto. |
| 04 DLQ | `ReliableConsumer`: hace 1 intento + 3 reintentos (1 s, 2 s, 4 s) y después publica en `<topic>.dlq` con headers `x-error`, `x-original-topic`, `x-original-offset`, `x-attempts`. |
| 05 servicio caído | Commit manual (`autoCommit: false`) **después** de procesar. Al volver, el grupo sigue desde el último offset confirmado. |
| 06 correlationId | Va en el sobre y como header `x-correlation-id`. crm-analytics lo indexa en `event_log`. |
| 07 contratos versionados | `libs/contracts`: sobre con `version`, tipos por evento y mapa evento → topic. |

## Decisiones (y en qué se completa el documento)

1. **Outbox desde el principio (fase 4 incluida).** activation-api, billing y provisioning no publican directo. Escriben el evento en `outbox` dentro de la misma transacción que su cambio de estado, y un relay lo publica de inmediato (más un barrido cada 500 ms). Si el servicio se cae entre guardar y publicar, el evento no se pierde. Si se cae entre publicar y marcar SENT, se publica dos veces, y la idempotencia lo absorbe.

2. **eventId determinístico en los eventos derivados.** `BillingAccountCreated` de `act-123` siempre tiene el mismo `eventId` (UUID v5 de `act-123:BillingAccountCreated`). Si un mensaje se reprocesa, el evento resultante es *el mismo* evento y no uno nuevo.

3. **Agregador con update condicional.** La transición a ACTIVE o FAILED es un `updateOne({status: {$in: [PENDING, IN_PROGRESS]}})`. Solo quien logra el cambio publica `ActivationCompleted`/`ActivationFailed`. Así es imposible publicarlo dos veces, aunque haya dos instancias de activation-api o lleguen dos resultados a la vez.

4. **Caso borde de billing.** Si `ActivationFailed` llega antes que `ActivationRequested` (son topics distintos, no hay orden entre ellos), billing guarda `CANCEL_REQUESTED`. Cuando después procesa el pedido, crea la cuenta y la anula en el acto.

5. **Timeout de la saga (completa el documento).** Si un mensaje termina en la DLQ, su resultado nunca llega y la activación quedaría colgada en IN_PROGRESS. `SagaTimeoutService` la pasa a FAILED a los 30 s (`SAGA_TIMEOUT_MS`) y publica `ActivationFailed`, que dispara la compensación normal.

6. **Error técnico simulado (extensión compatible del contrato).** `simulateFailure` sigue siendo `none | billing | provisioning` (fallos de *negocio* → `BillingFailed`/`ProvisioningFailed`). Se sumó el campo **opcional** `simulateCrash`: el servicio lanza una excepción en cada intento, y así se ven en vivo los reintentos y la DLQ. Por ser un campo opcional no se sube `version`.

7. **Emails.** Un email no es transaccional. notification registra el envío en `notifications` (único por `activationId + eventType`) apenas sale, fuera de la transacción. Así un reintento no lo vuelve a mandar. La única ventana de duplicado es un crash justo entre enviar y registrar (at-least-once).

8. **Detalles de NestJS.** `postfixId: ''` evita que Nest agregue `-server` a los groupId. Los productores usan `producerOnlyMode: true` para no crear grupos `-client` vacíos. Los headers propios usan el prefijo `x-`, porque `kafka_correlationId` haría que Nest trate el mensaje como request/response.

9. **Una imagen para todo el monorepo.** El `Dockerfile` raíz instala y compila una vez, y cada servicio elige su `command`. Billing no tiene `container_name` ni puertos, así se puede escalar.

## Limitaciones conocidas (para discutir)

- **Provisioning no tiene compensación.** Si billing falla y provisioning ya aprovisionó, la línea queda aprovisionada. El documento solo pide compensar billing.
- **Kafka de un solo broker** (replication factor 1). Alcanza para mostrar particiones, offsets y grupos, pero no para tolerancia a fallos del broker.
- **Lease del outbox con varias instancias.** Con dos instancias del mismo servicio, el orden por cliente en la publicación puede alterarse si un lease vence. En la POC no ocurre en la práctica.
