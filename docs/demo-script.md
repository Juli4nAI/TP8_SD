# Guion de la demo (unos 15 minutos)

Preparación: `docker compose up -d --build` antes de empezar, y esperar a que `docker compose ps` muestre todo `running` (kafka-init en `exited (0)`). Pantalla dividida: http://localhost:5173 a la izquierda y http://localhost:8080 a la derecha. Tené una terminal abierta en la carpeta del repo.

## 1. Camino feliz: fan-out (2 min)

1. En la UI elegí *Sin fallo* y hacé clic en **Contratar**.
2. Señalá que el POST respondió **202 Accepted** enseguida: la API no espera a nadie.
3. En la timeline, `ActivationRequested` aparece en el carril de activation-api. Billing y provisioning reaccionan **en paralelo** al mismo evento, cada uno en su carril. activation-api junta los dos OK y publica `ActivationCompleted`.
4. En Kafka UI → Topics → `activation.requested` → Messages, mostrá la clave `C-1234` y la partición.
5. Abrí Mailhog: llegó el email de bienvenida.

> Concepto: **un evento, varias reacciones**. activation-api no conoce a billing ni a provisioning.

## 2. Fallo y compensación: saga sin llamadas directas (3 min)

1. Elegí *Falla provisioning* y hacé clic en **Contratar**.
2. En la timeline aparecen `BillingAccountCreated`, `ProvisioningFailed`, `ActivationFailed` y `BillingAccountCancelled`, este último en naranja porque es la compensación.
3. En Mailhog llega el email de error.
4. *Falla billing*: no hay nada que compensar, porque la cuenta nunca se creó.

> Concepto: no hay transacción distribuida. Cada servicio deshace lo suyo cuando ve el evento de fallo.

## 3. Servicio caído: los offsets (3 min)

```powershell
docker compose stop notification-service
```

1. Hacé clic en **Contratar 3 seguidas**.
2. En Kafka UI → Consumers → `notification-svc`: **lag 3**. Los mensajes están en Kafka, esperando.
3. Levantá el servicio de nuevo:

```powershell
docker compose start notification-service
```

4. El lag vuelve a 0 y llegan los 3 emails a Mailhog.

> Concepto: Kafka no borra el mensaje al leerlo. Cada grupo guarda hasta dónde llegó (offset confirmado *después* de procesar).

## 4. Escalar: consumer group y particiones (2 min)

```powershell
docker compose up -d --scale billing-service=2
```

1. En Kafka UI → Consumers → `billing-svc`: ahora hay 2 miembros y las 3 particiones quedan repartidas 2 + 1.
2. Mencioná la pausa del rebalanceo. Con 4 instancias, una quedaría ociosa: el máximo de paralelismo es el número de particiones.
3. Para volver: `docker compose up -d --scale billing-service=1`.

## 5. Nuevo consumidor: replay del log (2 min)

```powershell
docker compose --profile loyalty up -d loyalty-service
docker compose logs -f loyalty-service
```

1. loyalty no existía cuando ocurrieron las activaciones. Arranca con el grupo nuevo `loyalty` y lee `activation.events` **desde el inicio**: asigna puntos por todas las activaciones exitosas del historial.
2. No se tocó ningún otro servicio.

> Cierre: esto con APIs REST directas no se puede hacer. El log de eventos es la fuente de verdad y se puede volver a leer.

## Extras si sobra tiempo o el profe pregunta

- **Idempotencia.** Sobre una activación terminada hacé clic en **Reenviar eventos**. En `docker compose logs billing-service notification-service` aparece `ya procesado → descartado`, y no hay cuenta ni email nuevo.
- **Reintentos y DLQ.** Elegí *Error técnico en billing*. Billing reintenta a 1 s, 2 s y 4 s, después lo manda a `activation.requested.dlq` (Kafka UI → headers `x-error`). A los 30 s la saga vence y pasa a FAILED. También podés usar `scripts/send-invalid.ps1`, que manda un mensaje que no es JSON.
- **Reprocesar CRM (RF-10).** `scripts/replay-crm.ps1`.

## Comandos útiles

```powershell
docker compose ps
docker compose logs -f activation-api billing-service provisioning-service
docker compose exec mongo mongosh billing --quiet --eval "db.billing_accounts.find().toArray()"
docker compose exec mongo mongosh crm --quiet --eval "db.event_log.countDocuments()"
docker compose down -v   # borra todo, incluidos los datos
```
