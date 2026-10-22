#!/bin/sh
# Crea los 4 topics de negocio y sus DLQ: 3 particiones, retención de 7 días.
# Lo ejecuta el servicio kafka-init una sola vez, cuando el broker está sano.
set -e

BOOTSTRAP="${BOOTSTRAP:-kafka:29092}"
KT=/opt/kafka/bin/kafka-topics.sh
RETENTION_MS=604800000 # 7 días

for topic in activation.requested billing.events provisioning.events activation.events; do
  for name in "$topic" "$topic.dlq"; do
    "$KT" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
      --topic "$name" --partitions 3 --replication-factor 1 \
      --config retention.ms="$RETENTION_MS"
  done
done

echo "Topics disponibles:"
"$KT" --bootstrap-server "$BOOTSTRAP" --list
