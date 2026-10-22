# RF-10: crm-analytics reprocesa TODO el historial desde el inicio (versión PowerShell).
# 1) detiene el servicio (un grupo con miembros activos no se puede resetear)
# 2) vuelve los offsets del grupo "crm-analytics" al principio de cada topic
# 3) vacía su base (event_log + processed_events) para reconstruirla desde cero
# 4) lo vuelve a levantar: relee todos los eventos retenidos (7 días)
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
docker compose stop crm-analytics-service
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 `
  --group crm-analytics --reset-offsets --to-earliest --all-topics --execute
docker compose exec -T mongo mongosh crm --quiet --eval "db.event_log.deleteMany({}); db.processed_events.deleteMany({}); 'base crm vaciada'"
docker compose start crm-analytics-service
Write-Host "Listo. Mira como reprocesa: docker compose logs -f crm-analytics-service"
