# Criterio de aceptación: "un mensaje inválido termina en la DLQ y el resto del flujo sigue".
# Publica un mensaje que NO es JSON en activation.requested (clave C-9999).
# billing, provisioning y crm-analytics lo reintentan 1 s, 2 s y 4 s y lo mandan a
# activation.requested.dlq con el motivo en los headers (verlo en Kafka UI).
Set-Location (Join-Path $PSScriptRoot "..")
"C-9999|esto no es un evento valido" | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh `
  --bootstrap-server localhost:9092 --topic activation.requested `
  --property parse.key=true --property "key.separator=|"
Write-Host "Mensaje invalido publicado. En ~7 s aparece en activation.requested.dlq"
