# Una sola imagen para todo el monorepo: se instala y compila una vez,
# y cada servicio de docker-compose elige qué app arranca con `command`.
FROM node:22-alpine

WORKDIR /app
ENV KAFKAJS_NO_PARTITIONER_WARNING=1

# 1) Dependencias (capa cacheada mientras no cambien los package.json)
COPY package.json package-lock.json ./
COPY libs/contracts/package.json libs/contracts/
COPY libs/kafka-toolkit/package.json libs/kafka-toolkit/
COPY apps/activation-api/package.json apps/activation-api/
COPY apps/billing-service/package.json apps/billing-service/
COPY apps/provisioning-service/package.json apps/provisioning-service/
COPY apps/notification-service/package.json apps/notification-service/
COPY apps/crm-analytics-service/package.json apps/crm-analytics-service/
COPY apps/loyalty-service/package.json apps/loyalty-service/
COPY apps/demo-ui/package.json apps/demo-ui/
RUN npm ci --no-audit --no-fund

# 2) Código y build (libs primero, después las apps)
COPY . .
RUN npm run build

CMD ["node", "-e", "console.error('Definí el command del servicio en docker-compose.yml'); process.exit(1)"]
