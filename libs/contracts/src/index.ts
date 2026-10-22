/**
 * @poc/contracts — única fuente de verdad de los eventos (RNF-07).
 *
 * Convenciones:
 *  - Eventos en pasado (describen un hecho que ya ocurrió).
 *  - Topics con el formato dominio.tipo.
 *  - Cambios compatibles: se suman campos OPCIONALES y se mantiene `version`.
 *    Cambios incompatibles: suben `version`.
 */
import { createHash, randomUUID } from 'crypto';

// ------------------------------------------------------------------ topics

export const TOPICS = {
  ACTIVATION_REQUESTED: 'activation.requested',
  BILLING_EVENTS: 'billing.events',
  PROVISIONING_EVENTS: 'provisioning.events',
  ACTIVATION_EVENTS: 'activation.events',
} as const;

export type Topic = (typeof TOPICS)[keyof typeof TOPICS];

export const ALL_TOPICS: Topic[] = Object.values(TOPICS);

/** Dead Letter Queue de un topic: `<topic>.dlq` */
export const dlqOf = (topic: string): string => `${topic}.dlq`;

// ------------------------------------------------------------------ sobre común

export const EVENT_VERSION = 1;

export interface EventEnvelope<TType extends string = string, TPayload = unknown> {
  /** Identificador único; base de la idempotencia. */
  eventId: string;
  eventType: TType;
  version: number;
  occurredAt: string;
  /** El activationId: agrupa todos los eventos de una activación. */
  correlationId: string;
  /** También es la clave del mensaje en Kafka: garantiza orden por cliente. */
  customerId: string;
  /** Servicio que publicó el evento. */
  source: string;
  payload: TPayload;
}

// ------------------------------------------------------------------ payloads

/** Fallos de NEGOCIO simulados (se publican como BillingFailed / ProvisioningFailed). */
export type SimulateFailure = 'none' | 'billing' | 'provisioning';

/**
 * Error TÉCNICO simulado (extensión compatible: campo opcional).
 * El servicio indicado lanza una excepción en cada intento → reintentos 1s/2s/4s → DLQ.
 */
export type SimulateCrash = 'none' | 'billing' | 'provisioning';

export interface ActivationRequestedPayload {
  planId: string;
  channel: 'web';
  simulateFailure: SimulateFailure;
  simulateCrash?: SimulateCrash;
}

export interface BillingAccountCreatedPayload {
  accountId: string;
  planId: string;
}
export interface BillingFailedPayload {
  reason: string;
}
export interface BillingAccountCancelledPayload {
  accountId: string;
  reason: string;
}

export interface ProvisioningCompletedPayload {
  orderId: string;
  lineNumber: string;
}
export interface ProvisioningFailedPayload {
  reason: string;
}

export type FailedStep = 'billing' | 'provisioning' | 'timeout';

export interface ActivationCompletedPayload {
  planId: string;
  accountId?: string;
  lineNumber?: string;
}
export interface ActivationFailedPayload {
  planId: string;
  failedStep: FailedStep;
  reason: string;
}

// ------------------------------------------------------------------ eventos

export type ActivationRequested = EventEnvelope<'ActivationRequested', ActivationRequestedPayload>;

export type BillingAccountCreated = EventEnvelope<'BillingAccountCreated', BillingAccountCreatedPayload>;
export type BillingFailed = EventEnvelope<'BillingFailed', BillingFailedPayload>;
export type BillingAccountCancelled = EventEnvelope<'BillingAccountCancelled', BillingAccountCancelledPayload>;
export type BillingEvent = BillingAccountCreated | BillingFailed | BillingAccountCancelled;

export type ProvisioningCompleted = EventEnvelope<'ProvisioningCompleted', ProvisioningCompletedPayload>;
export type ProvisioningFailed = EventEnvelope<'ProvisioningFailed', ProvisioningFailedPayload>;
export type ProvisioningEvent = ProvisioningCompleted | ProvisioningFailed;

export type ActivationCompleted = EventEnvelope<'ActivationCompleted', ActivationCompletedPayload>;
export type ActivationFailed = EventEnvelope<'ActivationFailed', ActivationFailedPayload>;
export type ActivationOutcomeEvent = ActivationCompleted | ActivationFailed;

export type DomainEvent = ActivationRequested | BillingEvent | ProvisioningEvent | ActivationOutcomeEvent;
export type EventType = DomainEvent['eventType'];

/** En qué topic se publica cada tipo de evento. */
export const TOPIC_BY_EVENT: Record<EventType, Topic> = {
  ActivationRequested: TOPICS.ACTIVATION_REQUESTED,
  BillingAccountCreated: TOPICS.BILLING_EVENTS,
  BillingFailed: TOPICS.BILLING_EVENTS,
  BillingAccountCancelled: TOPICS.BILLING_EVENTS,
  ProvisioningCompleted: TOPICS.PROVISIONING_EVENTS,
  ProvisioningFailed: TOPICS.PROVISIONING_EVENTS,
  ActivationCompleted: TOPICS.ACTIVATION_EVENTS,
  ActivationFailed: TOPICS.ACTIVATION_EVENTS,
};

// ------------------------------------------------------------------ helpers

/**
 * UUID determinístico (estilo v5, SHA-1) a partir de un texto.
 * Se usa para los eventos DERIVADOS: si un servicio reprocesa el mismo mensaje,
 * genera exactamente el mismo eventId y los consumidores lo descartan como duplicado.
 */
export function deterministicId(name: string): string {
  const h = createHash('sha1').update(`activation-poc:${name}`).digest();
  h[6] = (h[6] & 0x0f) | 0x50; // versión 5
  h[8] = (h[8] & 0x3f) | 0x80; // variante RFC 4122
  const hex = h.subarray(0, 16).toString('hex');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20, 32)}`;
}

type EventOf<T extends EventType> = Extract<DomainEvent, { eventType: T }>;

export interface CreateEventInput<T extends EventType> {
  correlationId: string;
  customerId: string;
  source: string;
  payload: EventOf<T>['payload'];
  /** Si no se indica: aleatorio para ActivationRequested, determinístico para el resto. */
  eventId?: string;
}

export function createEvent<T extends EventType>(eventType: T, input: CreateEventInput<T>): EventOf<T> {
  const eventId =
    input.eventId ??
    (eventType === 'ActivationRequested' ? randomUUID() : deterministicId(`${input.correlationId}:${eventType}`));

  return {
    eventId,
    eventType,
    version: EVENT_VERSION,
    occurredAt: new Date().toISOString(),
    correlationId: input.correlationId,
    customerId: input.customerId,
    source: input.source,
    payload: input.payload,
  } as EventOf<T>;
}

/** Valida que un objeto cumpla el sobre común. Lanza un Error descriptivo si no. */
export function assertEnvelope(value: unknown): asserts value is DomainEvent {
  if (!value || typeof value !== 'object') {
    throw new Error('Mensaje inválido: no es un JSON con el sobre del evento');
  }
  const v = value as Record<string, unknown>;
  for (const field of ['eventId', 'eventType', 'correlationId', 'customerId', 'source', 'occurredAt']) {
    if (typeof v[field] !== 'string' || !(v[field] as string).length) {
      throw new Error(`Mensaje inválido: falta el campo "${field}"`);
    }
  }
  if (typeof v.version !== 'number') throw new Error('Mensaje inválido: falta "version"');
  if (!v.payload || typeof v.payload !== 'object') throw new Error('Mensaje inválido: falta "payload"');
}

// ------------------------------------------------------------------ headers

/**
 * Nombres de headers propios. OJO: no usar "kafka_correlationId" ni "kafka_replyTopic",
 * porque NestJS los interpreta como request/response y no como evento.
 */
export const HEADERS = {
  CORRELATION_ID: 'x-correlation-id',
  EVENT_TYPE: 'x-event-type',
  EVENT_ID: 'x-event-id',
  SOURCE: 'x-source',
  // DLQ
  ERROR: 'x-error',
  ORIGINAL_TOPIC: 'x-original-topic',
  ORIGINAL_PARTITION: 'x-original-partition',
  ORIGINAL_OFFSET: 'x-original-offset',
  CONSUMER_GROUP: 'x-consumer-group',
  ATTEMPTS: 'x-attempts',
} as const;
