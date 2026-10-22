import { Controller, Logger, OnModuleInit } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext } from '@nestjs/microservices';
import { ActivationOutcomeEvent, TOPICS } from '@poc/contracts';
import { MongoService, ReliableConsumer } from '@poc/kafka-toolkit';
import { Collection } from 'mongodb';
import { Mailer } from './mailer';

interface NotificationDoc {
  activationId: string;
  eventType: string;
  eventId: string;
  to: string;
  subject: string;
  status: 'SENT';
  sentAt: Date;
}

/** Consumer group "notification-svc": activation.events */
@Controller()
export class NotificationConsumer implements OnModuleInit {
  private readonly logger = new Logger('Notification');

  constructor(
    private readonly consumer: ReliableConsumer,
    private readonly mongo: MongoService,
    private readonly mailer: Mailer,
  ) {}

  private get notifications(): Collection<NotificationDoc> {
    return this.mongo.db.collection<NotificationDoc>('notifications');
  }

  async onModuleInit(): Promise<void> {
    // Un email por activación y tipo de evento (RF-07 sin duplicados)
    await this.notifications.createIndex({ activationId: 1, eventType: 1 }, { unique: true, name: 'activation_eventType' });
  }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  onActivationOutcome(@Ctx() ctx: KafkaContext) {
    return this.consumer.process<ActivationOutcomeEvent>(ctx, async (e) => {
      const key = { activationId: e.correlationId, eventType: e.eventType };

      // Un email NO es transaccional (no se puede "deshacer"). Por eso, además de
      // processed_events, se mira si ya se envió y se registra apenas sale.
      if (await this.notifications.findOne(key)) {
        this.logger.log(`${e.correlationId}: el email de ${e.eventType} ya se había enviado`);
        return;
      }

      const to = `${e.customerId.toLowerCase().replace(/[^a-z0-9._-]/g, '')}@clientes.demo`;
      const { subject, html } = this.render(e);
      await this.mailer.send(to, subject, html);

      await this.notifications.updateOne(
        key,
        { $setOnInsert: { ...key, eventId: e.eventId, to, subject, status: 'SENT', sentAt: new Date() } },
        { upsert: true },
      );
      this.logger.log(`${e.correlationId}: email "${subject}" enviado a ${to}`);
    });
  }

  private render(e: ActivationOutcomeEvent): { subject: string; html: string } {
    const row = (k: string, v?: string) => (v ? `<tr><td style="color:#666;padding:2px 12px 2px 0">${k}</td><td><b>${v}</b></td></tr>` : '');
    if (e.eventType === 'ActivationCompleted') {
      return {
        subject: `¡Bienvenido! Tu plan ${e.payload.planId} ya está activo`,
        html: `<h2>¡Tu plan está activo!</h2>
<p>Hola ${e.customerId}, terminamos de activar tu servicio.</p>
<table>${row('Plan', e.payload.planId)}${row('Línea', e.payload.lineNumber)}${row('Cuenta', e.payload.accountId)}${row('Activación', e.correlationId)}</table>`,
      };
    }
    return {
      subject: `No pudimos activar tu plan ${e.payload.planId}`,
      html: `<h2>Hubo un problema con tu activación</h2>
<p>Hola ${e.customerId}, no pudimos completar la activación. No se te va a cobrar nada.</p>
<table>${row('Plan', e.payload.planId)}${row('Paso', e.payload.failedStep)}${row('Motivo', e.payload.reason)}${row('Activación', e.correlationId)}</table>`,
    };
  }
}
