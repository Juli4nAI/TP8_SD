import { Injectable } from '@nestjs/common';
import { createTransport, Transporter } from 'nodemailer';

/** SMTP hacia Mailhog (http://localhost:8025 para ver la bandeja). */
@Injectable()
export class Mailer {
  private readonly transporter: Transporter = createTransport({
    host: process.env.SMTP_HOST ?? 'localhost',
    port: Number(process.env.SMTP_PORT ?? 1025),
    secure: false,
    ignoreTLS: true,
  });

  async send(to: string, subject: string, html: string): Promise<void> {
    await this.transporter.sendMail({
      from: '"Activaciones POC" <no-reply@activaciones.demo>',
      to,
      subject,
      html,
    });
  }
}
