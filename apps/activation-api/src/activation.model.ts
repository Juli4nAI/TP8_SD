import { FailedStep, SimulateCrash, SimulateFailure } from '@poc/contracts';

export type ActivationStatus = 'PENDING' | 'IN_PROGRESS' | 'ACTIVE' | 'FAILED';
export const OPEN_STATUSES: ActivationStatus[] = ['PENDING', 'IN_PROGRESS'];

export interface StepResult {
  status: 'OK' | 'FAILED';
  at: string;
  eventId: string;
  data?: Record<string, unknown>;
}

export interface HistoryEntry {
  eventType: string;
  at: string;
  source: string;
  eventId?: string;
  detail?: string;
}

/** Documento de la colección `activations` (sección 07). */
export interface ActivationDoc {
  _id: string; // = activationId = correlationId
  customerId: string;
  planId: string;
  status: ActivationStatus;
  simulateFailure: SimulateFailure;
  simulateCrash?: SimulateCrash;
  steps: {
    billing?: StepResult;
    provisioning?: StepResult;
  };
  failure?: { step: FailedStep; reason: string };
  history: HistoryEntry[];
  createdAt: Date;
  updatedAt: Date;
}

export interface CreateActivationDto {
  customerId: string;
  planId: string;
  simulateFailure?: SimulateFailure;
  simulateCrash?: SimulateCrash;
}
