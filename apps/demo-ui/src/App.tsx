import { FormEvent, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { io } from 'socket.io-client';

// ------------------------------------------------------------------ tipos (espejo de activation-api)

type Status = 'PENDING' | 'IN_PROGRESS' | 'ACTIVE' | 'FAILED';

interface HistoryEntry {
  eventType: string;
  at: string;
  source: string;
  eventId?: string;
  detail?: string;
}

interface Activation {
  _id: string;
  customerId: string;
  planId: string;
  status: Status;
  simulateFailure: string;
  simulateCrash?: string;
  failure?: { step: string; reason: string };
  history: HistoryEntry[];
  createdAt: string;
  updatedAt: string;
}

type FailureMode = 'none' | 'billing' | 'provisioning' | 'crash-billing';

const API = (import.meta.env.VITE_API_URL as string | undefined) ?? `${location.protocol}//${location.hostname}:3000`;
const KAFKA_UI = `${location.protocol}//${location.hostname}:8080`;
const MAILHOG = `${location.protocol}//${location.hostname}:8025`;

const PLANS = ['PLAN DIGITAL', 'PLAN FIBRA 300', 'PLAN MÓVIL 20GB', 'PLAN EMPRESAS'];

const FAILURE_OPTIONS: { value: FailureMode; label: string; hint: string }[] = [
  { value: 'none', label: 'Sin fallo', hint: 'Camino feliz: billing y provisioning confirman.' },
  { value: 'provisioning', label: 'Falla provisioning', hint: 'ProvisioningFailed → ActivationFailed → billing anula la cuenta.' },
  { value: 'billing', label: 'Falla billing', hint: 'BillingFailed → ActivationFailed. No hay cuenta para anular.' },
  {
    value: 'crash-billing',
    label: 'Error técnico en billing',
    hint: 'Billing reintenta 1 s, 2 s y 4 s, manda el mensaje a la DLQ y la saga vence a los 30 s.',
  },
];

const STATUS_LABEL: Record<Status, string> = {
  PENDING: 'Pendiente',
  IN_PROGRESS: 'En curso',
  ACTIVE: 'Activa',
  FAILED: 'Fallida',
};

// Carriles de la timeline: quién publicó cada evento
type Lane = 'api' | 'billing' | 'provisioning';
const LANES: { id: Lane; label: string; topic: string }[] = [
  { id: 'api', label: 'activation-api', topic: 'activation.requested · activation.events' },
  { id: 'billing', label: 'billing', topic: 'billing.events' },
  { id: 'provisioning', label: 'provisioning', topic: 'provisioning.events' },
];

function laneOf(source: string): Lane {
  if (source === 'billing') return 'billing';
  if (source === 'provisioning') return 'provisioning';
  return 'api';
}

type Tone = 'neutral' | 'ok' | 'fail' | 'comp' | 'demo';
function toneOf(eventType: string): Tone {
  if (eventType === 'ManualResend') return 'demo';
  if (eventType === 'BillingAccountCancelled') return 'comp';
  if (/Failed$/.test(eventType)) return 'fail';
  if (/(Created|Completed)$/.test(eventType)) return 'ok';
  return 'neutral';
}

const EVENT_TEXT: Record<string, string> = {
  ActivationRequested: 'Se pidió la activación',
  BillingAccountCreated: 'Cuenta de facturación creada',
  BillingFailed: 'Facturación rechazó el alta',
  BillingAccountCancelled: 'Cuenta anulada (compensación)',
  ProvisioningCompleted: 'Línea aprovisionada',
  ProvisioningFailed: 'No se pudo aprovisionar',
  ActivationCompleted: 'Activación completa',
  ActivationFailed: 'Activación fallida',
  ManualResend: 'Eventos reenviados a mano',
};

const time = (iso: string) =>
  new Date(iso).toLocaleTimeString('es-AR', { hour: '2-digit', minute: '2-digit', second: '2-digit' }) +
  '.' +
  String(new Date(iso).getMilliseconds()).padStart(3, '0');

// ------------------------------------------------------------------ app

export function App() {
  const [items, setItems] = useState<Record<string, Activation>>({});
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [connected, setConnected] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const upsert = useCallback((doc: Activation) => {
    setItems((prev) => ({ ...prev, [doc._id]: doc }));
  }, []);

  useEffect(() => {
    fetch(`${API}/activations?limit=30`)
      .then((r) => r.json())
      .then((list: Activation[]) => {
        setItems(Object.fromEntries(list.map((a) => [a._id, a])));
        if (list[0]) setSelectedId((cur) => cur ?? list[0]._id);
      })
      .catch(() => setError(`No se pudo conectar con activation-api en ${API}. ¿Está levantado docker compose?`));

    const socket = io(API, { transports: ['websocket'] });
    socket.on('connect', () => {
      setConnected(true);
      setError(null);
    });
    socket.on('disconnect', () => setConnected(false));
    socket.on('activation:update', upsert);
    return () => {
      socket.disconnect();
    };
  }, [upsert]);

  const list = useMemo(
    () => Object.values(items).sort((a, b) => b.createdAt.localeCompare(a.createdAt)),
    [items],
  );
  const selected = selectedId ? items[selectedId] : undefined;

  return (
    <div className="shell">
      <header className="top">
        <div>
          <h1>Activación de servicios</h1>
          <p className="sub">Un evento en Kafka, cuatro servicios reaccionando. Nadie se llama directamente.</p>
        </div>
        <nav className="tools">
          <span className={`ws ${connected ? 'on' : 'off'}`} title="Conexión WebSocket con activation-api">
            {connected ? 'En vivo' : 'Sin conexión'}
          </span>
          <a href={KAFKA_UI} target="_blank" rel="noreferrer">
            Kafka UI
          </a>
          <a href={MAILHOG} target="_blank" rel="noreferrer">
            Mailhog
          </a>
        </nav>
      </header>

      {error && <div className="banner">{error}</div>}

      <main className="grid">
        <aside className="side">
          <OrderForm
            onCreated={(id) => setSelectedId(id)}
            onError={setError}
          />
          <section className="history" aria-label="Activaciones">
            <h2>Activaciones</h2>
            {list.length === 0 ? (
              <p className="empty">Todavía no hay activaciones. Contratá un plan para ver el flujo de eventos.</p>
            ) : (
              <ul>
                {list.map((a) => (
                  <li key={a._id}>
                    <button
                      className={`row ${a._id === selectedId ? 'sel' : ''}`}
                      onClick={() => setSelectedId(a._id)}
                    >
                      <span className="rid">{a._id}</span>
                      <span className="rcust">
                        {a.customerId} · {a.planId}
                      </span>
                      <StatusPill status={a.status} />
                    </button>
                  </li>
                ))}
              </ul>
            )}
          </section>
        </aside>

        <section className="stage" aria-live="polite">
          {selected ? <Timeline activation={selected} onError={setError} /> : <StageEmpty />}
        </section>
      </main>
    </div>
  );
}

// ------------------------------------------------------------------ formulario (RF-01)

function OrderForm({ onCreated, onError }: { onCreated: (id: string) => void; onError: (e: string | null) => void }) {
  const [customerId, setCustomerId] = useState('C-1234');
  const [planId, setPlanId] = useState(PLANS[0]);
  const [mode, setMode] = useState<FailureMode>('none');
  const [busy, setBusy] = useState(false);

  const create = async (count: number) => {
    setBusy(true);
    onError(null);
    try {
      let lastId = '';
      for (let i = 0; i < count; i++) {
        const body = {
          customerId: count > 1 ? `${customerId}-${i + 1}` : customerId,
          planId,
          simulateFailure: mode === 'billing' || mode === 'provisioning' ? mode : 'none',
          simulateCrash: mode === 'crash-billing' ? 'billing' : 'none',
        };
        const res = await fetch(`${API}/activations`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
        });
        const json = await res.json();
        if (res.status !== 202) throw new Error(json?.message ?? `HTTP ${res.status}`);
        lastId = json.activationId;
      }
      onCreated(lastId);
    } catch (err) {
      onError(`No se pudo crear la activación: ${(err as Error).message}`);
    } finally {
      setBusy(false);
    }
  };

  const submit = (e: FormEvent) => {
    e.preventDefault();
    void create(1);
  };

  const hint = FAILURE_OPTIONS.find((o) => o.value === mode)?.hint;

  return (
    <form className="order" onSubmit={submit}>
      <h2>Contratar plan</h2>
      <label>
        Cliente
        <input value={customerId} onChange={(e) => setCustomerId(e.target.value)} required maxLength={30} />
      </label>
      <label>
        Plan
        <select value={planId} onChange={(e) => setPlanId(e.target.value)}>
          {PLANS.map((p) => (
            <option key={p}>{p}</option>
          ))}
        </select>
      </label>
      <fieldset>
        <legend>Forzar un fallo</legend>
        <div className="modes">
          {FAILURE_OPTIONS.map((o) => (
            <label key={o.value} className={`mode ${mode === o.value ? 'on' : ''}`}>
              <input type="radio" name="mode" value={o.value} checked={mode === o.value} onChange={() => setMode(o.value)} />
              {o.label}
            </label>
          ))}
        </div>
        <p className="hint">{hint}</p>
      </fieldset>
      <div className="actions">
        <button type="submit" className="primary" disabled={busy}>
          {busy ? 'Enviando…' : 'Contratar'}
        </button>
        <button type="button" disabled={busy} onClick={() => void create(3)} title="Escenario 3: con notification detenido">
          Contratar 3 seguidas
        </button>
      </div>
    </form>
  );
}

// ------------------------------------------------------------------ timeline en carriles (RF-08)

function Timeline({ activation: a, onError }: { activation: Activation; onError: (e: string | null) => void }) {
  const [resending, setResending] = useState(false);
  const seen = useRef<Set<string>>(new Set());
  const [fresh, setFresh] = useState<Set<string>>(new Set());

  const entries = useMemo(
    () => a.history.map((h, i) => ({ ...h, key: `${h.eventId ?? h.eventType}-${i}` })),
    [a.history],
  );

  // Resalta los eventos que llegan en vivo (no los que ya estaban al abrir)
  useEffect(() => {
    seen.current = new Set(entries.map((e) => e.key));
    setFresh(new Set());
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [a._id]);
  useEffect(() => {
    const incoming = entries.filter((e) => !seen.current.has(e.key)).map((e) => e.key);
    if (incoming.length) {
      incoming.forEach((k) => seen.current.add(k));
      setFresh(new Set(incoming));
    }
  }, [entries]);

  const t0 = entries[0] ? new Date(entries[0].at).getTime() : 0;
  const finished = a.status === 'ACTIVE' || a.status === 'FAILED';

  const resend = async () => {
    setResending(true);
    try {
      const res = await fetch(`${API}/activations/${a._id}/resend`, { method: 'POST' });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
    } catch (err) {
      onError(`No se pudo reenviar: ${(err as Error).message}`);
    } finally {
      setResending(false);
    }
  };

  return (
    <article className="tl">
      <header className={`tl-head s-${a.status}`}>
        <div>
          <h2>{a._id}</h2>
          <p>
            {a.customerId} contrató <b>{a.planId}</b>
            {a.simulateFailure !== 'none' && <> con fallo forzado en {a.simulateFailure}</>}
            {a.simulateCrash && <> con error técnico en {a.simulateCrash}</>}
          </p>
          {a.failure && <p className="why">{a.failure.reason}</p>}
        </div>
        <div className="tl-side">
          <StatusPill status={a.status} big />
          {finished && (
            <button onClick={() => void resend()} disabled={resending} title="Reenvía los eventos con el mismo eventId: no deben duplicarse cuentas ni emails">
              {resending ? 'Reenviando…' : 'Reenviar eventos'}
            </button>
          )}
        </div>
      </header>

      <div className="lanes" role="table" aria-label="Eventos de la activación por servicio">
        <div className="lane-heads" role="row">
          <span className="t-col" role="columnheader">
            t
          </span>
          {LANES.map((l) => (
            <span key={l.id} className={`lane-head l-${l.id}`} role="columnheader">
              <b>{l.label}</b>
              <small>{l.topic}</small>
            </span>
          ))}
        </div>

        {entries.map((e) => {
          const lane = laneOf(e.source);
          const tone = toneOf(e.eventType);
          const delta = new Date(e.at).getTime() - t0;
          return (
            <div key={e.key} className={`ev-row ${fresh.has(e.key) ? 'fresh' : ''}`} role="row">
              <span className="t-col" role="cell" title={time(e.at)}>
                +{(delta / 1000).toFixed(2)} s
              </span>
              {LANES.map((l) => (
                <span key={l.id} className={`cell l-${l.id}`} role="cell">
                  {l.id === lane && (
                    <span className={`ev t-${tone}`}>
                      <b className="ev-type">{e.eventType}</b>
                      <span className="ev-text">{EVENT_TEXT[e.eventType] ?? ''}</span>
                      {e.detail && <span className="ev-detail">{e.detail}</span>}
                      {e.eventId && <code className="ev-id">{e.eventId.slice(0, 8)}</code>}
                    </span>
                  )}
                </span>
              ))}
            </div>
          );
        })}

        {!finished && (
          <div className="ev-row waiting" role="row">
            <span className="t-col" role="cell" />
            {LANES.map((l) => (
              <span key={l.id} className="cell" role="cell">
                {l.id !== 'api' && !a.history.some((h) => laneOf(h.source) === l.id) && <span className="wait">esperando…</span>}
              </span>
            ))}
          </div>
        )}
      </div>

      <footer className="tl-foot">
        Todos estos eventos comparten <code>correlationId = {a._id}</code> y la clave <code>{a.customerId}</code>: en Kafka UI
        podés buscarlos por cualquiera de los dos.
      </footer>
    </article>
  );
}

function StatusPill({ status, big }: { status: Status; big?: boolean }) {
  return <span className={`pill p-${status} ${big ? 'big' : ''}`}>{STATUS_LABEL[status]}</span>;
}

function StageEmpty() {
  return (
    <div className="stage-empty">
      <h2>La timeline aparece acá</h2>
      <p>
        Contratá un plan a la izquierda. Vas a ver cómo <b>billing</b> y <b>provisioning</b> reaccionan en paralelo al mismo
        evento, y cómo activation-api junta los resultados.
      </p>
    </div>
  );
}
