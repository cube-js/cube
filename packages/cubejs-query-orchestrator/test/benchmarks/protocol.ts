import { MethodCounter } from './instrument';

export type WorkerBenchSettings = {
  queueResponseSize: number,
  concurrency: number,
  handlerLatencyMs: number,
};

export type WorkerStartMessage = {
  type: 'start',
  tenantPrefix: string,
  benchSettings: WorkerBenchSettings,
  reconcileIntervalMs: number,
};

export type WorkerMessage =
  | WorkerStartMessage
  | { type: 'tickRequest', seq: number }
  | { type: 'shutdown' };

export type WorkerSnapshot = {
  handlersStarted: number,
  handlersFinished: number,
  methods: Record<string, MethodCounter>,
  events: Record<string, number>,
  fastTrack: { attempts: number, hits: number },
};

export type ParentMessage =
  | { type: 'counters', seq: number, data: WorkerSnapshot }
  | { type: 'done' };
