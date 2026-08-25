import { BenchCounters, BenchQueueSettings, cloneMethods, MethodCounter } from './instrument';

export type WorkerStartMessage = {
  type: 'start',
  tenantPrefix: string,
  benchSettings: BenchQueueSettings,
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

/** Detached from the live counters, so a snapshot in flight over IPC cannot keep moving */
export function counterSnapshot(counters: Omit<BenchCounters, 'connections'>): WorkerSnapshot {
  return {
    handlersStarted: counters.handlersStarted,
    handlersFinished: counters.handlersFinished,
    methods: cloneMethods(counters.methods),
    events: { ...counters.events },
    fastTrack: { ...counters.fastTrack },
  };
}
