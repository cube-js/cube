// eslint-disable-next-line import/no-extraneous-dependencies
import 'source-map-support/register';

import fs from 'fs';
import path from 'path';
import readline from 'readline';
import { fork } from 'child_process';
// eslint-disable-next-line import/no-extraneous-dependencies
import yargs from 'yargs';
// eslint-disable-next-line import/no-extraneous-dependencies
import { hideBin } from 'yargs/helpers';
import { pausePromise } from '@cubejs-backend/shared';
import { BenchRun, estimateRunMs, rhoOf, Suite, SUITES, suiteByName } from './suites';

type Args = {
  suites: string[],
  driver?: 'cubestore' | 'memory',
  fastTrack: boolean[],
  settleMs: number,
  runTimeoutMs: number,
  only?: string[],
  repeat: number,
  out?: string,
  dryRun: boolean,
  report?: string,
  list: boolean,
};

const commaList = (v: unknown): string[] => String(v).split(',').map((s) => s.trim()).filter(Boolean);

function parseArgs(argv: string[]): Args {
  const parsed = yargs(argv)
    .scriptName('bench:suite')
    // A variadic positional is only collected by a command builder, and angle brackets would make
    // it required, which --list and --report do not satisfy
    .command('$0 [suites..]', 'Run queue benchmark suites, off and on, into a .jsonl', (y) => y
      .positional('suites', {
        describe: 'Suite names, or "all" for every suite except the smoke one',
        type: 'string',
        array: true,
        default: [] as string[],
      }))
    .example('$0 S1 --dry-run', 'price the sweep before starting it')
    .example('$0 S1 S6 --settle=5000', 'run two suites back to back')
    .example('$0 S1 --only=rho=2.5 --repeat=3', 'one point, three times')
    .example('$0 --report results.jsonl', 'redraw the table from a finished run')
    .option('driver', {
      describe: 'Override the driver the suite declares',
      choices: ['cubestore', 'memory'] as const,
    })
    .option('fast-track', {
      describe: 'Which passes to run for each configuration',
      default: 'off,on',
      coerce: (v: unknown) => commaList(v).map((p) => p === 'on' || p === 'true'),
    })
    .option('settle', {
      describe: 'Pause between runs, ms — lets the previous run\'s connections go away',
      type: 'number',
      default: 3000,
    })
    .option('run-timeout', {
      describe: 'Kill a run after this long, ms',
      type: 'number',
      default: 30 * 60 * 1000,
    })
    .option('only', {
      describe: 'Comma separated substrings; keeps just the matching runs',
      coerce: commaList,
    })
    // Above capacity a single pair is not enough to conclude from — the same point, several times
    .option('repeat', {
      describe: 'Run each selected point this many times, labelled #1, #2, …',
      type: 'number',
      default: 1,
    })
    .option('out', {
      describe: 'Write the .jsonl here instead of .context/bench-results',
      type: 'string',
    })
    .option('dry-run', {
      describe: 'Print the matrix with computed \u03c1 and estimated wall clock, run nothing',
      type: 'boolean',
      default: false,
    })
    .option('report', {
      describe: 'Redraw the summary from an existing .jsonl and exit',
      type: 'string',
    })
    .option('list', {
      describe: 'List the suites and exit',
      type: 'boolean',
      default: false,
    })
    .check((a) => {
      if (!a.list && !a.report && (a.suites as string[]).length === 0) {
        throw new Error('Name at least one suite, or pass --list / --report');
      }
      if (a.repeat < 1) {
        throw new Error('--repeat must be at least 1');
      }

      return true;
    })
    .strict()
    .wrap(Math.min(120, process.stdout.columns || 120))
    .parseSync();

  return {
    suites: parsed.suites as string[],
    driver: parsed.driver,
    fastTrack: parsed.fastTrack,
    settleMs: parsed.settle,
    runTimeoutMs: parsed.runTimeout,
    only: parsed.only,
    repeat: parsed.repeat,
    out: parsed.out,
    dryRun: parsed.dryRun,
    report: parsed.report,
    list: parsed.list,
  };
}

// dist/test/benchmarks -> dist -> package -> packages -> repo root
const repoRoot = path.resolve(__dirname, '../../../../..');

function resultsDir(): string {
  const dir = path.resolve(repoRoot, '.context/bench-results');
  fs.mkdirSync(dir, { recursive: true });

  return dir;
}

function stamp(): string {
  return new Date().toISOString().replace(/[:.]/g, '-').slice(0, 16);
}

type RunRecord = any;

function selectRuns(suite: Suite, args: Args): BenchRun[] {
  return args.only ? suite.runs.filter((r) => args.only!.some((o) => r.label.includes(o))) : suite.runs;
}

async function executeRun(suite: Suite, benchRun: BenchRun, fastTrack: boolean, args: Args, sink: fs.WriteStream): Promise<RunRecord> {
  const driver = args.driver || suite.driver;
  const entry = path.resolve(__dirname, driver === 'memory' ? 'QueueMemory.bench.js' : 'QueueCubestore.bench.js');
  const runId = `${suite.name}/${benchRun.label}/${fastTrack ? 'on' : 'off'}`;

  console.log(`\n=== ${runId} — ${JSON.stringify(benchRun.axis)} — est. ${Math.round(estimateRunMs(benchRun.env) / 1000)}s ===`);

  const child = fork(entry, [], {
    execArgv: process.execArgv,
    stdio: ['inherit', 'pipe', 'inherit', 'ipc'],
    env: {
      ...process.env,
      ...benchRun.env,
      CUBEJS_QUEUE_FAST_TRACK: `${fastTrack}`,
      BENCH_RUN_ID: runId,
      BENCH_SUITE: suite.name,
      BENCH_LABEL: benchRun.label,
      BENCH_AXIS: JSON.stringify(benchRun.axis),
    },
  });

  let result: RunRecord = null;

  const rl = readline.createInterface({ input: child.stdout! });
  rl.on('line', (line) => {
    if (line.startsWith('BENCH_RESULT ')) {
      result = JSON.parse(line.slice('BENCH_RESULT '.length));
      sink.write(`${JSON.stringify({ type: 'run', ...result })}\n`);
    } else if (line.startsWith('BENCH_TICK ')) {
      sink.write(`${JSON.stringify({ type: 'tick', ...JSON.parse(line.slice('BENCH_TICK '.length)) })}\n`);
    } else {
      console.log(`  | ${line}`);
    }
  });

  const timeoutMs = Math.max(args.runTimeoutMs, estimateRunMs(benchRun.env) * 2 + 120000);
  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    child.kill('SIGKILL');
  }, timeoutMs);

  // The child exits right after printing, so its last lines can still be unread at that point
  const drained = new Promise<void>((resolve) => rl.on('close', resolve));
  const code = await new Promise<number | null>((resolve) => child.on('exit', resolve));
  await drained;
  clearTimeout(timer);

  if (!result) {
    const error = timedOut ? `timed out after ${timeoutMs}ms` : `exited with code ${code} without a BENCH_RESULT`;
    console.error(`  !! ${runId}: ${error}`);
    const failure = { runId, suite: suite.name, label: benchRun.label, axis: benchRun.axis, settings: { fastTrack }, error };
    sink.write(`${JSON.stringify({ type: 'run', ...failure })}\n`);

    return failure;
  }

  return result;
}

const fmt = (v: any, digits = 2) => (typeof v === 'number' ? Number(v.toFixed(digits)) : '—');

/**
 * The workers poll reconcile on a timer that production does not have, and at a low ρ that poll is
 * most of the traffic — it dilutes the headline badly. The submitting process is the honest view.
 */
function mainCallsPerQuery(record: RunRecord): number | null {
  const methods = record?.driverCalls?.main;
  const completed = record?.outcome?.completed;
  if (!methods || !completed) {
    return null;
  }

  return Object.values<any>(methods).reduce((acc, m) => acc + m.started, 0) / completed;
}

/**
 * Where queries drop, per-completed is an efficiency reading and per-pushed is the cost one — the
 * two diverge sharply and quoting only the first turns a flat cost into an apparent saving
 */
function callsPerPushed(record: RunRecord): number | null {
  const total = record?.driverCalls?.total;
  const pushed = record?.outcome?.pushed;

  return total && pushed ? total / pushed : null;
}

function pct(off: number | null | undefined, on: number | null | undefined): string {
  if (!off || on === null || on === undefined) {
    return '—';
  }

  const delta = ((on - off) / off) * 100;

  return `${delta >= 0 ? '+' : ''}${delta.toFixed(1)}%`;
}

function markdownTable(records: RunRecord[]): string {
  const byLabel = new Map<string, { off?: RunRecord, on?: RunRecord }>();
  for (const r of records) {
    const key = `${r.suite}/${r.label}`;
    const pair = byLabel.get(key) || {};
    if (r.settings?.fastTrack) {
      pair.on = r;
    } else {
      pair.off = r;
    }
    byLabel.set(key, pair);
  }

  const header = ['run', 'ρ', 'rate q/s', 'done off→on', 'fail off→on', 'calls/pushed off→on', 'main calls/q off→on', 'Δ main', 'peak calls/s off→on', 'p95 ms off→on', 'elapsed s off→on', 'FT hit%', 'lost'];
  const lines = [`| ${header.join(' | ')} |`, `|${header.map(() => '---').join('|')}|`];

  for (const [key, { off, on }] of byLabel) {
    const sample = on || off;
    const missRate = on?.driverCalls?.fastTrack?.missRate;
    const mainOff = mainCallsPerQuery(off);
    const mainOn = mainCallsPerQuery(on);

    if (off?.error || on?.error) {
      lines.push(`| ${key} | ${[`off: ${off?.error || 'ok'}`, `on: ${on?.error || 'ok'}`].join(', ')} ${header.slice(2).map(() => '| —').join(' ')} |`);
    } else if (sample) {
      lines.push(`| ${[
        key,
        fmt(sample.derived?.actualRho),
        fmt(sample.derived?.actualRateQps),
        `${off?.outcome?.completed ?? '—'}→${on?.outcome?.completed ?? '—'}`,
        `${off?.outcome?.failed?.total ?? '—'}→${on?.outcome?.failed?.total ?? '—'}`,
        `${fmt(callsPerPushed(off))}→${fmt(callsPerPushed(on))}`,
        `${fmt(mainOff)}→${fmt(mainOn)}`,
        pct(mainOff, mainOn),
        `${off?.driverCalls?.peakPerSec ?? '—'}→${on?.driverCalls?.peakPerSec ?? '—'}`,
        `${fmt(off?.latencyMs?.p95, 0)}→${fmt(on?.latencyMs?.p95, 0)}`,
        `${fmt((off?.timing?.elapsedMs ?? 0) / 1000, 1)}→${fmt((on?.timing?.elapsedMs ?? 0) / 1000, 1)}`,
        typeof missRate === 'number' ? `${((1 - missRate) * 100).toFixed(1)}%` : '—',
        `${off?.events?.merged?.['Orphaned execution result'] ?? 0}→${on?.events?.merged?.['Orphaned execution result'] ?? 0}`,
      ].join(' | ')} |`);
    }
  }

  return lines.join('\n');
}

function idleTable(records: RunRecord[]): string | null {
  const idle = records.filter((r) => r.idle?.callsPerSecPerProcess !== null && r.idle?.callsPerSecPerProcess !== undefined);
  if (idle.length === 0) {
    return null;
  }

  const header = ['run', 'fast track', 'workers', 'reconcile ms', 'idle calls', 'calls/s/process'];
  const lines = [`| ${header.join(' | ')} |`, `|${header.map(() => '---').join('|')}|`];

  for (const r of idle) {
    lines.push(`| ${[
      `${r.suite}/${r.label}`,
      r.settings.fastTrack ? 'on' : 'off',
      r.settings.workers,
      r.settings.workerReconcileMs,
      r.idle.driverCalls,
      fmt(r.idle.callsPerSecPerProcess),
    ].join(' | ')} |`);
  }

  return lines.join('\n');
}

function report(records: RunRecord[]) {
  console.log('\n');
  console.log(markdownTable(records));

  const idle = idleTable(records);
  if (idle) {
    console.log('\nIdle floor\n');
    console.log(idle);
  }
}

function dryRun(suites: Suite[], args: Args) {
  let totalMs = 0;
  const header = ['suite', 'run', 'axis', 'ρ', 'queries', 'period s', 'est. s per pass'];
  const lines = [`| ${header.join(' | ')} |`, `|${header.map(() => '---').join('|')}|`];

  for (const suite of suites) {
    for (const benchRun of selectRuns(suite, args)) {
      const est = estimateRunMs(benchRun.env);
      totalMs += (est + args.settleMs) * args.fastTrack.length * args.repeat;

      lines.push(`| ${[
        suite.name,
        benchRun.label,
        JSON.stringify(benchRun.axis),
        fmt(rhoOf(benchRun.env)),
        benchRun.env.BENCH_TOTAL_QUERIES,
        fmt(parseInt(benchRun.env.BENCH_PERIOD_MS || '0', 10) / 1000, 0),
        Math.round(est / 1000),
      ].join(' | ')} |`);
    }
  }

  console.log(lines.join('\n'));
  console.log(`\n${args.fastTrack.length} pass(es) per run — estimated total ${(totalMs / 60000).toFixed(0)} min`);
}

(async () => {
  const args = parseArgs(hideBin(process.argv));

  if (args.list) {
    for (const suite of SUITES) {
      console.log(`${suite.name.padEnd(6)} ${suite.runs.length} runs, ${suite.driver} — ${suite.description}`);
    }

    return;
  }

  if (args.report) {
    const records = fs.readFileSync(args.report, 'utf-8')
      .split('\n')
      .filter((l) => l.trim())
      .map((l) => JSON.parse(l))
      .filter((r) => r.type === 'run');

    report(records);

    return;
  }

  if (args.suites.length === 0) {
    throw new Error('Usage: run-suite.js <suite|all> [--driver=…] [--fast-track=off,on] [--only=…] [--dry-run] | --list | --report <file.jsonl>');
  }

  const suites = args.suites.includes('all')
    ? SUITES.filter((s) => s.name !== 'smoke')
    : args.suites.map(suiteByName);

  if (args.dryRun) {
    dryRun(suites, args);

    return;
  }

  const outPath = args.out || path.resolve(resultsDir(), `${suites.map((s) => s.name).join('+')}-${stamp()}.jsonl`);
  const sink = fs.createWriteStream(outPath, { flags: 'a' });
  console.log(`Writing to ${outPath}`);

  const records: RunRecord[] = [];

  for (const suite of suites) {
    for (const benchRun of selectRuns(suite, args)) {
      for (let pass = 0; pass < args.repeat; pass++) {
        for (const fastTrack of args.fastTrack) {
          const labelled = args.repeat > 1
            ? { ...benchRun, label: `${benchRun.label}#${pass + 1}` }
            : benchRun;
          records.push(await executeRun(suite, labelled, fastTrack, args, sink));
          // Let the previous run's connections and Cube Store's own bookkeeping settle
          await pausePromise(args.settleMs);
        }
      }
    }
  }

  await new Promise<void>((resolve) => sink.end(resolve));

  report(records);
  console.log(`\nResults: ${outPath}`);
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
