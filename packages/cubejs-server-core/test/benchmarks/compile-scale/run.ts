/**
 * Data model compile-time benchmark at 10k-100k cube members.
 *
 *   yarn bench:compile                                  # 10k,20k,50k,100k; jinja
 *   yarn bench:compile --scales=10k,50k --formats=jinja,js --repeat=3
 *   yarn bench:compile --scales=100k --cpu-prof         # writes .cpuprofile files and a hot-spot summary
 *
 * Transpilation is always native (CUBEJS_TRANSPILATION_NATIVE=true), as Cube Cloud runs it.
 * Each run compiles in a fresh process so heap state and module caches of one run do not leak into
 * the next. See README.md for what the model contains and how to read the output.
 */
import fs from 'fs';
import path from 'path';
import { fork } from 'child_process';
import { parseArgs } from 'util';

import { BASE_CUBES, MEMBERS_PER_CUBE, MODEL_FORMATS, ModelFormat } from './model-generator';
import type { WorkerInput, WorkerResult } from './worker';
import { summarizeProfiles } from './profile-summary';

type Run = WorkerResult & { repeat: number };

const MEMBERS_PER_COPY = BASE_CUBES * MEMBERS_PER_CUBE;

function parseScale(s: string): number {
  const m = s.trim().toLowerCase().match(/^(\d+)(k?)$/);
  if (!m) {
    throw new Error(`--scales: expected a member count like 10k or 20000, got "${s}"`);
  }
  const members = Number(m[1]) * (m[2] ? 1000 : 1);
  if (members % MEMBERS_PER_COPY !== 0) {
    throw new Error(`--scales: ${members} is not a multiple of ${MEMBERS_PER_COPY} (${BASE_CUBES} cubes x ${MEMBERS_PER_CUBE} members)`);
  }

  return members / MEMBERS_PER_COPY;
}

const list = <T extends string>(v: string, allowed: readonly T[], flag: string): T[] => v.split(',').map((s) => {
  const t = s.trim() as T;
  if (!allowed.includes(t)) {
    throw new Error(`--${flag}: expected one of ${allowed.join(', ')}, got "${s}"`);
  }
  return t;
});

function runWorker(input: WorkerInput, profileDir?: string): Promise<WorkerResult> {
  return new Promise((resolve, reject) => {
    const execArgv = ['--expose-gc', '--max-old-space-size=16384'];
    if (profileDir) {
      execArgv.push('--cpu-prof', `--cpu-prof-dir=${profileDir}`);
    }
    const child = fork(path.join(__dirname, 'worker.js'), [], {
      execArgv,
      env: {
        ...process.env,
        COMPILE_BENCH_INPUT: JSON.stringify(input),
        CUBEJS_TRANSPILATION_NATIVE: 'true',
        NODE_NO_WARNINGS: '1',
      },
    });
    let result: WorkerResult | undefined;
    child.on('message', (m) => {
      result = m as WorkerResult;
    });
    child.on('error', reject);
    child.on('exit', (code) => (result ? resolve(result) : reject(new Error(`Worker exited with ${code}`))));
  });
}

const fmt = (ms: number) => (ms >= 10000 ? `${(ms / 1000).toFixed(1)}s` : `${Math.round(ms)}ms`);
const median = (xs: number[]) => [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)];

const sum = (xs: number[]) => xs.reduce((a, b) => a + (b || 0), 0);

const freshness = (r: Run) => {
  if (r.format === 'yaml') {
    return '-';
  }

  return r.recompileSawNewMembers ? 'yes' : 'STALE';
};

function printTable(runs: Run[]) {
  const groups = new Map<string, Run[]>();

  for (const r of runs) {
    const key = `${r.format}|${r.copies}`;
    groups.set(key, [...(groups.get(key) || []), r]);
  }

  const header = ['format', 'members', 'dynamic', 'cubes+views', 'cold', 'per 1k', 'transpile', 'evaluate',
    'compilers', 'same ver', 'recompile', 'new members', 'heap', 'rss'];
  const rows = [...groups.values()].map((rs) => {
    const r = rs.find((x) => x.cold.totalMs === median(rs.map((y) => y.cold.totalMs)))!;
    return [
      r.format, `${r.cubeMembers / 1000}k`, `${r.dynamicMembers / 1000}k`, `${r.cubes}+${r.viewsCount}`,
      `${fmt(r.cold.totalMs)}${rs.length > 1 ? ` (n=${rs.length})` : ''}`, fmt(r.cold.totalMs / (r.cubeMembers / 1000)),
      fmt(sum(r.cold.transpileMs)), fmt(sum(r.cold.evaluateMs)), fmt(sum(Object.values(r.cold.compilers))),
      fmt(r.sameVersionMs), fmt(r.recompile.totalMs), freshness(r),
      `${Math.round(r.heapUsedMb)}MB`, `${Math.round(r.maxRssMb)}MB`,
    ];
  });

  const widths = header.map((h, i) => Math.max(h.length, ...rows.map((r) => r[i].length)));
  const line = (cells: string[]) => cells.map((c, i) => c.padEnd(widths[i])).join('  ');
  console.log(`\n${line(header)}\n${widths.map((w) => '-'.repeat(w)).join('  ')}`);
  rows.forEach((r) => console.log(line(r)));

  // Compiler passes ranked by their share at the largest scale measured
  const largest = runs.reduce((a, b) => (b.copies > a.copies ? b : a));

  for (const [label, t] of [['cold compile', largest.cold], ['recompile', largest.recompile]] as const) {
    const top = Object.entries(t.compilers).sort((a, b) => b[1] - a[1]).filter(([, v]) => v >= 1);
    console.log(`\n${label} at ${largest.cubeMembers / 1000}k (${largest.format}), stage:class`);

    for (const [k, v] of top) {
      console.log(`  ${k.padEnd(40)} ${fmt(v).padStart(8)}  ${((v / t.totalMs) * 100).toFixed(1)}%`);
    }
    console.log(`  ${'transpile per stage'.padEnd(40)} ${t.transpileMs.map(fmt).join(' / ')}`);
    console.log(`  ${'evaluate per stage'.padEnd(40)} ${t.evaluateMs.map(fmt).join(' / ')}`);
  }
}

async function main() {
  const { values } = parseArgs({
    options: {
      scales: { type: 'string', default: '10k,20k,50k,100k' },
      formats: { type: 'string', default: 'jinja' },
      repeat: { type: 'string', default: '1' },
      'no-views': { type: 'boolean', default: false },
      'compiler-cache-size': { type: 'string' },
      'cpu-prof': { type: 'boolean', default: false },
      'prof-dir': { type: 'string' },
      out: { type: 'string' },
    },
  });

  const copies = values.scales!.split(',').map(parseScale);
  const formats = list<ModelFormat>(values.formats!, MODEL_FORMATS, 'formats');
  const repeat = Number(values.repeat);
  const profRoot = values['cpu-prof'] ? path.resolve(values['prof-dir'] || 'compile-bench-profiles') : undefined;

  const runs: Run[] = [];

  for (const format of formats) {
    for (const c of copies) {
      for (let i = 0; i < repeat; i++) {
        const profileDir = profRoot && path.join(profRoot, `${format}-${c * MEMBERS_PER_COPY / 1000}k-${i}`);
        const input: WorkerInput = {
          copies: c,
          format,
          views: !values['no-views'],
          compilerCacheSize: values['compiler-cache-size'] ? Number(values['compiler-cache-size']) : undefined,
        };
        const result = await runWorker(input, profileDir);
        const run: Run = { ...result, repeat: i };
        runs.push(run);
        console.log(`${format} ${run.cubeMembers / 1000}k #${i}: cold ${fmt(run.cold.totalMs)}, recompile ${fmt(run.recompile.totalMs)}${run.errors ? ` ERRORS: ${run.errors}` : ''}`);
        if (values.out) {
          fs.appendFileSync(values.out, `${JSON.stringify(run)}\n`);
        }
        if (profileDir) {
          console.log(summarizeProfiles(profileDir, 25));
        }
      }
    }
  }

  printTable(runs);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
