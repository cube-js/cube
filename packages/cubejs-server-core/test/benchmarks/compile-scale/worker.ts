/**
 * Compiles one generated data model in a fresh process and reports timings to the parent.
 * The process is started by run.ts; running it directly prints the result instead.
 *
 * The model is compiled through CompilerApi the way CubejsServerCore.getCompilerApi drives it:
 * one CompilerApi per app, whose schemaVersion is read from the request's security context. The
 * run then asks for the same version again, and finally publishes a new revision of the member
 * API and bumps the version, which is how a deployment tells Cube that dynamic members changed.
 */
import fs from 'fs';
import os from 'os';
import path from 'path';
import { performance } from 'perf_hooks';
import { DataSchemaCompiler } from '@cubejs-backend/schema-compiler/dist/src/compiler/DataSchemaCompiler';
import { CubeSymbols } from '@cubejs-backend/schema-compiler/dist/src/compiler/CubeSymbols';
import { CubeValidator } from '@cubejs-backend/schema-compiler/dist/src/compiler/CubeValidator';
import { CubeEvaluator } from '@cubejs-backend/schema-compiler/dist/src/compiler/CubeEvaluator';
import { CubeDictionary } from '@cubejs-backend/schema-compiler/dist/src/compiler/CubeDictionary';
import { JoinGraph } from '@cubejs-backend/schema-compiler/dist/src/compiler/JoinGraph';
import { CubeToMetaTransformer } from '@cubejs-backend/schema-compiler/dist/src/compiler/CubeToMetaTransformer';
import { ViewCompilationGate } from '@cubejs-backend/schema-compiler/dist/src/compiler/ViewCompilationGate';
import { PostgresQuery } from '@cubejs-backend/schema-compiler';

import { CompilerApi } from '../../../src/core/CompilerApi';
import { buildMemberApi, generateModel, ModelFormat } from './model-generator';
import { DOMAINS } from './domains';

export type WorkerInput = {
  copies: number;
  format: ModelFormat;
  views: boolean;
  /** CompilerApi's compilerCacheSize: entries kept of compiled scripts, YAML and Jinja output */
  compilerCacheSize?: number;
};

export type CompileTimings = {
  totalMs: number;
  /** Stage wall time before the stage's files are evaluated: transpilation, Jinja, YAML */
  transpileMs: number[];
  /** Stage wall time evaluating transpiled files in the VM context */
  evaluateMs: number[];
  /** `${stage}:${CompilerClass}` to time spent in its compile() */
  compilers: Record<string, number>;
};

export type WorkerResult = WorkerInput & {
  cubes: number;
  viewsCount: number;
  cubeMembers: number;
  dynamicMembers: number;
  files: number;
  sourceBytes: number;
  cold: CompileTimings;
  /** getCompilers() for the version already compiled */
  sameVersionMs: number;
  /** getCompilers() after the member API moved to a new revision and the version was bumped */
  recompile: CompileTimings;
  /** Whether the recompiled model shows the new member API revision */
  recompileSawNewMembers: boolean;
  heapUsedMb: number;
  maxRssMb: number;
  errors?: string;
};

type Timings = { stage: number; stageStart: number } & CompileTimings;

let current: Timings | undefined;

const newTimings = (): Timings => ({ stage: -1, stageStart: performance.now(), totalMs: 0, transpileMs: [], evaluateMs: [], compilers: {} });

/** Wraps compile() of a compiler class; `active` keeps a subclass's super.compile() from counting twice */
function timeCompile(proto: any, active: Set<object>) {
  const original = proto.compile;
  proto.compile = function compile(...args: any[]) {
    const timings = current;
    if (!timings || active.has(this)) {
      return original.apply(this, args);
    }

    const start = performance.now();
    active.add(this);
    const record = () => {
      active.delete(this);
      const key = `${timings.stage}:${this.constructor.name}`;
      timings.compilers[key] = (timings.compilers[key] || 0) + performance.now() - start;
    };

    try {
      const res = original.apply(this, args);
      if (res && typeof res.then === 'function') {
        return res.finally(record);
      }

      record();
      return res;
    } catch (e) {
      record();
      throw e;
    }
  };
}

/** Times every compiler pass and the transpile and evaluate part of every stage */
function instrument() {
  const active = new Set<object>();
  [CubeDictionary, CubeSymbols, CubeValidator, ViewCompilationGate, CubeEvaluator, JoinGraph, CubeToMetaTransformer]
    .forEach((cls) => timeCompile(cls.prototype, active));

  // compileCubeFiles marks the end of a stage's transpilation; its compileFile calls evaluate the
  // transpiled files, then it hands over to the compilers timed above
  const proto = DataSchemaCompiler.prototype as any;
  const { compileCubeFiles, compileFile } = proto;
  proto.compileCubeFiles = function (...args: any[]) {
    const timings = current!;
    timings.stage += 1;
    timings.transpileMs[timings.stage] = performance.now() - timings.stageStart;
    timings.evaluateMs[timings.stage] = 0;
    return compileCubeFiles.apply(this, args).finally(() => {
      timings.stageStart = performance.now();
    });
  };
  proto.compileFile = function (...args: any[]) {
    const start = performance.now();

    try {
      return compileFile.apply(this, args);
    } finally {
      current!.evaluateMs[current!.stage] += performance.now() - start;
    }
  };
}

async function timeGetCompilers(api: CompilerApi): Promise<CompileTimings> {
  current = newTimings();
  const start = performance.now();

  try {
    await api.getCompilers();
  } finally {
    current.totalMs = performance.now() - start;
  }
  const { stage, stageStart, ...timings } = current;
  current = undefined;
  return timings;
}

const writeMemberApi = (file: string, revision: number) => {
  // Through a rename, so a reader never sees a half-written file
  fs.writeFileSync(`${file}.tmp`, JSON.stringify(buildMemberApi(revision)));
  fs.renameSync(`${file}.tmp`, file);
};

/**
 * Plans a query over dynamic members of the first fact cube, one of them reaching into a joined
 * cube, and fails on SQL that still carries unresolved `{...}` references
 */
function checkSql(compilers: any, copy: number) {
  const fact = `${DOMAINS[0].entities[0].name}_${copy}`;
  const cube = compilers.cubeEvaluator.cubeFromPath(fact);
  const pick = (members: Record<string, any>, pred: (m: any) => boolean) => Object.keys(members).find((n) => pred(members[n]));
  const dimension = pick(cube.dimensions, (d) => /Loaded from the member API/.test(d.description || '') && d.type === 'string');
  const measure = pick(cube.measures, (m) => /Loaded from the member API/.test(m.description || '') && m.type === 'sum');
  const query = new PostgresQuery(compilers, {
    measures: [`${fact}.count`, ...(measure ? [`${fact}.${measure}`] : [])],
    dimensions: dimension ? [`${fact}.${dimension}`] : [],
  });
  const [sql] = query.buildSqlAndParams();
  if (/\{[\w.@]+\}/.test(sql)) {
    throw new Error(`Unresolved member references in generated SQL:\n${sql}`);
  }
}

export async function runOnce(input: WorkerInput): Promise<WorkerResult> {
  instrument();

  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'compile-bench-'));
  const memberApiFile = path.join(dir, 'member-api.json');
  writeMemberApi(memberApiFile, 1);
  const model = generateModel(input.copies, input.format, { views: input.views, memberApiFile });

  // What CubejsServerCore does per app: the compile context is the request context the
  // CompilerApi was created for, and schemaVersion is re-bound to each request's context
  const schemaVersion = ({ securityContext }) => securityContext.schemaVersion;
  const context = { securityContext: { copies: input.copies, schemaVersion: 'v1' } };
  const api = new CompilerApi(
    { localPath: () => dir, dataSchemaFiles: async () => model.files },
    async () => 'postgres',
    { compileContext: context, schemaVersion: () => schemaVersion(context), compilerCacheSize: input.compilerCacheSize },
  );

  let errors: string | undefined;
  const fail = (e: any) => {
    errors = String(e?.stack || e?.message || e).slice(0, 4000);
  };

  let cold = newTimings() as CompileTimings;
  let sameVersionMs = 0;
  let recompile = newTimings() as CompileTimings;
  let recompileSawNewMembers = false;

  try {
    cold = await timeGetCompilers(api);
    checkSql(await api.getCompilers(), input.copies - 1);

    const start = performance.now();
    await api.getCompilers();
    sameVersionMs = performance.now() - start;

    writeMemberApi(memberApiFile, 2);
    const request = { securityContext: { ...context.securityContext, schemaVersion: 'v2' } };
    api.schemaVersion = () => schemaVersion(request);
    recompile = await timeGetCompilers(api);

    const compilers = await api.getCompilers();
    checkSql(compilers, 0);
    const fact = compilers.cubeEvaluator.cubeFromPath(`${DOMAINS[0].entities[0].name}_0`);
    recompileSawNewMembers = input.format === 'yaml' || Object.values<any>(fact.dimensions)
      .some((d) => /revision 2$/.test(d.description || ''));
  } catch (e) {
    fail(e);
  }

  if (global.gc) {
    global.gc();
  }

  fs.rmSync(dir, { recursive: true, force: true });

  return {
    ...input,
    cubes: model.cubes,
    viewsCount: model.views,
    cubeMembers: model.cubeMembers,
    dynamicMembers: model.dynamicMembers,
    files: model.files.length,
    sourceBytes: model.files.reduce((n, f) => n + f.content.length, 0),
    cold,
    sameVersionMs,
    recompile,
    recompileSawNewMembers,
    heapUsedMb: process.memoryUsage().heapUsed / 1024 / 1024,
    maxRssMb: process.resourceUsage().maxRSS / 1024,
    errors,
  };
}

if (require.main === module) {
  const input: WorkerInput = JSON.parse(process.env.COMPILE_BENCH_INPUT || '{"copies":1,"format":"jinja","views":true}');
  runOnce(input).then((result) => {
    if (process.send) {
      process.send(result, () => process.exit(0));
    } else {
      console.log(JSON.stringify(result, null, 2));
      process.exit(0);
    }
  }, (e) => {
    console.error(e);
    process.exit(1);
  });
}
