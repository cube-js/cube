/**
 * Compiles one generated data model in a fresh process and reports timings to the parent.
 * The process is started by run.ts; running it directly prints the result instead.
 */
import { performance } from 'perf_hooks';

import { prepareCompiler } from '../../../src/compiler/PrepareCompiler';
import { DataSchemaCompiler } from '../../../src/compiler/DataSchemaCompiler';
import { generateModel, ModelFormat } from './model-generator';

export type WorkerInput = {
  copies: number;
  format: ModelFormat;
  views: boolean;
};

export type WorkerResult = WorkerInput & {
  cubes: number;
  viewsCount: number;
  cubeMembers: number;
  files: number;
  sourceBytes: number;
  generateMs: number;
  totalMs: number;
  /** Stage (0-3) wall time before the stage's files are evaluated: transpilation, Jinja, YAML */
  transpileMs: number[];
  /** Stage wall time evaluating transpiled files in the VM context */
  evaluateMs: number[];
  /** `${stage}:${CompilerClass}` to time spent in its compile() */
  compilers: Record<string, number>;
  heapUsedMb: number;
  maxRssMb: number;
  errors?: string;
};

type Timed = { compile: (...args: any[]) => any };

function timeCompiler(target: Timed, label: () => string, sink: Record<string, number>) {
  const original = target.compile.bind(target);
  target.compile = (...args: any[]) => {
    const start = performance.now();
    const record = () => {
      const key = label();
      sink[key] = (sink[key] || 0) + performance.now() - start;
    };
    const res = original(...args);
    if (res && typeof res.then === 'function') {
      return res.finally(record);
    }
    record();
    return res;
  };
}

export async function runOnce(input: WorkerInput): Promise<WorkerResult> {
  const generateStart = performance.now();
  const model = generateModel(input.copies, input.format, { views: input.views });
  const generateMs = performance.now() - generateStart;

  const { compiler } = prepareCompiler({
    localPath: () => __dirname,
    dataSchemaFiles: async () => model.files,
  }, { adapter: 'postgres', compileContext: model.compileContext });

  const compilers: Record<string, number> = {};
  const transpileMs: number[] = [];
  const evaluateMs: number[] = [];
  let stage = -1;
  let stageStart = 0;

  // Every compiler pass of the pipeline, keyed by the stage it ran in
  const lists = ['cubeNameCompilers', 'preTranspileCubeCompilers', 'viewCompilers', 'cubeCompilers',
    'contextCompilers', 'viewGroupCompilers', 'metaCompilers'];
  const passes = new Set<Timed>([
    ...lists.flatMap((list) => (compiler as any)[list] as Timed[]),
    (compiler as any).viewCompilationGate,
  ]);
  const label = (c: Timed) => () => `${stage}:${c.constructor.name}`;
  passes.forEach((c) => timeCompiler(c, label(c), compilers));

  // compileCubeFiles marks the end of a stage's transpilation; its compileFile calls evaluate the
  // transpiled files, then it hands over to the compilers timed above
  const proto = DataSchemaCompiler.prototype as any;
  const { compileCubeFiles, compileFile } = proto;
  const self = compiler as any;
  self.compileCubeFiles = function (...args: any[]) {
    stage += 1;
    transpileMs[stage] = performance.now() - stageStart;
    evaluateMs[stage] = 0;
    return compileCubeFiles.apply(this, args).finally(() => {
      stageStart = performance.now();
    });
  };
  self.compileFile = function (...args: any[]) {
    const start = performance.now();
    try {
      return compileFile.apply(this, args);
    } finally {
      evaluateMs[stage] += performance.now() - start;
    }
  };

  const start = performance.now();
  stageStart = start;
  let errors: string | undefined;
  try {
    await compiler.compile();
  } catch (e: any) {
    errors = String(e?.message || e).slice(0, 4000);
  }
  const totalMs = performance.now() - start;

  if (global.gc) {
    global.gc();
  }

  return {
    ...input,
    cubes: model.cubes,
    viewsCount: model.views,
    cubeMembers: model.cubeMembers,
    files: model.files.length,
    sourceBytes: model.files.reduce((n, f) => n + f.content.length, 0),
    generateMs,
    totalMs,
    transpileMs,
    evaluateMs,
    compilers,
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
