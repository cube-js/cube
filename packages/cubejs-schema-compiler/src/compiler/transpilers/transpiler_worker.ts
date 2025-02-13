import workerpool from 'workerpool';
import { transformSync } from '@swc/core';

type TransferContent = {
  fileName: string;
  content: string;
  transpilers: string[];
  cubeNames: string[];
  cubeSymbols: Record<string, Record<string, boolean>>;
  contextSymbols: Record<string, string>;
};

type TranspilerPlugin = [string, Record<string, any>];


const transpilers = {
  ValidationTranspiler:
    (_data: TransferContent): TranspilerPlugin => ['@cubejs-backend/validation-transpiler-swc-plugin', {}],
  ImportExportTranspiler:
    (_data: TransferContent): TranspilerPlugin => ['@cubejs-backend/import-export-transpiler-swc-plugin', {}],
  CubeCheckDuplicatePropTranspiler:
    (_data: TransferContent): TranspilerPlugin => ['@cubejs-backend/check-dup-prop-transpiler-swc-plugin', {}],
  CubePropContextTranspiler:
    (data: TransferContent): TranspilerPlugin => ['@cubejs-backend/cube-prop-ctx-transpiler-swc-plugin', {
      cubeNames: data.cubeNames,
      cubeSymbols: data.cubeSymbols,
      contextSymbols: data.contextSymbols,
    }],
};

const transpile = (data: TransferContent) => {
  const transpilersConfigs = data.transpilers.map(transpilerName => {
    const ts = transpilers[transpilerName];
    if (ts) {
      return ts(data);
    } else {
      throw new Error(`Transpiler ${ts} not supported`);
    }
  });

  // We're already in dedicated worker, no need to use async here
  const result = transformSync(data.content,
    {
      filename: data.fileName,
      jsc: {
        target: 'es2015',
        experimental: {
          plugins: transpilersConfigs,
        },
      },
      swcrc: false,
      inputSourceMap: false,
      isModule: true,
    });

  return {
    content: result.code,
  };
};

workerpool.worker({
  transpile,
});
