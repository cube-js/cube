import workerpool from 'workerpool';
import { transformSync } from '@swc/core';

import { ErrorReporter } from '../ErrorReporter';

type TransferContent = {
  fileName: string;
  content: string;
  transpilers: string[];
  cubeNames: string[];
  cubeSymbolsNames: Record<string, Record<string, boolean>>;
};

type TranspilerPlugin = [string, Record<string, any>];

const errorsReport = new ErrorReporter(null, []);

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
      cubeSymbols: data.cubeSymbolsNames
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
        experimental: {
          plugins: transpilersConfigs,
        },
      },
    });

  return {
    content: result.code,
    errors: errorsReport.getErrors(),
    warnings: errorsReport.getWarnings()
  };
};

workerpool.worker({
  transpile,
});
