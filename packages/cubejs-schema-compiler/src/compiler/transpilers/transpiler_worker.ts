import workerpool from 'workerpool';
import { parse } from '@babel/parser';
import babelGenerator from '@babel/generator';
import babelTraverse from '@babel/traverse';

import { ValidationTranspiler } from './ValidationTranspiler';
import { ImportExportTranspiler } from './ImportExportTranspiler';
import { CubeCheckDuplicatePropTranspiler } from './CubeCheckDuplicatePropTranspiler';
import { CubePropContextTranspiler } from './CubePropContextTranspiler';
import { ErrorReporter } from '../ErrorReporter';
import { LightweightSymbolResolver } from './LightweightSymbolResolver';
import { LightweightNodeCubeDictionary } from './LightweightNodeCubeDictionary';

type TransferContent = {
  fileName: string;
  content: string;
  transpilers: string[];
  cubeNames: string[];
  cubeSymbolsNames: Record<string, Record<string, boolean>>;
};

const cubeDictionary = new LightweightNodeCubeDictionary();
const cubeSymbols = new LightweightSymbolResolver();
const errorsReport = new ErrorReporter(null, []);

const transpilers = {
  ValidationTranspiler: new ValidationTranspiler(),
  ImportExportTranspiler: new ImportExportTranspiler(),
  CubeCheckDuplicatePropTranspiler: new CubeCheckDuplicatePropTranspiler(),
  CubePropContextTranspiler: new CubePropContextTranspiler(cubeSymbols, cubeDictionary, cubeSymbols),
};

const transpile = (data: TransferContent) => {
  cubeDictionary.setCubeNames(data.cubeNames);
  cubeSymbols.setSymbols(data.cubeSymbolsNames);

  const ast = parse(
    data.content,
    {
      sourceFilename: data.fileName,
      sourceType: 'module',
      plugins: ['objectRestSpread']
    },
  );

  data.transpilers.forEach(transpilerName => {
    if (transpilers[transpilerName]) {
      errorsReport.inFile(data);
      babelTraverse(ast, transpilers[transpilerName].traverseObject(errorsReport));
      errorsReport.exitFile();
    } else {
      throw new Error(`Transpiler ${transpilerName} not supported`);
    }
  });

  const content = babelGenerator(ast, {}, data.content).code;

  return {
    content,
    errors: errorsReport.getErrors(),
    warnings: errorsReport.getWarnings()
  };
};

workerpool.worker({
  transpile,
});
