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
import { IIFETranspiler } from './IIFETranspiler';

type TransferContent = {
  fileName: string;
  content: string;
  transpilers: string[];
  cubeNames: string[];
  cubeSymbols: Record<string, Record<string, boolean>>;
};

const cubeDictionary = new LightweightNodeCubeDictionary();
const cubeSymbols = new LightweightSymbolResolver();
const errorsReport = new ErrorReporter(null, []);

const transpilers = {
  ValidationTranspiler: new ValidationTranspiler(),
  ImportExportTranspiler: new ImportExportTranspiler(),
  CubeCheckDuplicatePropTranspiler: new CubeCheckDuplicatePropTranspiler(),
  CubePropContextTranspiler: new CubePropContextTranspiler(cubeSymbols, cubeDictionary, cubeSymbols),
  IIFETranspiler: new IIFETranspiler(),
};

const transpile = (data: TransferContent) => {
  cubeDictionary.setCubeNames(data.cubeNames);
  cubeSymbols.setSymbols(data.cubeSymbols);

  const ast = parse(
    data.content,
    {
      sourceFilename: data.fileName,
      sourceType: 'module',
      plugins: ['objectRestSpread']
    },
  );

  errorsReport.inFile(data);
  data.transpilers.forEach(transpilerName => {
    if (transpilers[transpilerName]) {
      babelTraverse(ast, transpilers[transpilerName].traverseObject(errorsReport));
    } else {
      throw new Error(`Transpiler ${transpilerName} not supported`);
    }
  });
  errorsReport.exitFile();

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
