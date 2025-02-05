import { parentPort, workerData } from 'worker_threads';

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

type FileContent = {
  fileName: string;
  content: string;
  transpilers: string[];
};

const cubeDictionary = new LightweightNodeCubeDictionary(workerData.cubeNames);
const cubeSymbols = new LightweightSymbolResolver(workerData.cubeSymbolsNames);
const errorsReport = new ErrorReporter(null, []);

const transpilers = {
  ValidationTranspiler: new ValidationTranspiler(),
  ImportExportTranspiler: new ImportExportTranspiler(),
  CubeCheckDuplicatePropTranspiler: new CubeCheckDuplicatePropTranspiler(),
  CubePropContextTranspiler: new CubePropContextTranspiler(cubeSymbols, cubeDictionary, cubeSymbols),
};

if (parentPort) {
  parentPort.on('message', (file: FileContent) => {
    const ast = parse(
      file.content,
      {
        sourceFilename: file.fileName,
        sourceType: 'module',
        plugins: ['objectRestSpread']
      },
    );

    file.transpilers.forEach(transpilerName => {
      if (transpilers[transpilerName]) {
        errorsReport.inFile(file);
        babelTraverse(ast, transpilers[transpilerName].traverseObject(errorsReport));
        errorsReport.exitFile();
      } else {
        throw new Error(`Transpiler ${transpilerName} not supported`);
      }
    });

    const content = babelGenerator(ast, {}, file.content).code;

    // @ts-ignore
    parentPort.postMessage({
      content,
      errors: errorsReport.getErrors(),
      warnings: errorsReport.getWarnings()
    });
  });
}
