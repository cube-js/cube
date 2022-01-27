import type { SchemaFileRepository } from '../types';

import { CubeValidator } from './CubeValidator';
import { DataSchemaCompiler, DataSchemaCompilerOptions } from './DataSchemaCompiler';
import {
  CubeCheckDuplicatePropTranspiler,
  CubePropContextTranspiler,
  ImportExportTranspiler,
  TranspilerInterface,
  ValidationTranspiler,
} from './transpilers';
import { Funnels, RefreshKeys, Reflection } from '../extensions';
import { CubeSymbols } from './CubeSymbols';
import { CubeDictionary } from './CubeDictionary';
import { CubeEvaluator } from './CubeEvaluator';
import { ContextEvaluator } from './ContextEvaluator';
import { JoinGraph } from './JoinGraph';
import { CubeToMetaTransformer } from './CubeToMetaTransformer';
import { CompilerCache } from './CompilerCache';

export interface PrepareCompilerOptions extends DataSchemaCompilerOptions {
  adapter?: string;
  allowJsDuplicatePropsInSchema?: boolean;
  headCommitId?: string;
  maxQueryCacheAge?: number;
  maxQueryCacheSize?: number;
}

export const prepareCompiler = (repo: SchemaFileRepository, options: PrepareCompilerOptions) => {
  const {
    allowJsDuplicatePropsInSchema,
    maxQueryCacheSize,
    maxQueryCacheAge
  } = options;

  const cubeDictionary = new CubeDictionary();
  const cubeSymbols = new CubeSymbols();
  const cubeValidator = new CubeValidator(cubeSymbols);
  const cubeEvaluator = new CubeEvaluator(cubeValidator);
  const contextEvaluator = new ContextEvaluator(cubeEvaluator);
  const joinGraph = new JoinGraph(cubeValidator, cubeEvaluator);
  const metaTransformer = new CubeToMetaTransformer(cubeValidator, cubeEvaluator, contextEvaluator, joinGraph);
  const compilerCache = new CompilerCache({ maxQueryCacheSize, maxQueryCacheAge });

  const transpilers: TranspilerInterface[] = [
    new ValidationTranspiler(),
    new ImportExportTranspiler(),
    new CubePropContextTranspiler(cubeSymbols, cubeDictionary),
  ];

  if (!allowJsDuplicatePropsInSchema) {
    transpilers.push(new CubeCheckDuplicatePropTranspiler());
  }

  const compiler = new DataSchemaCompiler(repo, Object.assign({}, {
    cubeNameCompilers: [cubeDictionary],
    preTranspileCubeCompilers: [cubeSymbols, cubeValidator],
    transpilers,
    cubeCompilers: [cubeEvaluator, joinGraph, metaTransformer],
    contextCompilers: [contextEvaluator],
    cubeFactory: cubeSymbols.createCube.bind(cubeSymbols),
    compilerCache,
    extensions: {
      Funnels,
      RefreshKeys,
      Reflection
    },
    compileContext: options.compileContext,
    standalone: options.standalone
  }, options));

  return {
    compiler,
    metaTransformer,
    cubeEvaluator,
    contextEvaluator,
    joinGraph,
    compilerCache,
    headCommitId: options.headCommitId
  };
};

export const compile = (repo: SchemaFileRepository, options: PrepareCompilerOptions): Promise<ReturnType<typeof prepareCompiler>> => {
  const compilers = prepareCompiler(repo, options);
  return compilers.compiler.compile().then(
    () => compilers
  );
};
