import { SchemaFileRepository } from '@cubejs-backend/shared';
import { CubeValidator } from './CubeValidator';
import { DataSchemaCompiler } from './DataSchemaCompiler';
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
import { YamlCompiler } from './YamlCompiler';

export type PrepareCompilerOptions = {
  allowNodeRequire?: boolean;
  allowJsDuplicatePropsInSchema?: boolean;
  maxQueryCacheSize?: number;
  maxQueryCacheAge?: number;
  compileContext?: any;
  standalone?: boolean;
  headCommitId?: string;
  adapter?: string;
};

export const prepareCompiler = (repo: SchemaFileRepository, options: PrepareCompilerOptions = {}) => {
  const cubeDictionary = new CubeDictionary();
  const cubeSymbols = new CubeSymbols();
  const cubeValidator = new CubeValidator(cubeSymbols);
  const cubeEvaluator = new CubeEvaluator(cubeValidator);
  const contextEvaluator = new ContextEvaluator(cubeEvaluator);
  const joinGraph = new JoinGraph(cubeValidator, cubeEvaluator);
  const metaTransformer = new CubeToMetaTransformer(cubeValidator, cubeEvaluator, contextEvaluator, joinGraph);
  const { maxQueryCacheSize, maxQueryCacheAge } = options;
  const compilerCache = new CompilerCache({ maxQueryCacheSize, maxQueryCacheAge });
  const yamlCompiler = new YamlCompiler(cubeSymbols, cubeDictionary);

  const transpilers: TranspilerInterface[] = [
    new ValidationTranspiler(),
    new ImportExportTranspiler(),
    new CubePropContextTranspiler(cubeSymbols, cubeDictionary),
  ];

  if (!options.allowJsDuplicatePropsInSchema) {
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
    standalone: options.standalone,
    yamlCompiler
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

export const compile = (repo: SchemaFileRepository, options?: PrepareCompilerOptions) => {
  const compilers = prepareCompiler(repo, options);
  return compilers.compiler.compile().then(
    () => compilers
  );
};
