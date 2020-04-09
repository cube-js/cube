const CubeValidator = require('./CubeValidator');
const DataSchemaCompiler = require('./DataSchemaCompiler');
const CubePropContextTranspiler = require('./CubePropContextTranspiler');
const ImportExportTranspiler = require('./ImportExportTranspiler');
const CubeSymbols = require('./CubeSymbols');
const CubeDictionary = require('./CubeDictionary');
const CubeEvaluator = require('./CubeEvaluator');
const ContextEvaluator = require('./ContextEvaluator');
const DashboardTemplateEvaluator = require('./DashboardTemplateEvaluator');
const JoinGraph = require('./JoinGraph');
const Funnels = require('../extensions/Funnels');
const RefreshKeys = require('../extensions/RefreshKeys');
const Reflection = require('../extensions/Reflection');
const CubeToMetaTransformer = require('./CubeToMetaTransformer');
const CompilerCache = require('./CompilerCache');

exports.compile = (repo, options) => {
  const compilers = exports.prepareCompiler(repo, options);
  return compilers.compiler.compile().then(
    () => compilers
  );
};

exports.prepareCompiler = (repo, options) => {
  const cubeDictionary = new CubeDictionary();
  const cubeSymbols = new CubeSymbols();
  const cubeValidator = new CubeValidator(cubeSymbols);
  const cubeEvaluator = new CubeEvaluator(cubeValidator);
  const contextEvaluator = new ContextEvaluator(cubeEvaluator);
  const joinGraph = new JoinGraph(cubeValidator, cubeEvaluator);
  const dashboardTemplateEvaluator = new DashboardTemplateEvaluator(cubeEvaluator);
  const metaTransformer = new CubeToMetaTransformer(cubeValidator, cubeEvaluator, contextEvaluator, joinGraph);
  const { maxQueryCacheSize, maxQueryCacheAge } = options;
  const compilerCache = new CompilerCache({ maxQueryCacheSize, maxQueryCacheAge });
  const compiler = new DataSchemaCompiler(repo, Object.assign({}, {
    cubeNameCompilers: [cubeDictionary],
    preTranspileCubeCompilers: [cubeSymbols, cubeValidator],
    transpilers: [new ImportExportTranspiler(), new CubePropContextTranspiler(cubeSymbols, cubeDictionary)],
    cubeCompilers: [cubeEvaluator, joinGraph, metaTransformer],
    contextCompilers: [contextEvaluator],
    dashboardTemplateCompilers: [dashboardTemplateEvaluator],
    cubeFactory: cubeSymbols.createCube.bind(cubeSymbols),
    compilerCache,
    extensions: {
      Funnels,
      RefreshKeys,
      Reflection
    },
    compileContext: options.compileContext
  }, options));
  return {
    compiler,
    metaTransformer,
    cubeEvaluator,
    contextEvaluator,
    dashboardTemplateEvaluator,
    joinGraph,
    compilerCache,
    headCommitId: options.headCommitId
  };
};
