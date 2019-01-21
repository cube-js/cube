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
const QueryBuilder = require('../adapter/QueryBuilder');
const Funnels = require('../extensions/Funnels');
const CubeToMetaTransformer = require('./CubeToMetaTransformer');

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
  const query = QueryBuilder.query(
    { cubeEvaluator, joinGraph },
    options.adapter,
    {}
  );
  const metaTransformer = new CubeToMetaTransformer(cubeValidator, cubeEvaluator, contextEvaluator, joinGraph, query);
  const compiler = new DataSchemaCompiler(repo, Object.assign({}, {
    cubeNameCompilers: [cubeDictionary],
    preTranspileCubeCompilers: [cubeSymbols, cubeValidator],
    transpilers: [new ImportExportTranspiler(), new CubePropContextTranspiler(cubeSymbols, cubeDictionary)],
    cubeCompilers: [cubeEvaluator, joinGraph, metaTransformer],
    contextCompilers: [contextEvaluator],
    dashboardTemplateCompilers: [dashboardTemplateEvaluator],
    cubeFactory: cubeSymbols.createCube.bind(cubeSymbols),
    extensions: {
      Funnels
    }
  }, options));
  return {
    compiler,
    metaTransformer,
    cubeEvaluator,
    contextEvaluator,
    dashboardTemplateEvaluator,
    joinGraph,
    headCommitId: options.headCommitId
  };
};
