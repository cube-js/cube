const QueryBuilder = require('@cubejs-backend/schema-compiler/adapter/QueryBuilder');
const PrepareCompiler = require('@cubejs-backend/schema-compiler/compiler/PrepareCompiler');
const crypto = require('crypto');

class CompilerApi {
  constructor(repository, dbType, options) {
    this.repository = repository;
    this.dbType = dbType;
    this.options = options || {};
    this.allowNodeRequire = options.allowNodeRequire == null ? true : options.allowNodeRequire;
    this.logger = this.options.logger;
    this.preAggregationsSchema = this.options.preAggregationsSchema;
    this.allowUngroupedWithoutPrimaryKey = this.options.allowUngroupedWithoutPrimaryKey;
    this.schemaVersion = this.options.schemaVersion;
    this.compileContext = options.compileContext;
  }

  async getCompilers(options) {
    const { requestId } = options || {};
    let compilerVersion = (
      this.schemaVersion && await this.schemaVersion() ||
      'default_schema_version'
    ).toString();
    if (this.options.devServer) {
      const files = await this.repository.dataSchemaFiles();
      compilerVersion += `_${crypto.createHash('md5').update(JSON.stringify(files)).digest("hex")}`;
    }
    if (!this.compilers || this.compilerVersion !== compilerVersion) {
      this.logger(this.compilers ? 'Recompiling schema' : 'Compiling schema', {
        version: compilerVersion,
        requestId
      });
      // TODO check if saving this promise can produce memory leak?
      this.compilers = PrepareCompiler.compile(this.repository, {
        allowNodeRequire: this.allowNodeRequire,
        compileContext: this.compileContext
      });
      this.compilerVersion = compilerVersion;
    }
    return this.compilers;
  }

  getDbType(dataSource) {
    if (typeof this.dbType === 'function') {
      return this.dbType({ dataSource: dataSource || 'default' });
    }
    return this.dbType;
  }

  async getSql(query, options) {
    options = options || {};
    const { includeDebugInfo } = options;
    const dbType = this.getDbType('default');
    const compilers = await this.getCompilers({ requestId: query.requestId });
    let sqlGenerator = this.createQuery(compilers, dbType, query);
    if (!sqlGenerator) {
      throw new Error(`Unknown dbType: ${dbType}`);
    }

    const dataSource = compilers.compiler.withQuery(sqlGenerator, () => sqlGenerator.dataSource);

    if (dataSource !== 'default' && dbType !== this.getDbType(dataSource)) {
      // TODO consider more efficient way than instantiating query
      sqlGenerator = this.createQuery(compilers, this.getDbType(dataSource), query);
    }

    return compilers.compiler.withQuery(sqlGenerator, () => ({
      external: sqlGenerator.externalPreAggregationQuery(),
      sql: sqlGenerator.buildSqlAndParams(),
      timeDimensionAlias: sqlGenerator.timeDimensions[0] && sqlGenerator.timeDimensions[0].unescapedAliasName(),
      timeDimensionField: sqlGenerator.timeDimensions[0] && sqlGenerator.timeDimensions[0].dimension,
      order: sqlGenerator.order,
      cacheKeyQueries: sqlGenerator.cacheKeyQueries(),
      preAggregations: sqlGenerator.preAggregations.preAggregationsDescription(),
      dataSource: sqlGenerator.dataSource,
      aliasNameToMember: sqlGenerator.aliasNameToMember,
      rollupMatchResults: includeDebugInfo ?
        sqlGenerator.preAggregations.rollupMatchResultDescriptions() : undefined,
      canUseTransformedQuery: sqlGenerator.preAggregations.canUseTransformedQuery()
    }));
  }

  async scheduledPreAggregations() {
    const { cubeEvaluator } = await this.getCompilers();
    return cubeEvaluator.scheduledPreAggregations();
  }

  createQuery(compilers, dbType, query) {
    return QueryBuilder.query(
      compilers,
      dbType, {
        ...query,
        externalDbType: this.options.externalDbType,
        preAggregationsSchema: this.preAggregationsSchema,
        allowUngroupedWithoutPrimaryKey: this.allowUngroupedWithoutPrimaryKey
      }
    );
  }

  async metaConfig(options) {
    return (await this.getCompilers(options)).metaTransformer.cubes;
  }
}

module.exports = CompilerApi;
