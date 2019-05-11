const QueryBuilder = require('@cubejs-backend/schema-compiler/adapter/QueryBuilder');
const PrepareCompiler = require('@cubejs-backend/schema-compiler/compiler/PrepareCompiler');
const crypto = require('crypto');

class CompilerApi {
  constructor(repository, dbType, options) {
    this.repository = repository;
    this.dbType = dbType;
    this.options = options || {};
    this.logger = this.options.logger;
  }

  async getCompilers() {
    let compilerVersion = (
      this.options.schemaVersion && this.options.schemaVersion() ||
      'default_schema_version'
    ).toString();
    if (this.options.devServer) {
      const files = await this.repository.dataSchemaFiles();
      compilerVersion += `_${crypto.createHash('md5').update(JSON.stringify(files)).digest("hex")}`;
    }
    if (!this.compilers || this.compilerVersion !== compilerVersion) {
      this.logger('Compiling schema', { version: compilerVersion });
      // TODO check if saving this promise can produce memory leak?
      this.compilers = PrepareCompiler.compile(this.repository, { adapter: this.dbType });
      this.compilerVersion = compilerVersion;
    }
    return this.compilers;
  }

  async getSql(query) {
    const sqlGenerator = QueryBuilder.query(
      await this.getCompilers(),
      this.dbType, {
        ...query,
        externalDbType: this.options.externalDbType
      }
    );
    return (await this.getCompilers()).compiler.withQuery(sqlGenerator, () => ({
      external: sqlGenerator.externalPreAggregationQuery(),
      sql: sqlGenerator.buildSqlAndParams(),
      timeDimensionAlias: sqlGenerator.timeDimensions[0] && sqlGenerator.timeDimensions[0].unescapedAliasName(),
      timeDimensionField: sqlGenerator.timeDimensions[0] && sqlGenerator.timeDimensions[0].dimension,
      order: sqlGenerator.order,
      cacheKeyQueries: sqlGenerator.cacheKeyQueries(),
      preAggregations: sqlGenerator.preAggregations.preAggregationsDescription()
    }));
  }

  async metaConfig() {
    return (await this.getCompilers()).metaTransformer.cubes;
  }
}

module.exports = CompilerApi;
