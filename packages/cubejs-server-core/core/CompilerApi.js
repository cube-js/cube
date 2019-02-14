const QueryBuilder = require('@cubejs-backend/schema-compiler/adapter/QueryBuilder');
const PrepareCompiler = require('@cubejs-backend/schema-compiler/compiler/PrepareCompiler');

class CompilerApi {
  constructor(repository, dbType) {
    this.repository = repository;
    this.dbType = dbType;
  }

  async getCompilers() {
    if (!this.compilers) {
      this.compilers = await PrepareCompiler.compile(this.repository, { adapter: this.dbType }); // TODO mutex and rebuild
    }
    return this.compilers;
  }

  async getSql(query) {
    const sqlGenerator = QueryBuilder.query(
      await this.getCompilers(),
      this.dbType,
      query
    );
    return (await this.getCompilers()).compiler.withQuery(sqlGenerator, () => ({
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