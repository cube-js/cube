const QueryBuilder = require('@cubejs-backend/schema-compiler/adapter/QueryBuilder');

class CompilerApi {
  constructor(compilers, dbType) {
    this.compilers = compilers;
    this.dbType = dbType;
  }

  async getSql(query) {
    const sqlGenerator = QueryBuilder.query(
      this.compilers,
      this.dbType,
      query
    );
    return this.compilers.compiler.withQuery(sqlGenerator, () => ({
      sql: sqlGenerator.buildSqlAndParams(),
      timeDimensionAlias: sqlGenerator.timeDimensions[0] && sqlGenerator.timeDimensions[0].unescapedAliasName(),
      timeDimensionField: sqlGenerator.timeDimensions[0] && sqlGenerator.timeDimensions[0].dimension,
      order: sqlGenerator.order,
      cacheKeyQueries: sqlGenerator.cacheKeyQueries(),
      preAggregations: sqlGenerator.preAggregations.preAggregationsDescription()
    }));
  }

  async metaConfig() {
    return this.compilers.metaTransformer.cubes;
  }
}

module.exports = CompilerApi;