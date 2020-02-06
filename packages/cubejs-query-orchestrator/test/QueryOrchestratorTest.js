/* globals describe, it, should, before */
const QueryOrchestrator = require('../orchestrator/QueryOrchestrator');

class MockDriver {
  constructor() {
    this.tables = [];
    this.executedQueries = [];
  }

  async query(query) {
    this.executedQueries.push(query);
    return [query];
  }

  async getTablesQuery(schema) {
    return this.tables.map(t => ({ table_name: t.replace(`${schema}.`, '') }));
  }

  async createSchemaIfNotExists(schema) {
    this.schema = schema;
    return null;
  }

  async loadPreAggregationIntoTable(preAggregationTableName) {
    this.tables.push(preAggregationTableName);
  }
}

describe('QueryOrchestrator', () => {
  let mockDriver = null;

  before(() => {
    mockDriver = new MockDriver();
  });

  const queryOrchestrator = new QueryOrchestrator(
    'TEST', async () => mockDriver, (msg, params) => console.log(msg, params)
  );

  it('basic', async () => {
    const query = {
      query: "SELECT \"orders__created_at_week\" \"orders__created_at_week\", sum(\"orders__count\") \"orders__count\" FROM (SELECT * FROM stb_pre_aggregations.orders_number_and_count20191101) as partition_union  WHERE (\"orders__created_at_week\" >= ($1::timestamptz::timestamptz AT TIME ZONE 'UTC') AND \"orders__created_at_week\" <= ($2::timestamptz::timestamptz AT TIME ZONE 'UTC')) GROUP BY 1 ORDER BY 1 ASC LIMIT 10000",
      values: ["2019-11-01T00:00:00Z", "2019-11-30T23:59:59Z"],
      cacheKeyQueries: {
        renewalThreshold: 21600,
        queries: [["SELECT date_trunc('hour', (NOW()::timestamptz AT TIME ZONE 'UTC')) as current_hour", []]]
      },
      preAggregations: [{
        preAggregationsSchema: "stb_pre_aggregations",
        tableName: "stb_pre_aggregations.orders_number_and_count20191101",
        loadSql: ["CREATE TABLE stb_pre_aggregations.orders_number_and_count20191101 AS SELECT\n      date_trunc('week', (\"orders\".created_at::timestamptz AT TIME ZONE 'UTC')) \"orders__created_at_week\", count(\"orders\".id) \"orders__count\", sum(\"orders\".number) \"orders__number\"\n    FROM\n      public.orders AS \"orders\"\n  WHERE (\"orders\".created_at >= $1::timestamptz AND \"orders\".created_at <= $2::timestamptz) GROUP BY 1", ["2019-11-01T00:00:00Z", "2019-11-30T23:59:59Z"]],
        invalidateKeyQueries: [["SELECT date_trunc('hour', (NOW()::timestamptz AT TIME ZONE 'UTC')) as current_hour", []]]
      }],
      renewQuery: true
    };
    const result = await queryOrchestrator.fetchQuery(query);
    console.log(result.data[0]);
    should(result.data[0]).match(/orders_number_and_count20191101_kjypcoio_5yftl5il/);
  });

  it('indexes', async () => {
    const query = {
      query: "SELECT \"orders__created_at_week\" \"orders__created_at_week\", sum(\"orders__count\") \"orders__count\" FROM (SELECT * FROM stb_pre_aggregations.orders_number_and_count20191101) as partition_union  WHERE (\"orders__created_at_week\" >= ($1::timestamptz::timestamptz AT TIME ZONE 'UTC') AND \"orders__created_at_week\" <= ($2::timestamptz::timestamptz AT TIME ZONE 'UTC')) GROUP BY 1 ORDER BY 1 ASC LIMIT 10000",
      values: ["2019-11-01T00:00:00Z", "2019-11-30T23:59:59Z"],
      cacheKeyQueries: {
        renewalThreshold: 21600,
        queries: [["SELECT date_trunc('hour', (NOW()::timestamptz AT TIME ZONE 'UTC')) as current_hour", []]]
      },
      preAggregations: [{
        preAggregationsSchema: "stb_pre_aggregations",
        tableName: "stb_pre_aggregations.orders_number_and_count20191101",
        loadSql: ["CREATE TABLE stb_pre_aggregations.orders_number_and_count20191101 AS SELECT\n      date_trunc('week', (\"orders\".created_at::timestamptz AT TIME ZONE 'UTC')) \"orders__created_at_week\", count(\"orders\".id) \"orders__count\", sum(\"orders\".number) \"orders__number\"\n    FROM\n      public.orders AS \"orders\"\n  WHERE (\"orders\".created_at >= $1::timestamptz AND \"orders\".created_at <= $2::timestamptz) GROUP BY 1", ["2019-11-01T00:00:00Z", "2019-11-30T23:59:59Z"]],
        invalidateKeyQueries: [["SELECT date_trunc('hour', (NOW()::timestamptz AT TIME ZONE 'UTC')) as current_hour", []]],
        indexesSql: [{
          sql: ['CREATE INDEX orders_number_and_count_week20191101 ON stb_pre_aggregations.orders_number_and_count20191101 ("orders__created_at_week")', []],
          indexName: 'orders_number_and_count_week20191101'
        }],
      }],
      renewQuery: true
    };
    const result = await queryOrchestrator.fetchQuery(query);
    console.log(result.data[0]);
    should(result.data[0]).match(/orders_number_and_count20191101_l3kvjcmu_khbemovd/);
    should(mockDriver.executedQueries).matchAny(/CREATE INDEX orders_number_and_count_week20191101_l3kvjcmu_khbemovd/);
  });
});
