/* globals describe, beforeAll, afterAll, beforeEach, test, expect */
const crypto = require('crypto');

class MockDriver {
  constructor() {
    this.tables = [];
    this.executedQueries = [];
    this.cancelledQueries = [];
  }

  query(query) {
    this.executedQueries.push(query);
    let promise = Promise.resolve([query]);
    if (query.match(`orders_too_big`)) {
      promise = promise.then((res) => new Promise(resolve => setTimeout(() => resolve(res), 3000)));
    }
    promise.cancel = async () => {
      this.cancelledQueries.push(query);
    };
    return promise;
  }

  async getTablesQuery(schema) {
    return this.tables.map(t => ({ table_name: t.replace(`${schema}.`, '') }));
  }

  async createSchemaIfNotExists(schema) {
    this.schema = schema;
    return null;
  }

  loadPreAggregationIntoTable(preAggregationTableName, loadSql) {
    this.tables.push(preAggregationTableName.substring(0, 100));
    return this.query(loadSql);
  }

  async dropTable(tableName) {
    this.tables = this.tables.filter(t => t !== tableName);
    return this.query(`DROP TABLE ${tableName}`);
  }
}

describe('PreAggregations', () => {
  let mockDriver = null;
  let queryCache = null;
  const basicQuery = {
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
    requestId: 'basic'
  };
  const basicQueryWithRenew = {
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
    renewQuery: true,
    requestId: 'basic'
  };

  beforeEach(() => {
    mockDriver = new MockDriver();

    jest.resetModules();
    const QueryCache = require('../orchestrator/QueryCache');
    queryCache = new QueryCache(
      "TEST",
      async () => mockDriver,
      (msg, params) => {},
      {
        queueOptions: {
          executionTimeout: 1
        },
      },
    );
  });

  describe('loadAllPreAggregationsIfNeeded', () => {
    let preAggregations = null;

    beforeEach(async () => {
      const PreAggregations = require('../orchestrator/PreAggregations');
      preAggregations = new PreAggregations(
        "TEST",
        async () => mockDriver,
        (msg, params) => {},
        queryCache,
        {
          queueOptions: {
            executionTimeout: 1
          },
        },
      );
    });

    test('syncronously create rollup from scratch', async () => {
      const result = await preAggregations.loadAllPreAggregationsIfNeeded(basicQueryWithRenew);
      expect(result[0][1].targetTableName).toMatch(/stb_pre_aggregations.orders_number_and_count20191101_kjypcoio_5yftl5il/);
    });
  });

  describe(`loadAllPreAggregationsIfNeeded with externalRefresh true`, () => {
    let preAggregations = null;

    beforeEach(async () => {
      const PreAggregations = require('../orchestrator/PreAggregations');
      preAggregations = new PreAggregations(
        "TEST",
        async () => mockDriver,
        (msg, params) => {},
        queryCache,
        {
          queueOptions: {
            executionTimeout: 1
          },
          externalRefresh: true,
        },
      );
    });

    test('fail if waitForRenew is also specified', async () => {
      await expect(preAggregations.loadAllPreAggregationsIfNeeded(basicQueryWithRenew))
        .rejects.toThrowError(/Invalid configuration/);
    });

    test('fail if rollup doesn\'t already exist', async () => {
      await expect(preAggregations.loadAllPreAggregationsIfNeeded(basicQuery))
        .rejects.toThrowError(/One or more pre-aggregation tables could not be found to satisfy that query/);
    });
  });
});
