/* globals describe, beforeAll, afterAll, test, expect */
const QueryOrchestrator = require('../orchestrator/QueryOrchestrator');

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

describe('QueryOrchestrator', () => {
  let mockDriver = null;
  const queryOrchestrator = new QueryOrchestrator(
    'TEST',
    async () => mockDriver,
    (msg, params) => console.log(msg, params), {
      preAggregationsOptions: {
        queueOptions: {
          executionTimeout: 1
        },
        usedTablePersistTime: 1
      }
    }
  );

  beforeAll(() => {
    mockDriver = new MockDriver();
  });

  afterAll(async () => {
    await queryOrchestrator.cleanup();
  });

  test('basic', async () => {
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
      renewQuery: true,
      requestId: 'basic'
    };
    const result = await queryOrchestrator.fetchQuery(query);
    console.log(result.data[0]);
    expect(result.data[0]).toMatch(/orders_number_and_count20191101_kjypcoio_5yftl5il/);
  });

  test('indexes', async () => {
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
      renewQuery: true,
      requestId: 'indexes'
    };
    const result = await queryOrchestrator.fetchQuery(query);
    console.log(result.data[0]);
    expect(result.data[0]).toMatch(/orders_number_and_count20191101_l3kvjcmu_khbemovd/);
    expect(mockDriver.executedQueries.join(',')).toMatch(/CREATE INDEX orders_number_and_count_week20191101_l3kvjcmu_khbemovd/);
  });

  test('silent truncate', async () => {
    const query = {
      query: "SELECT \"orders__created_at_week\" \"orders__created_at_week\", sum(\"orders__count\") \"orders__count\" FROM (SELECT * FROM stb_pre_aggregations.orders_number_and_count_and_very_very_very_very_very_very_long20191101) as partition_union  WHERE (\"orders__created_at_week\" >= ($1::timestamptz::timestamptz AT TIME ZONE 'UTC') AND \"orders__created_at_week\" <= ($2::timestamptz::timestamptz AT TIME ZONE 'UTC')) GROUP BY 1 ORDER BY 1 ASC LIMIT 10000",
      values: ["2019-11-01T00:00:00Z", "2019-11-30T23:59:59Z"],
      cacheKeyQueries: {
        renewalThreshold: 21600,
        queries: [["SELECT date_trunc('hour', (NOW()::timestamptz AT TIME ZONE 'UTC')) as current_hour", []]]
      },
      preAggregations: [{
        preAggregationsSchema: "stb_pre_aggregations",
        tableName: "stb_pre_aggregations.orders_number_and_count_and_very_very_very_very_very_very_long20191101",
        loadSql: ["CREATE TABLE stb_pre_aggregations.orders_number_and_count_and_very_very_very_very_very_very_long20191101 AS SELECT\n      date_trunc('week', (\"orders\".created_at::timestamptz AT TIME ZONE 'UTC')) \"orders__created_at_week\", count(\"orders\".id) \"orders__count\", sum(\"orders\".number) \"orders__number\"\n    FROM\n      public.orders AS \"orders\"\n  WHERE (\"orders\".created_at >= $1::timestamptz AND \"orders\".created_at <= $2::timestamptz) GROUP BY 1", ["2019-11-01T00:00:00Z", "2019-11-30T23:59:59Z"]],
        invalidateKeyQueries: [["SELECT date_trunc('hour', (NOW()::timestamptz AT TIME ZONE 'UTC')) as current_hour", []]],
      }],
      renewQuery: true,
      requestId: 'silent truncate'
    };
    let thrown = true;
    try {
      await queryOrchestrator.fetchQuery(query);
      thrown = false;
    } catch (e) {
      expect(e.message).toMatch(/Pre-aggregation table is not found/);
    }
    expect(thrown).toBe(true);
  });

  test('cancel pre-aggregation', async () => {
    const query = {
      query: "SELECT \"orders__created_at_week\" \"orders__created_at_week\", sum(\"orders__count\") \"orders__count\" FROM (SELECT * FROM stb_pre_aggregations.orders_number_and_count20181101) as partition_union  WHERE (\"orders__created_at_week\" >= ($1::timestamptz::timestamptz AT TIME ZONE 'UTC') AND \"orders__created_at_week\" <= ($2::timestamptz::timestamptz AT TIME ZONE 'UTC')) GROUP BY 1 ORDER BY 1 ASC LIMIT 10000",
      values: ["2018-11-01T00:00:00Z", "2018-11-30T23:59:59Z"],
      cacheKeyQueries: {
        renewalThreshold: 21600,
        queries: [["SELECT date_trunc('hour', (NOW()::timestamptz AT TIME ZONE 'UTC')) as current_hour", []]]
      },
      preAggregations: [{
        preAggregationsSchema: "stb_pre_aggregations",
        tableName: "stb_pre_aggregations.orders_number_and_count20181101",
        loadSql: ["CREATE TABLE stb_pre_aggregations.orders_number_and_count20181101 AS SELECT\n      date_trunc('week', (\"orders\".created_at::timestamptz AT TIME ZONE 'UTC')) \"orders__created_at_week\", count(\"orders\".id) \"orders__count\", sum(\"orders\".number) \"orders__number\"\n    FROM\n      public.orders_too_big AS \"orders\"\n  WHERE (\"orders\".created_at >= $1::timestamptz AND \"orders\".created_at <= $2::timestamptz) GROUP BY 1", ["2018-11-01T00:00:00Z", "2018-11-30T23:59:59Z"]],
        invalidateKeyQueries: [["SELECT date_trunc('hour', (NOW()::timestamptz AT TIME ZONE 'UTC')) as current_hour", []]]
      }],
      renewQuery: true,
      requestId: 'cancel pre-aggregation'
    };
    try {
      await queryOrchestrator.fetchQuery(query);
    } catch (e) {
      expect(e.toString()).toMatch(/timeout/);
    }
    expect(mockDriver.cancelledQueries[0]).toMatch(/orders_too_big/);
  });

  test('save structure versions', async () => {
    mockDriver.tables = [];
    await queryOrchestrator.fetchQuery({
      query: `SELECT * FROM stb_pre_aggregations.orders`,
      values: [],
      cacheKeyQueries: {
        renewalThreshold: 21600,
        queries: []
      },
      preAggregations: [{
        preAggregationsSchema: "stb_pre_aggregations",
        tableName: "stb_pre_aggregations.orders",
        loadSql: ["CREATE TABLE stb_pre_aggregations.orders AS SELECT * FROM public.orders", []],
        invalidateKeyQueries: [["SELECT 1", []]]
      }],
      renewQuery: true,
      requestId: 'save structure versions'
    });

    await queryOrchestrator.fetchQuery({
      query: `SELECT * FROM stb_pre_aggregations.orders`,
      values: [],
      cacheKeyQueries: {
        renewalThreshold: 21600,
        queries: []
      },
      preAggregations: [{
        preAggregationsSchema: "stb_pre_aggregations",
        tableName: "stb_pre_aggregations.orders",
        loadSql: ["CREATE TABLE stb_pre_aggregations.orders AS SELECT * FROM public.orders1", []],
        invalidateKeyQueries: [["SELECT 1", []]]
      }],
      renewQuery: true,
      requestId: 'save structure versions'
    });

    await new Promise(resolve => setTimeout(() => resolve(), 1000));

    for (let i = 0; i < 5; i++) {
      await queryOrchestrator.fetchQuery({
        query: `SELECT * FROM stb_pre_aggregations.orders`,
        values: [],
        cacheKeyQueries: {
          renewalThreshold: 21600,
          queries: []
        },
        preAggregations: [{
          preAggregationsSchema: "stb_pre_aggregations",
          tableName: "stb_pre_aggregations.orders",
          loadSql: ["CREATE TABLE stb_pre_aggregations.orders AS SELECT * FROM public.orders", []],
          invalidateKeyQueries: [["SELECT 2", []]]
        }],
        renewQuery: true,
        requestId: 'save structure versions'
      });
    }
    expect(mockDriver.tables).toContainEqual(expect.stringMatching(/orders_f5v4jw3p_4eysppzt/));
    expect(mockDriver.tables).toContainEqual(expect.stringMatching(/orders_mjooke4_ezlvkhjl/));
  });
});
