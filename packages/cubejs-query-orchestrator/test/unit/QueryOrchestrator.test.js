/* globals describe, beforeEach, afterEach, test, expect */
import { QueryOrchestrator } from '../../src/orchestrator/QueryOrchestrator';

class MockDriver {
  constructor() {
    this.tables = [];
    this.tablesReady = [];
    this.executedQueries = [];
    this.cancelledQueries = [];
  }

  query(query) {
    this.executedQueries.push(query);
    let promise = Promise.resolve([query]);
    if (query.match('orders_too_big')) {
      promise = promise.then((res) => new Promise(resolve => setTimeout(() => resolve(res), 3000)));
    }

    if (query.match('orders_delay')) {
      promise = promise.then((res) => new Promise(resolve => setTimeout(() => resolve(res), 800)));
    }

    if (query.match(/^SELECT NOW\(\)$/)) {
      promise = promise.then(() => new Date().toJSON());
    }

    if (this.tablesReady.find(t => query.indexOf(t) !== -1)) {
      promise = promise.then(res => res.concat({ tableReady: true }));
    }

    promise.cancel = () => {
      this.cancelledQueries.push(query);
    };
    return promise;
  }

  async getTablesQuery(schema) {
    if (this.tablesQueryDelay) {
      await this.delay(this.tablesQueryDelay);
    }
    return this.tables.map(t => ({ table_name: t.replace(`${schema}.`, '') }));
  }

  delay(timeout) {
    return new Promise(resolve => setTimeout(() => resolve(), timeout));
  }

  async createSchemaIfNotExists(schema) {
    this.schema = schema;
    return null;
  }

  loadPreAggregationIntoTable(preAggregationTableName, loadSql) {
    this.tables.push(preAggregationTableName.substring(0, 100));
    const promise = this.query(loadSql);
    const resPromise = promise.then(() => this.tablesReady.push(preAggregationTableName.substring(0, 100)));
    resPromise.cancel = promise.cancel;
    return resPromise;
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
    (msg, params) => console.log(new Date().toJSON(), msg, params), {
      preAggregationsOptions: {
        queueOptions: {
          executionTimeout: 1
        },
        usedTablePersistTime: 1
      }
    }
  );

  beforeEach(() => {
    mockDriver = new MockDriver();
  });

  afterEach(async () => {
    await queryOrchestrator.cleanup();
  });

  test('basic', async () => {
    const query = {
      query: 'SELECT "orders__created_at_week" "orders__created_at_week", sum("orders__count") "orders__count" FROM (SELECT * FROM stb_pre_aggregations.orders_number_and_count20191101) as partition_union  WHERE ("orders__created_at_week" >= ($1::timestamptz::timestamptz AT TIME ZONE \'UTC\') AND "orders__created_at_week" <= ($2::timestamptz::timestamptz AT TIME ZONE \'UTC\')) GROUP BY 1 ORDER BY 1 ASC LIMIT 10000',
      values: ['2019-11-01T00:00:00Z', '2019-11-30T23:59:59Z'],
      cacheKeyQueries: {
        renewalThreshold: 21600,
        queries: [['SELECT date_trunc(\'hour\', (NOW()::timestamptz AT TIME ZONE \'UTC\')) as current_hour', []]]
      },
      preAggregations: [{
        preAggregationsSchema: 'stb_pre_aggregations',
        tableName: 'stb_pre_aggregations.orders_number_and_count20191101',
        loadSql: ['CREATE TABLE stb_pre_aggregations.orders_number_and_count20191101 AS SELECT\n      date_trunc(\'week\', ("orders".created_at::timestamptz AT TIME ZONE \'UTC\')) "orders__created_at_week", count("orders".id) "orders__count", sum("orders".number) "orders__number"\n    FROM\n      public.orders AS "orders"\n  WHERE ("orders".created_at >= $1::timestamptz AND "orders".created_at <= $2::timestamptz) GROUP BY 1', ['2019-11-01T00:00:00Z', '2019-11-30T23:59:59Z']],
        invalidateKeyQueries: [['SELECT date_trunc(\'hour\', (NOW()::timestamptz AT TIME ZONE \'UTC\')) as current_hour', []]]
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
      query: 'SELECT "orders__created_at_week" "orders__created_at_week", sum("orders__count") "orders__count" FROM (SELECT * FROM stb_pre_aggregations.orders_number_and_count20191101) as partition_union  WHERE ("orders__created_at_week" >= ($1::timestamptz::timestamptz AT TIME ZONE \'UTC\') AND "orders__created_at_week" <= ($2::timestamptz::timestamptz AT TIME ZONE \'UTC\')) GROUP BY 1 ORDER BY 1 ASC LIMIT 10000',
      values: ['2019-11-01T00:00:00Z', '2019-11-30T23:59:59Z'],
      cacheKeyQueries: {
        renewalThreshold: 21600,
        queries: [['SELECT date_trunc(\'hour\', (NOW()::timestamptz AT TIME ZONE \'UTC\')) as current_hour', []]]
      },
      preAggregations: [{
        preAggregationsSchema: 'stb_pre_aggregations',
        tableName: 'stb_pre_aggregations.orders_number_and_count20191101',
        loadSql: ['CREATE TABLE stb_pre_aggregations.orders_number_and_count20191101 AS SELECT\n      date_trunc(\'week\', ("orders".created_at::timestamptz AT TIME ZONE \'UTC\')) "orders__created_at_week", count("orders".id) "orders__count", sum("orders".number) "orders__number"\n    FROM\n      public.orders AS "orders"\n  WHERE ("orders".created_at >= $1::timestamptz AND "orders".created_at <= $2::timestamptz) GROUP BY 1', ['2019-11-01T00:00:00Z', '2019-11-30T23:59:59Z']],
        invalidateKeyQueries: [['SELECT date_trunc(\'hour\', (NOW()::timestamptz AT TIME ZONE \'UTC\')) as current_hour', []]],
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
      query: 'SELECT "orders__created_at_week" "orders__created_at_week", sum("orders__count") "orders__count" FROM (SELECT * FROM stb_pre_aggregations.orders_number_and_count_and_very_very_very_very_very_very_long20191101) as partition_union  WHERE ("orders__created_at_week" >= ($1::timestamptz::timestamptz AT TIME ZONE \'UTC\') AND "orders__created_at_week" <= ($2::timestamptz::timestamptz AT TIME ZONE \'UTC\')) GROUP BY 1 ORDER BY 1 ASC LIMIT 10000',
      values: ['2019-11-01T00:00:00Z', '2019-11-30T23:59:59Z'],
      cacheKeyQueries: {
        renewalThreshold: 21600,
        queries: [['SELECT date_trunc(\'hour\', (NOW()::timestamptz AT TIME ZONE \'UTC\')) as current_hour', []]]
      },
      preAggregations: [{
        preAggregationsSchema: 'stb_pre_aggregations',
        tableName: 'stb_pre_aggregations.orders_number_and_count_and_very_very_very_very_very_very_long20191101',
        loadSql: ['CREATE TABLE stb_pre_aggregations.orders_number_and_count_and_very_very_very_very_very_very_long20191101 AS SELECT\n      date_trunc(\'week\', ("orders".created_at::timestamptz AT TIME ZONE \'UTC\')) "orders__created_at_week", count("orders".id) "orders__count", sum("orders".number) "orders__number"\n    FROM\n      public.orders AS "orders"\n  WHERE ("orders".created_at >= $1::timestamptz AND "orders".created_at <= $2::timestamptz) GROUP BY 1', ['2019-11-01T00:00:00Z', '2019-11-30T23:59:59Z']],
        invalidateKeyQueries: [['SELECT date_trunc(\'hour\', (NOW()::timestamptz AT TIME ZONE \'UTC\')) as current_hour', []]],
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
      query: 'SELECT "orders__created_at_week" "orders__created_at_week", sum("orders__count") "orders__count" FROM (SELECT * FROM stb_pre_aggregations.orders_number_and_count20181101) as partition_union  WHERE ("orders__created_at_week" >= ($1::timestamptz::timestamptz AT TIME ZONE \'UTC\') AND "orders__created_at_week" <= ($2::timestamptz::timestamptz AT TIME ZONE \'UTC\')) GROUP BY 1 ORDER BY 1 ASC LIMIT 10000',
      values: ['2018-11-01T00:00:00Z', '2018-11-30T23:59:59Z'],
      cacheKeyQueries: {
        renewalThreshold: 21600,
        queries: [['SELECT date_trunc(\'hour\', (NOW()::timestamptz AT TIME ZONE \'UTC\')) as current_hour', []]]
      },
      preAggregations: [{
        preAggregationsSchema: 'stb_pre_aggregations',
        tableName: 'stb_pre_aggregations.orders_number_and_count20181101',
        loadSql: ['CREATE TABLE stb_pre_aggregations.orders_number_and_count20181101 AS SELECT\n      date_trunc(\'week\', ("orders".created_at::timestamptz AT TIME ZONE \'UTC\')) "orders__created_at_week", count("orders".id) "orders__count", sum("orders".number) "orders__number"\n    FROM\n      public.orders_too_big AS "orders"\n  WHERE ("orders".created_at >= $1::timestamptz AND "orders".created_at <= $2::timestamptz) GROUP BY 1', ['2018-11-01T00:00:00Z', '2018-11-30T23:59:59Z']],
        invalidateKeyQueries: [['SELECT date_trunc(\'hour\', (NOW()::timestamptz AT TIME ZONE \'UTC\')) as current_hour', []]]
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
      query: 'SELECT * FROM stb_pre_aggregations.orders',
      values: [],
      cacheKeyQueries: {
        renewalThreshold: 21600,
        queries: []
      },
      preAggregations: [{
        preAggregationsSchema: 'stb_pre_aggregations',
        tableName: 'stb_pre_aggregations.orders',
        loadSql: ['CREATE TABLE stb_pre_aggregations.orders AS SELECT * FROM public.orders', []],
        invalidateKeyQueries: [['SELECT 1', []]]
      }],
      renewQuery: true,
      requestId: 'save structure versions'
    });

    await queryOrchestrator.fetchQuery({
      query: 'SELECT * FROM stb_pre_aggregations.orders',
      values: [],
      cacheKeyQueries: {
        renewalThreshold: 21600,
        queries: []
      },
      preAggregations: [{
        preAggregationsSchema: 'stb_pre_aggregations',
        tableName: 'stb_pre_aggregations.orders',
        loadSql: ['CREATE TABLE stb_pre_aggregations.orders AS SELECT * FROM public.orders1', []],
        invalidateKeyQueries: [['SELECT 1', []]]
      }],
      renewQuery: true,
      requestId: 'save structure versions'
    });

    await new Promise(resolve => setTimeout(() => resolve(), 1000));

    for (let i = 0; i < 5; i++) {
      await queryOrchestrator.fetchQuery({
        query: 'SELECT * FROM stb_pre_aggregations.orders',
        values: [],
        cacheKeyQueries: {
          renewalThreshold: 21600,
          queries: []
        },
        preAggregations: [{
          preAggregationsSchema: 'stb_pre_aggregations',
          tableName: 'stb_pre_aggregations.orders',
          loadSql: ['CREATE TABLE stb_pre_aggregations.orders AS SELECT * FROM public.orders', []],
          invalidateKeyQueries: [['SELECT 2', []]]
        }],
        renewQuery: true,
        requestId: 'save structure versions'
      });
    }
    expect(mockDriver.tables).toContainEqual(expect.stringMatching(/orders_f5v4jw3p_4eysppzt/));
    expect(mockDriver.tables).toContainEqual(expect.stringMatching(/orders_mjooke4_ezlvkhjl/));
  });

  test('intermittent empty rollup', async () => {
    const firstQuery = queryOrchestrator.fetchQuery({
      query: 'SELECT * FROM stb_pre_aggregations.orders_d20181102',
      values: [],
      cacheKeyQueries: {
        renewalThreshold: 21600,
        queries: []
      },
      preAggregations: [{
        preAggregationsSchema: 'stb_pre_aggregations',
        tableName: 'stb_pre_aggregations.orders_d20181102',
        loadSql: ['CREATE TABLE stb_pre_aggregations.orders_d20181102 AS SELECT * FROM public.orders_delay', []],
        invalidateKeyQueries: [['SELECT 2', []]]
      }],
      requestId: 'intermittent empty rollup'
    });

    queryOrchestrator.fetchQuery({
      query: 'SELECT "orders__created_at_week" "orders__created_at_week", sum("orders__count") "orders__count" FROM (SELECT * FROM stb_pre_aggregations.orders_d20181101) as partition_union  WHERE ("orders__created_at_week" >= ($1::timestamptz::timestamptz AT TIME ZONE \'UTC\') AND "orders__created_at_week" <= ($2::timestamptz::timestamptz AT TIME ZONE \'UTC\')) GROUP BY 1 ORDER BY 1 ASC LIMIT 10000',
      values: ['2018-11-01T00:00:00Z', '2018-11-30T23:59:59Z'],
      cacheKeyQueries: {
        renewalThreshold: 21600,
        queries: [['SELECT date_trunc(\'hour\', (NOW()::timestamptz AT TIME ZONE \'UTC\')) as current_hour', []]]
      },
      preAggregations: [{
        preAggregationsSchema: 'stb_pre_aggregations',
        tableName: 'stb_pre_aggregations.orders_d20181101',
        loadSql: [
          'CREATE TABLE stb_pre_aggregations.orders_d20181101 AS SELECT\n      date_trunc(\'week\', ("orders".created_at::timestamptz AT TIME ZONE \'UTC\')) "orders__created_at_week", count("orders".id) "orders__count", sum("orders".number) "orders__number"\n    FROM\n      public.orders_delay AS "orders"\n  WHERE ("orders".created_at >= $1::timestamptz AND "orders".created_at <= $2::timestamptz) GROUP BY 1',
          ['2018-11-01T00:00:00Z', '2018-11-30T23:59:59Z']
        ],
        invalidateKeyQueries: [['SELECT date_trunc(\'hour\', (NOW()::timestamptz AT TIME ZONE \'UTC\')) as current_hour', []]]
      }],
      requestId: 'intermittent empty rollup'
    });

    await firstQuery;

    const res = await queryOrchestrator.fetchQuery({
      query: 'SELECT "orders__created_at_week" "orders__created_at_week", sum("orders__count") "orders__count" FROM (SELECT * FROM stb_pre_aggregations.orders_d20181101) as partition_union  WHERE ("orders__created_at_week" >= ($1::timestamptz::timestamptz AT TIME ZONE \'UTC\') AND "orders__created_at_week" <= ($2::timestamptz::timestamptz AT TIME ZONE \'UTC\')) GROUP BY 1 ORDER BY 1 ASC LIMIT 10000',
      values: ['2018-11-01T00:00:00Z', '2018-11-30T23:59:59Z'],
      cacheKeyQueries: {
        renewalThreshold: 21600,
        queries: [['SELECT date_trunc(\'hour\', (NOW()::timestamptz AT TIME ZONE \'UTC\')) as current_hour', []]]
      },
      preAggregations: [{
        preAggregationsSchema: 'stb_pre_aggregations',
        tableName: 'stb_pre_aggregations.orders_d20181101',
        loadSql: [
          'CREATE TABLE stb_pre_aggregations.orders_d20181101 AS SELECT\n      date_trunc(\'week\', ("orders".created_at::timestamptz AT TIME ZONE \'UTC\')) "orders__created_at_week", count("orders".id) "orders__count", sum("orders".number) "orders__number"\n    FROM\n      public.orders_delay AS "orders"\n  WHERE ("orders".created_at >= $1::timestamptz AND "orders".created_at <= $2::timestamptz) GROUP BY 1',
          ['2018-11-01T00:00:00Z', '2018-11-30T23:59:59Z']
        ],
        invalidateKeyQueries: [['SELECT date_trunc(\'hour\', (NOW()::timestamptz AT TIME ZONE \'UTC\')) as current_hour', []]]
      }],
      requestId: 'intermittent empty rollup'
    });

    console.log(res);

    expect(res.data).toContainEqual(expect.objectContaining({ tableReady: true }));
  });

  test('continue serve old tables cache without resetting it', async () => {
    mockDriver.tablesQueryDelay = 300;
    const requestId = 'continue serve old tables cache without resetting it';
    const baseQuery = {
      query: 'SELECT * FROM stb_pre_aggregations.orders_d20181103',
      values: [],
      cacheKeyQueries: {
        renewalThreshold: 1,
        queries: []
      },
      preAggregations: [{
        preAggregationsSchema: 'stb_pre_aggregations',
        tableName: 'stb_pre_aggregations.orders_d20181103',
        loadSql: ['CREATE TABLE stb_pre_aggregations.orders_d20181103 AS SELECT * FROM public.orders', []],
        invalidateKeyQueries: [['SELECT NOW()', []]],
        refreshKeyRenewalThresholds: [0.001]
      }]
    };

    // create from scratch
    await queryOrchestrator.fetchQuery({
      ...baseQuery,
      requestId: `${requestId}: create from scratch`
    });

    // start renew refresh as scheduled refresh does
    const refresh = queryOrchestrator.fetchQuery({
      ...baseQuery,
      renewQuery: true,
      requestId: `${requestId}: start refresh`
    });

    await mockDriver.delay(100);

    let firstResolve = null;

    console.log('Starting race');

    // If database has a significant delay for pre-aggregations tables fetch we should continue serve rollup cache
    // instead of waiting tables fetch query to complete.
    await Promise.all([
      queryOrchestrator.fetchQuery({
        ...baseQuery,
        requestId: `${requestId}: race`
      }).then(() => {
        if (!firstResolve) {
          firstResolve = 'query';
        }
      }),
      mockDriver.delay(150).then(() => {
        if (!firstResolve) {
          firstResolve = 'delay';
        }
      })
    ]);

    await refresh;

    expect(firstResolve).toBe('query');
  });
});
