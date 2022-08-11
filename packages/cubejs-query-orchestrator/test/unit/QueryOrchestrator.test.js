/* globals jest, describe, beforeEach, afterEach, test, expect */

import { QueryOrchestrator } from '../../src/orchestrator/QueryOrchestrator';

class MockDriver {
  constructor({ csvImport } = {}) {
    this.tables = [];
    this.tablesReady = [];
    this.executedQueries = [];
    this.cancelledQueries = [];
    this.csvImport = csvImport;
    this.now = new Date().getTime();
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
      promise = promise.then(() => [{ now: new Date().toJSON() }]);
    }

    if (query.match(/^SELECT MAX\(timestamp\)/)) {
      promise = promise.then(() => [{ max: new Date('2021-06-01T00:00:00.000Z').toJSON() }]);
    }

    if (query.match(/^SELECT MIN\(timestamp\)/)) {
      promise = promise.then(() => [{ min: new Date('2021-05-01T00:00:00.000Z').toJSON() }]);
    }

    if (query.match(/^SELECT MAX\(created_at\)/)) {
      promise = promise.then(() => [{ max: null }]);
    }

    if (query.match(/^SELECT MIN\(created_at\)/)) {
      promise = promise.then(() => [{ min: null }]);
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
    return this.tables.filter(t => t.split('.')[0] === schema).map(t => ({ table_name: t.replace(`${schema}.`, '') }));
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

  async downloadTable(table, { csvImport } = {}) {
    if (this.csvImport && csvImport) {
      return { csvFile: `${table}.csv` };
    }
    return { rows: await this.query(`SELECT * FROM ${table}`) };
  }

  async tableColumnTypes() {
    return [{ name: 'foo', type: 'int' }];
  }

  nowTimestamp() {
    return this.now;
  }
}

class ExternalMockDriver extends MockDriver {
  constructor() {
    super();
    this.indexes = [];
    this.csvFiles = [];
  }

  async uploadTable(table) {
    this.tables.push(table.substring(0, 100));
    throw new Error('uploadTable has been called instead of uploadTableWithIndexes');
  }

  async uploadTableWithIndexes(table, columns, tableData, indexesSql) {
    this.tables.push(table.substring(0, 100));
    if (tableData.csvFile) {
      this.csvFiles.push(tableData.csvFile);
    }
    for (let i = 0; i < indexesSql.length; i++) {
      const [query, params] = indexesSql[i].sql;
      await this.query(query, params);
    }
    this.indexes = this.indexes.concat(indexesSql);
  }

  capabilities() {
    return { csvImport: true };
  }
}

describe('QueryOrchestrator', () => {
  jest.setTimeout(15000);
  let mockDriver = null;
  let fooMockDriver = null;
  let barMockDriver = null;
  let externalMockDriver = null;
  let queryOrchestrator = null;
  let queryOrchestratorExternalRefresh = null;
  let testCount = 1;

  beforeEach(() => {
    const mockDriverLocal = new MockDriver();
    const fooMockDriverLocal = new MockDriver();
    const barMockDriverLocal = new MockDriver();
    const csvMockDriverLocal = new MockDriver({ csvImport: 'true' });
    const externalMockDriverLocal = new ExternalMockDriver();

    const redisPrefix = `ORCHESTRATOR_TEST_${testCount++}`;
    const driverFactory = (dataSource) => {
      if (dataSource === 'foo') {
        return fooMockDriverLocal;
      } else if (dataSource === 'bar') {
        return barMockDriverLocal;
      } else if (dataSource === 'csv') {
        return csvMockDriverLocal;
      } else {
        return mockDriverLocal;
      }
    };
    const logger =
      (msg, params) => console.log(new Date().toJSON(), msg, params);
    const options = {
      externalDriverFactory: () => externalMockDriverLocal,
      queryCacheOptions: {
        queueOptions: () => ({
          concurrency: 2,
        }),
      },
      preAggregationsOptions: {
        maxPartitions: 32,
        queueOptions: () => ({
          executionTimeout: 2,
          concurrency: 2,
        }),
        usedTablePersistTime: 1
      },
    };

    queryOrchestrator =
      new QueryOrchestrator(redisPrefix, driverFactory, logger, options);
    queryOrchestratorExternalRefresh =
      new QueryOrchestrator(redisPrefix, driverFactory, logger, {
        ...options,
        preAggregationsOptions: {
          ...options.preAggregationsOptions,
          externalRefresh: true,
        },
      });
    mockDriver = mockDriverLocal;
    fooMockDriver = fooMockDriverLocal;
    barMockDriver = barMockDriverLocal;
    externalMockDriver = externalMockDriverLocal;
  });

  afterEach(async () => {
    await queryOrchestrator.cleanup();
    await queryOrchestratorExternalRefresh.cleanup();
  });

  test('basic', async () => {
    mockDriver.now = 12345000;
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
    const promise = queryOrchestrator.fetchQuery(query);
    console.log(await queryOrchestrator.queryStage(query));
    const result = await promise;
    console.log(result.data[0]);
    expect(result.data[0]).toMatch(/orders_number_and_count20191101_kjypcoio_5yftl5il/);
    expect(result.lastRefreshTime.getTime()).toEqual(12345000);
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

  test('index is part of query key', async () => {
    queryOrchestrator.fetchQuery({
      query: 'SELECT "orders__created_at_week" "orders__created_at_week", sum("orders__count") "orders__count" FROM (SELECT * FROM stb_pre_aggregations.orders_number_and_count20191102) as partition_union  WHERE ("orders__created_at_week" >= ($1::timestamptz::timestamptz AT TIME ZONE \'UTC\') AND "orders__created_at_week" <= ($2::timestamptz::timestamptz AT TIME ZONE \'UTC\')) GROUP BY 1 ORDER BY 1 ASC LIMIT 10000',
      values: ['2019-11-01T00:00:00Z', '2019-11-30T23:59:59Z'],
      cacheKeyQueries: {
        renewalThreshold: 21600,
        queries: [['SELECT date_trunc(\'hour\', (NOW()::timestamptz AT TIME ZONE \'UTC\')) as current_hour', []]]
      },
      preAggregations: [{
        preAggregationsSchema: 'stb_pre_aggregations',
        tableName: 'stb_pre_aggregations.orders_number_and_count20191102',
        loadSql: ['CREATE TABLE stb_pre_aggregations.orders_number_and_count20191102 AS SELECT\n      date_trunc(\'week\', ("orders".created_at::timestamptz AT TIME ZONE \'UTC\')) "orders__created_at_week", count("orders".id) "orders__count", sum("orders".number) "orders__number"\n    FROM\n      public.orders_delay AS "orders"\n  WHERE ("orders".created_at >= $1::timestamptz AND "orders".created_at <= $2::timestamptz) GROUP BY 1', ['2019-11-01T00:00:00Z', '2019-11-30T23:59:59Z']],
        invalidateKeyQueries: [['SELECT date_trunc(\'hour\', (NOW()::timestamptz AT TIME ZONE \'UTC\')) as current_hour', []]],
        indexesSql: [{
          sql: ['CREATE INDEX orders_number_and_count_week20191102 ON stb_pre_aggregations.orders_number_and_count20191102 ("orders__created_at_week")', []],
          indexName: 'orders_number_and_count_week20191102'
        }],
      }],
      renewQuery: true,
      requestId: 'index is part of query key'
    });
    await new Promise(resolve => setTimeout(() => resolve(), 400));
    const result = await queryOrchestrator.fetchQuery({
      query: 'SELECT "orders__created_at_week" "orders__created_at_week", sum("orders__count") "orders__count" FROM (SELECT * FROM stb_pre_aggregations.orders_number_and_count20191102) as partition_union  WHERE ("orders__created_at_week" >= ($1::timestamptz::timestamptz AT TIME ZONE \'UTC\') AND "orders__created_at_week" <= ($2::timestamptz::timestamptz AT TIME ZONE \'UTC\')) GROUP BY 1 ORDER BY 1 ASC LIMIT 10000',
      values: ['2019-11-01T00:00:00Z', '2019-11-30T23:59:59Z'],
      cacheKeyQueries: {
        renewalThreshold: 21600,
        queries: [['SELECT date_trunc(\'hour\', (NOW()::timestamptz AT TIME ZONE \'UTC\')) as current_hour', []]]
      },
      preAggregations: [{
        preAggregationsSchema: 'stb_pre_aggregations',
        tableName: 'stb_pre_aggregations.orders_number_and_count20191102',
        loadSql: ['CREATE TABLE stb_pre_aggregations.orders_number_and_count20191102 AS SELECT\n      date_trunc(\'week\', ("orders".created_at::timestamptz AT TIME ZONE \'UTC\')) "orders__created_at_week", count("orders".id) "orders__count", sum("orders".number) "orders__number"\n    FROM\n      public.orders_delay AS "orders"\n  WHERE ("orders".created_at >= $1::timestamptz AND "orders".created_at <= $2::timestamptz) GROUP BY 1', ['2019-11-01T00:00:00Z', '2019-11-30T23:59:59Z']],
        invalidateKeyQueries: [['SELECT date_trunc(\'hour\', (NOW()::timestamptz AT TIME ZONE \'UTC\')) as current_hour', []]],
        indexesSql: [],
      }],
      renewQuery: true,
      requestId: 'index is part of query key'
    });
    console.log(result.data[0]);
    expect(result.data[0]).toMatch(/orders_number_and_count20191102_c2mipl2c_n0ns2o1y/);
  });

  test('external indexes', async () => {
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
        external: true
      }],
      renewQuery: true,
      requestId: 'external indexes'
    };
    const result = await queryOrchestrator.fetchQuery(query);
    console.log(result.data[0]);
    expect(result.data[0]).toMatch(/orders_number_and_count20191101_l3kvjcmu_khbemovd/);
    expect(externalMockDriver.executedQueries.join(',')).toMatch(/CREATE INDEX orders_number_and_count_week20191101_l3kvjcmu_khbemovd/);
  });

  test('external join', async () => {
    const query = {
      query: 'SELECT * FROM stb_pre_aggregations.orders, stb_pre_aggregations.customers',
      values: [],
      cacheKeyQueries: {
        renewalThreshold: 21600,
        queries: [['SELECT date_trunc(\'hour\', (NOW()::timestamptz AT TIME ZONE \'UTC\')) as current_hour', []]]
      },
      preAggregations: [{
        preAggregationsSchema: 'stb_pre_aggregations',
        tableName: 'stb_pre_aggregations.orders',
        loadSql: ['CREATE TABLE stb_pre_aggregations.orders', []],
        invalidateKeyQueries: [['SELECT date_trunc(\'hour\', (NOW()::timestamptz AT TIME ZONE \'UTC\')) as current_hour', []]],
        external: true,
        dataSource: 'foo'
      }, {
        preAggregationsSchema: 'stb_pre_aggregations',
        tableName: 'stb_pre_aggregations.customers',
        loadSql: ['CREATE TABLE stb_pre_aggregations.customers', []],
        invalidateKeyQueries: [['SELECT date_trunc(\'hour\', (NOW()::timestamptz AT TIME ZONE \'UTC\')) as current_hour', []]],
        external: true,
        dataSource: 'bar'
      }],
      renewQuery: true,
      requestId: 'external join',
      dataSource: 'foo',
      external: true
    };
    const result = await queryOrchestrator.fetchQuery(query);
    console.log(result.data[0]);
    expect(fooMockDriver.executedQueries.join(',')).toMatch(/CREATE TABLE stb_pre_aggregations.orders/);
    expect(barMockDriver.executedQueries.join(',')).toMatch(/CREATE TABLE stb_pre_aggregations.customers/);
    expect(externalMockDriver.tables).toContainEqual(expect.stringMatching(/stb_pre_aggregations.customers/));
    expect(externalMockDriver.tables).toContainEqual(expect.stringMatching(/stb_pre_aggregations.orders/));
    expect(externalMockDriver.executedQueries.join(',')).toMatch(/SELECT \* FROM stb_pre_aggregations\.orders.*, stb_pre_aggregations\.customers.*/);
  });

  test('csv import', async () => {
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
        external: true,
        dataSource: 'csv',
      }],
      renewQuery: true,
      requestId: 'csv import'
    };
    const result = await queryOrchestrator.fetchQuery(query);
    console.log(result.data[0]);
    expect(externalMockDriver.csvFiles).toContainEqual(expect.stringMatching(/orders_number_and_count20191101.*\.csv$/));
  });

  test('non default data source pre-aggregation', async () => {
    const query = {
      query: 'SELECT * FROM stb_pre_aggregations.orders, stb_pre_aggregations.customers',
      values: [],
      cacheKeyQueries: {
        renewalThreshold: 21600,
        queries: [['SELECT date_trunc(\'hour\', (NOW()::timestamptz AT TIME ZONE \'UTC\')) as current_hour', []]]
      },
      preAggregations: [{
        preAggregationsSchema: 'stb_pre_aggregations',
        tableName: 'stb_pre_aggregations.orders',
        loadSql: ['CREATE TABLE stb_pre_aggregations.orders', []],
        invalidateKeyQueries: [['SELECT date_trunc(\'hour\', (NOW()::timestamptz AT TIME ZONE \'UTC\')) as current_hour', []]],
        dataSource: 'foo'
      }],
      renewQuery: true,
      requestId: 'non default data source pre-aggregation',
      dataSource: 'foo',
    };
    const result = await queryOrchestrator.fetchQuery(query);
    console.log(result.data[0]);
    expect(fooMockDriver.executedQueries.join(',')).toMatch(/CREATE TABLE stb_pre_aggregations.orders/);
    expect(mockDriver.executedQueries.length).toEqual(0);
  });

  test('non default data source query', async () => {
    const query = {
      query: 'SELECT * FROM orders',
      values: [],
      cacheKeyQueries: {
        renewalThreshold: 21600,
        queries: [['SELECT date_trunc(\'hour\', (NOW()::timestamptz AT TIME ZONE \'UTC\')) as current_hour', []]]
      },
      renewQuery: true,
      requestId: 'non default data source query',
      dataSource: 'foo',
    };
    const result = await queryOrchestrator.fetchQuery(query);
    console.log(result.data[0]);
    expect(fooMockDriver.executedQueries.join(',')).toMatch(/orders/);
    expect(mockDriver.executedQueries.length).toEqual(0);
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
    mockDriver.tablesQueryDelay = 600;
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
        invalidateKeyQueries: [['SELECT NOW()', [], {
          renewalThreshold: 0.001
        }]],
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

    await mockDriver.delay(200);

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
      mockDriver.delay(300).then(() => {
        if (!firstResolve) {
          firstResolve = 'delay';
        }
      })
    ]);

    await refresh;

    expect(firstResolve).toBe('query');
  });

  test('in memory cache', async () => {
    const query = {
      query: 'SELECT * FROM orders',
      values: [],
      cacheKeyQueries: {
        queries: [
          ['SELECT NOW()', [], {
            renewalThreshold: 21600,
          }],
          ['SELECT date_trunc(\'hour\', (NOW()::timestamptz AT TIME ZONE \'UTC\'))', [], {
            renewalThreshold: 120,
          }]
        ]
      },
      preAggregations: [{
        preAggregationsSchema: 'stb_pre_aggregations',
        tableName: 'stb_pre_aggregations.orders_d20201103',
        loadSql: ['CREATE TABLE stb_pre_aggregations.orders_d20201103 AS SELECT * FROM public.orders', []],
        invalidateKeyQueries: [['SELECT NOW() as now', [], {
          renewalThreshold: 86400,
        }]]
      }],
      requestId: 'in memory cache',
    };
    await queryOrchestrator.fetchQuery(query);
    await queryOrchestrator.fetchQuery(query);
    await queryOrchestrator.fetchQuery(query);
    expect(
      queryOrchestrator.queryCache.memoryCache.has(
        queryOrchestrator.queryCache.queryRedisKey(query.cacheKeyQueries.queries[0].slice(0, 2))
      )
    ).toBe(true);
    expect(
      queryOrchestrator.queryCache.memoryCache.has(
        queryOrchestrator.queryCache.queryRedisKey(query.cacheKeyQueries.queries[1].slice(0, 2))
      )
    ).toBe(false);
    expect(
      queryOrchestrator.queryCache.memoryCache.has(
        queryOrchestrator.queryCache.queryRedisKey(query.preAggregations[0].invalidateKeyQueries[0].slice(0, 2))
      )
    ).toBe(true);
  });

  test('load cache should respect external flag', async () => {
    const preAggregationsLoadCacheByDataSource = {};
    const externalPreAggregation = {
      preAggregationsLoadCacheByDataSource,
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
        external: true,
      }],
      renewQuery: true,
      requestId: 'load cache should respect external flag'
    };
    const internalPreAggregation = {
      preAggregationsLoadCacheByDataSource,
      query: 'SELECT "orders__created_at_week" "orders__created_at_week", sum("orders__count") "orders__count" FROM (SELECT * FROM stb_pre_aggregations.orders_number_and_count20191101) as partition_union  WHERE ("orders__created_at_week" >= ($1::timestamptz::timestamptz AT TIME ZONE \'UTC\') AND "orders__created_at_week" <= ($2::timestamptz::timestamptz AT TIME ZONE \'UTC\')) GROUP BY 1 ORDER BY 1 ASC LIMIT 10000',
      values: ['2019-11-01T00:00:00Z', '2019-11-30T23:59:59Z'],
      cacheKeyQueries: {
        renewalThreshold: 21600,
        queries: [['SELECT date_trunc(\'hour\', (NOW()::timestamptz AT TIME ZONE \'UTC\')) as current_hour', []]]
      },
      preAggregations: [{
        preAggregationsSchema: 'stb_pre_aggregations',
        tableName: 'stb_pre_aggregations.internal',
        loadSql: ['CREATE TABLE stb_pre_aggregations.internal AS SELECT\n      date_trunc(\'week\', ("orders".created_at::timestamptz AT TIME ZONE \'UTC\')) "orders__created_at_week", count("orders".id) "orders__count", sum("orders".number) "orders__number"\n    FROM\n      public.orders AS "orders"\n  WHERE ("orders".created_at >= $1::timestamptz AND "orders".created_at <= $2::timestamptz) GROUP BY 1', ['2019-11-01T00:00:00Z', '2019-11-30T23:59:59Z']],
        invalidateKeyQueries: [['SELECT date_trunc(\'hour\', (NOW()::timestamptz AT TIME ZONE \'UTC\')) as current_hour', []]],
      }],
      renewQuery: true,
      requestId: 'load cache should respect external flag'
    };
    await queryOrchestrator.fetchQuery(internalPreAggregation);
    await queryOrchestrator.fetchQuery(externalPreAggregation);
    await queryOrchestrator.fetchQuery(internalPreAggregation);
    console.log(mockDriver.tables);
    expect(mockDriver.tables.length).toBe(2);
  });

  test('pre-aggregation version entries', async () => {
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
      requestId: 'pre-aggregation version entries'
    });

    await queryOrchestrator.fetchQuery({
      query: 'SELECT * FROM stb_pre_aggregations_2.orders',
      values: [],
      cacheKeyQueries: {
        renewalThreshold: 21600,
        queries: []
      },
      preAggregations: [{
        preAggregationsSchema: 'stb_pre_aggregations_2',
        tableName: 'stb_pre_aggregations_2.orders',
        loadSql: ['CREATE TABLE stb_pre_aggregations_2.orders AS SELECT * FROM public.orders', []],
        invalidateKeyQueries: [['SELECT 3', []]]
      }],
      renewQuery: true,
      requestId: 'pre-aggregation version entries'
    });

    const {
      versionEntriesByTableName,
      structureVersionsByTableName
    } = await queryOrchestrator.getPreAggregationVersionEntries(
      [
        {
          preAggregation: {
            preAggregation: {
              external: false
            }
          },
          partitions: [
            {
              preAggregationsSchema: 'stb_pre_aggregations',
              tableName: 'stb_pre_aggregations.orders',
              loadSql: ['CREATE TABLE stb_pre_aggregations.orders AS SELECT * FROM public.orders', []],
              invalidateKeyQueries: [['SELECT 2', []]]
            }
          ]
        }
      ],
      'stb_pre_aggregations',
      'request-id'
    );

    expect(Object.keys(versionEntriesByTableName).length).toBe(1);
    expect(versionEntriesByTableName).toMatchObject({
      'stb_pre_aggregations.orders': [{
        table_name: 'stb_pre_aggregations.orders',
        content_version: 'mjooke4',
        structure_version: 'ezlvkhjl',
        naming_version: 2
      }]
    });

    expect(Object.keys(structureVersionsByTableName).length).toBe(1);
    expect(structureVersionsByTableName).toMatchObject({
      'stb_pre_aggregations.orders': 'ezlvkhjl'
    });
  });

  test('pre-aggregation schema cache', async () => {
    await queryOrchestrator.fetchQuery({
      query: 'SELECT * FROM pre_aggregations_1.orders',
      values: [],
      cacheKeyQueries: {
        renewalThreshold: 21600,
        queries: []
      },
      preAggregations: [{
        preAggregationsSchema: 'pre_aggregations_1',
        tableName: 'pre_aggregations_1.orders',
        loadSql: ['CREATE TABLE pre_aggregations_1.orders AS SELECT * FROM public.orders WHERE tenant_id = 1', []],
        invalidateKeyQueries: [['SELECT 1', []]]
      }],
      renewQuery: true,
      requestId: 'pre-aggregation schema cache'
    });

    await queryOrchestrator.fetchQuery({
      query: 'SELECT * FROM pre_aggregations_2.orders',
      values: [],
      cacheKeyQueries: {
        renewalThreshold: 21600,
        queries: []
      },
      preAggregations: [{
        preAggregationsSchema: 'pre_aggregations_2',
        tableName: 'pre_aggregations_2.orders',
        loadSql: ['CREATE TABLE pre_aggregations_2.orders AS SELECT * FROM public.orders WHERE tenant_id = 2', []],
        invalidateKeyQueries: [['SELECT 2', []]]
      }],
      renewQuery: true,
      requestId: 'pre-aggregation schema cache'
    });

    await queryOrchestrator.fetchQuery({
      query: 'SELECT * FROM pre_aggregations_1.orders',
      values: [],
      cacheKeyQueries: {
        renewalThreshold: 21600,
        queries: []
      },
      preAggregations: [{
        preAggregationsSchema: 'pre_aggregations_1',
        tableName: 'pre_aggregations_1.orders',
        loadSql: ['CREATE TABLE pre_aggregations_1.orders AS SELECT * FROM public.orders WHERE tenant_id = 1', []],
        invalidateKeyQueries: [['SELECT 1', []]]
      }],
      renewQuery: true,
      requestId: 'pre-aggregation schema cache'
    });

    console.log(mockDriver.tables);
    expect(mockDriver.tables.length).toEqual(2);
  });

  test('range partitions', async () => {
    const query = {
      query: 'SELECT * FROM stb_pre_aggregations.orders_d',
      values: [],
      cacheKeyQueries: {
        queries: []
      },
      preAggregations: [{
        preAggregationsSchema: 'stb_pre_aggregations',
        tableName: 'stb_pre_aggregations.orders_d',
        loadSql: [
          'CREATE TABLE stb_pre_aggregations.orders_d AS SELECT * FROM public.orders WHERE timestamp >= ? AND timestamp <= ?',
          ['__FROM_PARTITION_RANGE', '__TO_PARTITION_RANGE']
        ],
        invalidateKeyQueries: [['SELECT CASE WHEN NOW() > ? THEN NOW() END as now', ['__TO_PARTITION_RANGE'], {
          renewalThreshold: 1,
          updateWindowSeconds: 86400,
          renewalThresholdOutsideUpdateWindow: 86400,
          incremental: true
        }]],
        indexesSql: [{
          sql: ['CREATE INDEX orders_d_main ON stb_pre_aggregations.orders_d ("orders__created_at")', []],
          indexName: 'orders_d_main'
        }],
        preAggregationStartEndQueries: [
          ['SELECT MIN(timestamp) FROM orders', []],
          ['SELECT MAX(timestamp) FROM orders', []],
        ],
        partitionGranularity: 'day',
        timezone: 'UTC'
      }],
      requestId: 'range partitions',
    };
    await queryOrchestrator.fetchQuery(query);
    console.log(JSON.stringify(mockDriver.executedQueries));
    const nowQueries = mockDriver.executedQueries.filter(q => q.match(/NOW/)).length;
    await mockDriver.delay(2000);
    await queryOrchestrator.fetchQuery(query);
    console.log(JSON.stringify(mockDriver.executedQueries));
    expect(mockDriver.executedQueries.filter(q => q.match(/NOW/)).length).toEqual(nowQueries);
  });

  test('range partitions exceed maximum number', async () => {
    const query = {
      query: 'SELECT * FROM stb_pre_aggregations.orders_d',
      values: [],
      cacheKeyQueries: {
        queries: []
      },
      preAggregations: [{
        preAggregationsSchema: 'stb_pre_aggregations',
        tableName: 'stb_pre_aggregations.orders_d',
        loadSql: [
          'CREATE TABLE stb_pre_aggregations.orders_d AS SELECT * FROM public.orders WHERE timestamp >= ? AND timestamp <= ?',
          ['__FROM_PARTITION_RANGE', '__TO_PARTITION_RANGE']
        ],
        invalidateKeyQueries: [['SELECT CASE WHEN NOW() > ? THEN NOW() END as now', ['__TO_PARTITION_RANGE'], {
          renewalThreshold: 1,
          updateWindowSeconds: 86400,
          renewalThresholdOutsideUpdateWindow: 86400,
          incremental: true
        }]],
        indexesSql: [{
          sql: ['CREATE INDEX orders_d_main ON stb_pre_aggregations.orders_d ("orders__created_at")', []],
          indexName: 'orders_d_main'
        }],
        preAggregationStartEndQueries: [
          ['SELECT MIN(timestamp) FROM orders', []],
          ['SELECT MAX(timestamp) FROM orders', []],
        ],
        partitionGranularity: 'hour',
        timezone: 'UTC'
      }],
      requestId: 'range partitions',
    };
    await expect(async () => {
      await queryOrchestrator.fetchQuery(query);
    }).rejects.toThrow(
      'The maximum number of partitions (32) was reached for the pre-aggregation'
    );
  });

  test('empty partitions', async () => {
    const query = {
      query: 'SELECT * FROM stb_pre_aggregations.orders_d',
      values: [],
      cacheKeyQueries: {
        queries: []
      },
      preAggregations: [{
        preAggregationsSchema: 'stb_pre_aggregations',
        tableName: 'stb_pre_aggregations.orders_empty',
        loadSql: [
          'CREATE TABLE stb_pre_aggregations.orders_empty AS SELECT * FROM public.orders WHERE created_at >= ? AND created_at <= ?',
          ['__FROM_PARTITION_RANGE', '__TO_PARTITION_RANGE']
        ],
        invalidateKeyQueries: [['SELECT CASE WHEN NOW() > ? THEN NOW() END as now', ['__TO_PARTITION_RANGE'], {
          renewalThreshold: 1,
          updateWindowSeconds: 86400,
          renewalThresholdOutsideUpdateWindow: 86400,
          incremental: true
        }]],
        indexesSql: [{
          sql: ['CREATE INDEX orders_d_main ON stb_pre_aggregations.orders_d ("orders__created_at")', []],
          indexName: 'orders_d_main'
        }],
        preAggregationStartEndQueries: [
          ['SELECT MIN(created_at) FROM orders', []],
          ['SELECT MAX(created_at) FROM orders', []],
        ],
        partitionGranularity: 'day',
        timezone: 'UTC'
      }],
      requestId: 'empty partitions',
    };
    await queryOrchestrator.fetchQuery(query);
    console.log(JSON.stringify(mockDriver.executedQueries));
    expect(mockDriver.tables.length).toEqual(1);
  });

  test('empty partitions with externalRefresh', async () => {
    const query = {
      query: 'SELECT * FROM stb_pre_aggregations.orders_d',
      values: [],
      cacheKeyQueries: {
        queries: []
      },
      preAggregations: [{
        preAggregationsSchema: 'stb_pre_aggregations',
        tableName: 'stb_pre_aggregations.orders_empty',
        loadSql: [
          'CREATE TABLE stb_pre_aggregations.orders_empty AS SELECT * FROM public.orders WHERE created_at >= ? AND created_at <= ?',
          ['__FROM_PARTITION_RANGE', '__TO_PARTITION_RANGE']
        ],
        invalidateKeyQueries: [['SELECT CASE WHEN NOW() > ? THEN NOW() END as now', ['__TO_PARTITION_RANGE'], {
          renewalThreshold: 1,
          updateWindowSeconds: 86400,
          renewalThresholdOutsideUpdateWindow: 86400,
          incremental: true
        }]],
        indexesSql: [{
          sql: ['CREATE INDEX orders_d_main ON stb_pre_aggregations.orders_d ("orders__created_at")', []],
          indexName: 'orders_d_main'
        }],
        preAggregationStartEndQueries: [
          ['SELECT MIN(created_at) FROM orders', []],
          ['SELECT MAX(created_at) FROM orders', []],
        ],
        partitionGranularity: 'day',
        timezone: 'UTC'
      }],
      requestId: 'empty partitions',
    };
    await expect(async () => {
      await queryOrchestratorExternalRefresh.fetchQuery(query);
    }).rejects.toThrow(
      'Your configuration restricts query requests to only be served from ' +
      'pre-aggregations, and required pre-aggregation partitions were not ' +
      'built yet. Please make sure your refresh worker is configured ' +
      'correctly and running.'
    );
  });

  test('empty intersection', async () => {
    const query = {
      query: 'SELECT * FROM stb_pre_aggregations.orders_d',
      values: [],
      cacheKeyQueries: {
        queries: []
      },
      preAggregations: [{
        preAggregationsSchema: 'stb_pre_aggregations',
        tableName: 'stb_pre_aggregations.orders_d',
        loadSql: [
          'CREATE TABLE stb_pre_aggregations.orders_d AS SELECT * FROM public.orders WHERE timestamp >= ? AND timestamp <= ?',
          ['__FROM_PARTITION_RANGE', '__TO_PARTITION_RANGE']
        ],
        invalidateKeyQueries: [['SELECT CASE WHEN NOW() > ? THEN NOW() END as now', ['__TO_PARTITION_RANGE'], {
          renewalThreshold: 1,
          updateWindowSeconds: 86400,
          renewalThresholdOutsideUpdateWindow: 86400,
          incremental: true
        }]],
        indexesSql: [{
          sql: ['CREATE INDEX orders_d_main ON stb_pre_aggregations.orders_d ("orders__created_at")', []],
          indexName: 'orders_d_main'
        }],
        preAggregationStartEndQueries: [
          ['SELECT MIN(timestamp) FROM orders', []],
          ['SELECT MAX(timestamp) FROM orders', []],
        ],
        matchedTimeDimensionDateRange: ['2021-08-01T00:00:00.000', '2021-08-30T00:00:00.000'],
        partitionGranularity: 'day',
        timezone: 'UTC'
      }],
      requestId: 'empty intersection',
    };
    const result = await queryOrchestrator.fetchQuery(query);
    expect(result.data[0]).toMatch(/orders_d20210601/);
  });

  test('loadRefreshKeys', async () => {
    const preAggregationsLoadCacheByDataSource = {};
    const preAggregationExternalRefreshKey = {
      preAggregationsLoadCacheByDataSource,
      cacheKeyQueries: {
        renewalThreshold: 21600,
        queries: [
          ['SELECT refreshKey in source database', [], {
            renewalThreshold: 21600,
            external: false,
          }],
          ['SELECT refreshKey in external database', [], {
            renewalThreshold: 21600,
            external: true,
          }]
        ]
      },
      preAggregations: [],
      renewQuery: true,
      requestId: 'loadRefreshKeys should respect external flag'
    };

    const driverQueryMock = jest.spyOn(mockDriver, 'query').mockImplementation(async () => []);
    const externalDriverQueryMock = jest.spyOn(externalMockDriver, 'query').mockImplementation(async () => []);

    const refreshKeys = await queryOrchestrator.loadRefreshKeys(preAggregationExternalRefreshKey);
    console.log(refreshKeys);

    expect(driverQueryMock.mock.calls.length).toEqual(1);
    expect(driverQueryMock.mock.calls[0]).toEqual([
      'SELECT refreshKey in source database',
      [],
      {
        queryKey: [
          'SELECT refreshKey in source database',
          [],
        ],
        query: 'SELECT refreshKey in source database',
        values: [],
        requestId: preAggregationExternalRefreshKey.requestId,
      }
    ]);

    expect(externalDriverQueryMock.mock.calls.length).toEqual(1);
    expect(externalDriverQueryMock.mock.calls[0]).toEqual([
      'SELECT refreshKey in external database',
      [],
      {
        queryKey: [
          'SELECT refreshKey in external database',
          [],
        ],
        query: 'SELECT refreshKey in external database',
        values: [],
        requestId: preAggregationExternalRefreshKey.requestId,
      }
    ]);
  });
});
