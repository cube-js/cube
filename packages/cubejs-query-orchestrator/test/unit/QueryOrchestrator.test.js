/* globals jest, describe, beforeEach, afterEach, test, expect */
import { Readable } from 'stream';
import { QueryOrchestrator } from '../../src/orchestrator/QueryOrchestrator';

class MockDriver {
  constructor({ csvImport, schemaData } = {}) {
    this.tablesObj = [];
    this.tablesReady = [];
    this.executedQueries = [];
    this.cancelledQueries = [];
    this.droppedTables = [];
    this.csvImport = csvImport;
    this.now = new Date().getTime();
    this.schemaData = schemaData;
  }

  get tables() {
    return this.tablesObj.map(t => t.tableName || t);
  }

  resetTables() {
    this.tablesObj = [];
  }

  query(query) {
    this.executedQueries.push(query);

    // Handle metadata operations - check if query is an array with metadata operation
    if (Array.isArray(query) && query.length > 0 && typeof query[0] === 'string') {
      const operation = query[0];
      if (operation === 'METADATA:GET_SCHEMAS') {
        return this.getSchemas();
      } else if (operation === 'METADATA:GET_TABLES_FOR_SCHEMAS') {
        // Parse parameters from the query array
        let params = {};
        try {
          params = query[1] && query[1].length > 0 ? JSON.parse(query[1][0]) : {};
        } catch (error) {
          console.warn('Failed to parse JSON parameters for METADATA:GET_TABLES_FOR_SCHEMAS:', error);
        }
        return this.getTablesForSpecificSchemas(params.schemas || []);
      } else if (operation === 'METADATA:GET_COLUMNS_FOR_TABLES') {
        // Parse parameters from the query array
        let params = {};
        try {
          params = query[1] && query[1].length > 0 ? JSON.parse(query[1][0]) : {};
        } catch (error) {
          console.warn('Failed to parse JSON parameters for METADATA:GET_COLUMNS_FOR_TABLES:', error);
        }
        return this.getColumnsForSpecificTables(params.tables || []);
      }
    }

    // Handle regular SQL queries - ensure query is a string
    if (typeof query !== 'string') {
      return Promise.resolve([]);
    }

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

    if (query.match(/^SELECT '(\d+-\d+-\d+)'/)) {
      promise = promise.then(() => [{ date: new Date(`${query.match(/^SELECT '(\d+-\d+-\d+)'/)[1]}T00:00:00.000Z`).toJSON() }]);
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
    return this.tablesObj.filter(t => (t.tableName || t).split('.')[0] === schema)
      .map(t => ({
        table_name: (t.tableName || t).replace(`${schema}.`, ''),
        build_range_end: t.buildRangeEnd
      }));
  }

  delay(timeout) {
    return new Promise(resolve => setTimeout(() => resolve(), timeout));
  }

  async createSchemaIfNotExists(schema) {
    this.schema = schema;
    return null;
  }

  loadPreAggregationIntoTable(preAggregationTableName, loadSql) {
    this.tablesObj.push({ tableName: preAggregationTableName.substring(0, 100) });
    const promise = this.query(loadSql);
    const resPromise = promise.then(() => this.tablesReady.push(preAggregationTableName.substring(0, 100)));
    resPromise.cancel = promise.cancel;
    return resPromise;
  }

  async dropTable(tableName) {
    if (this.droppedTables.indexOf(tableName) !== -1) {
      throw new Error(`Can't drop table twice: ${tableName}`);
    }
    this.droppedTables.push(tableName);
    console.log(`Driver drops ${tableName}`);
    if (!this.tablesObj.find(t => (t.tableName || t) === tableName)) {
      throw new Error(`Can't drop missing table: ${tableName}`);
    }
    await this.query(`DROP TABLE ${tableName}`);
    if (this.tablesDropDelay) {
      await this.delay(this.tablesDropDelay);
    }
    if (!this.tablesObj.find(t => (t.tableName || t) === tableName)) {
      throw new Error(`Can't drop missing table: ${tableName}`);
    }
    this.tablesObj = this.tablesObj.filter(t => (t.tableName || t) !== tableName);
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

  capabilities() {
    return {};
  }

  async stream(sql) {
    return {
      rowStream: Readable.from((await this.query(sql)).map(r => (typeof r === 'string' ? { query: r } : r)))
    };
  }
}

class ExternalMockDriver extends MockDriver {
  constructor() {
    super();
    this.indexes = [];
    this.csvFiles = [];
  }

  async uploadTable(table) {
    this.tablesObj.push({ tableName: table.substring(0, 100) });
    throw new Error('uploadTable has been called instead of uploadTableWithIndexes');
  }

  async uploadTableWithIndexes(table, columns, tableData, indexesSql, uniqueKeyColumns, queryTracingObj, externalOptions) {
    this.tablesObj.push({
      tableName: table.substring(0, 100),
      buildRangeEnd: queryTracingObj?.buildRangeEnd,
      sealAt: externalOptions?.sealAt
    });
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

class MockDriverUnloadWithoutTempTableSupport extends MockDriver {
  capabilities() {
    return { unloadWithoutTempTable: true };
  }

  queryColumnTypes() {
    return [];
  }
}

class StreamingSourceMockDriver extends MockDriver {
  capabilities() {
    return { streamingSource: true };
  }

  loadPreAggregationIntoTable(preAggregationTableName, loadSql, params, options) {
    this.loadPreAggregationIntoTableStreamOffset = options.streamOffset;
    return super.loadPreAggregationIntoTable(preAggregationTableName, loadSql, options);
  }

  async downloadTable(table, { csvImport, streamOffset } = {}) {
    this.downloadTableStreamOffset = streamOffset;
    return super.downloadTable(table, { csvImport });
  }

  async downloadQueryResults(query, params, options) {
    this.downloadTableStreamOffset = options.streamOffset;
    return super.downloadTable(query);
  }
}

describe('QueryOrchestrator', () => {
  jest.setTimeout(15000);
  let mockDriver = null;
  let fooMockDriver = null;
  let barMockDriver = null;
  let mockDriverUnloadWithoutTempTableSupport = null;
  let streamingSourceMockDriver = null;
  let externalMockDriver = null;
  let queryOrchestrator = null;
  let queryOrchestrator2 = null;
  let queryOrchestratorExternalRefresh = null;
  let queryOrchestratorDropWithoutTouch = null;
  let testCount = 1;
  const schemaData = {
    public: {
      orders: [
        {
          name: 'id',
          type: 'integer',
          attributes: [],
        },
      ],
    },
  };

  beforeEach(() => {
    const mockDriverLocal = new MockDriver();
    const fooMockDriverLocal = new MockDriver({ schemaData });
    const barMockDriverLocal = new MockDriver();
    const csvMockDriverLocal = new MockDriver({ csvImport: 'true' });
    const mockDriverUnloadWithoutTempTableSupportLocal = new MockDriverUnloadWithoutTempTableSupport();
    const streamingSourceMockDriverLocal = new StreamingSourceMockDriver();
    const externalMockDriverLocal = new ExternalMockDriver();

    const redisPrefix = `ORCHESTRATOR_TEST_${testCount++}`;
    const driverFactory = (dataSource) => {
      if (dataSource === 'foo') {
        return fooMockDriverLocal;
      } else if (dataSource === 'bar') {
        return barMockDriverLocal;
      } else if (dataSource === 'mockDriverUnloadWithoutTempTableSupport') {
        return mockDriverUnloadWithoutTempTableSupportLocal;
      } else if (dataSource === 'streaming') {
        return streamingSourceMockDriverLocal;
      } else if (dataSource === 'csv') {
        return csvMockDriverLocal;
      } else {
        return mockDriverLocal;
      }
    };
    const logger =
      (msg, params) => console.log(new Date().toJSON(), msg, params);
    const options = (processUid) => ({
      externalDriverFactory: () => externalMockDriverLocal,
      queryCacheOptions: {
        queueOptions: () => ({
          concurrency: 2,
          processUid,
        }),
      },
      preAggregationsOptions: {
        maxPartitions: 100,
        queueOptions: () => ({
          executionTimeout: 2,
          concurrency: 2,
          processUid,
        }),
        usedTablePersistTime: 1
      },
    });

    queryOrchestrator =
      new QueryOrchestrator(redisPrefix, driverFactory, logger, options('p1'));
    queryOrchestrator2 =
      new QueryOrchestrator(redisPrefix, driverFactory, logger, options('p2'));
    queryOrchestratorExternalRefresh =
      new QueryOrchestrator(redisPrefix, driverFactory, logger, {
        ...options('p1'),
        preAggregationsOptions: {
          ...options('p1').preAggregationsOptions,
          externalRefresh: true,
        },
      });
    queryOrchestratorDropWithoutTouch =
      new QueryOrchestrator(redisPrefix, driverFactory, logger, {
        ...options('p1'),
        preAggregationsOptions: {
          ...options('p1').preAggregationsOptions,
          dropPreAggregationsWithoutTouch: true,
          touchTablePersistTime: 1,
        },
      });
    mockDriver = mockDriverLocal;
    fooMockDriver = fooMockDriverLocal;
    barMockDriver = barMockDriverLocal;
    externalMockDriver = externalMockDriverLocal;
    mockDriverUnloadWithoutTempTableSupport = mockDriverUnloadWithoutTempTableSupportLocal;
    streamingSourceMockDriver = streamingSourceMockDriverLocal;
  });

  afterEach(async () => {
    await queryOrchestrator.cleanup();
    await queryOrchestratorExternalRefresh.cleanup();
    await queryOrchestratorDropWithoutTouch.cleanup();
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
    mockDriver.resetTables();
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

  test('in memory expire', async () => {
    const query = (id) => ({
      query: 'SELECT * FROM orders',
      values: [],
      cacheKeyQueries: {
        queries: [
          ['SELECT NOW()', [], {
            renewalThreshold: 21600,
          }],
          ['SELECT date_trunc(\'hour\', (NOW()::timestamptz AT TIME ZONE \'UTC\'))', [], {
            renewalThreshold: 21600,
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
      expireSecs: 2,
      requestId: `in memory expire ${id}`,
    });
    await queryOrchestrator.fetchQuery(query(0));
    await queryOrchestrator.fetchQuery(query(1));
    await mockDriver.delay(2000);
    await queryOrchestrator.fetchQuery(query(2));
    expect(
      mockDriver.executedQueries.filter(q => q.match(/timestamptz/)).length
    ).toBe(2);
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
    await queryOrchestrator.fetchQuery(externalPreAggregation);
    console.log(mockDriver.tables);
    expect(mockDriver.tables.length).toBe(1);
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
        timestampPrecision: 3,
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
        timestampPrecision: 3,
        timezone: 'UTC'
      }],
      requestId: 'range partitions',
    };
    await expect(async () => {
      await queryOrchestrator.fetchQuery(query);
    }).rejects.toThrow(
      'Pre-aggregation \'stb_pre_aggregations.orders_d\' requested to build 745 partitions which exceeds the maximum number of partitions per pre-aggregation of 100'
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
        timestampPrecision: 3,
        timezone: 'UTC'
      }],
      requestId: 'empty partitions',
    };
    await queryOrchestrator.fetchQuery(query);
    console.log(JSON.stringify(mockDriver.executedQueries));
    expect(mockDriver.tables.length).toEqual(1);
  });

  test('empty partitions with externalRefresh', async () => {
    const query = ({ startQuery, endQuery, matchedTimeDimensionDateRange }) => ({
      query: 'SELECT * FROM stb_pre_aggregations.orders_empty',
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
          [startQuery || 'SELECT MIN(created_at) FROM orders', []],
          [endQuery || 'SELECT MAX(created_at) FROM orders', []],
        ],
        partitionGranularity: 'day',
        timestampPrecision: 3,
        timezone: 'UTC',
        matchedTimeDimensionDateRange
      }],
      requestId: 'empty partitions with externalRefresh',
    });
    await expect(async () => {
      await queryOrchestratorExternalRefresh.fetchQuery(query({}));
    }).rejects.toThrow(
      /refresh worker/
    );
    await queryOrchestrator.fetchQuery(query({ startQuery: 'SELECT \'2021-05-01\'', endQuery: 'SELECT \'2021-05-15\'' }));
    const result = await queryOrchestratorExternalRefresh.fetchQuery(query({
      startQuery: 'SELECT \'2021-05-01\'',
      endQuery: 'SELECT \'2021-05-15\'',
      matchedTimeDimensionDateRange: ['2021-05-31T00:00:00.000', '2021-05-31T23:59:59.999']
    }));
    console.log(JSON.stringify(result, null, 2));
    expect(result.data[0]).toMatch(/orders_empty20210515/);
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
        timestampPrecision: 3,
        timezone: 'UTC'
      }],
      requestId: 'empty intersection',
    };
    const result = await queryOrchestrator.fetchQuery(query);
    expect(result.data[0]).toMatch(/orders_d20210601/);
  });

  test('lambda partitions', async () => {
    const query = (matchedTimeDimensionDateRange) => ({
      query: 'SELECT * FROM stb_pre_aggregations.orders_d UNION ALL SELECT * FROM stb_pre_aggregations.orders_h',
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
        preAggregationStartEndQueries: [
          ['SELECT MIN(timestamp) FROM orders', []],
          ['SELECT \'2021-05-31\'', []],
        ],
        external: true,
        partitionGranularity: 'day',
        timestampPrecision: 3,
        timezone: 'UTC',
        rollupLambdaId: 'orders.d_lambda',
        matchedTimeDimensionDateRange
      }, {
        preAggregationsSchema: 'stb_pre_aggregations',
        tableName: 'stb_pre_aggregations.orders_h',
        loadSql: [
          'CREATE TABLE stb_pre_aggregations.orders_h AS SELECT * FROM public.orders WHERE timestamp >= ? AND timestamp <= ?',
          ['__FROM_PARTITION_RANGE', '__TO_PARTITION_RANGE']
        ],
        invalidateKeyQueries: [['SELECT CASE WHEN NOW() > ? THEN NOW() END as now', ['__TO_PARTITION_RANGE'], {
          renewalThreshold: 1,
          updateWindowSeconds: 86400,
          renewalThresholdOutsideUpdateWindow: 86400,
          incremental: true
        }]],
        preAggregationStartEndQueries: [
          ['SELECT \'2021-05-30\'', []],
          ['SELECT MAX(timestamp) FROM orders', []],
        ],
        external: true,
        partitionGranularity: 'hour',
        timestampPrecision: 3,
        timezone: 'UTC',
        rollupLambdaId: 'orders.d_lambda',
        lastRollupLambda: true,
        matchedTimeDimensionDateRange
      }],
      requestId: 'lambda partitions',
      external: true,
    });
    let result = await queryOrchestrator.fetchQuery(query());
    console.log(JSON.stringify(result, null, 2));
    expect(result.data[0]).toMatch(/orders_d20210501/);
    expect(result.data[0]).not.toMatch(/orders_h2021053000/);
    expect(result.data[0]).toMatch(/orders_h2021053100/);
    expect(result.data[0]).toMatch(/orders_h2021060100_uozkyaur_d004iq51/);

    result = await queryOrchestrator.fetchQuery(query(['2021-05-31T00:00:00.000', '2021-05-31T23:59:59.999']));
    console.log(JSON.stringify(result, null, 2));
    expect(result.data[0]).toMatch(/orders_h2021053100/);

    result = await queryOrchestratorExternalRefresh.fetchQuery(query());
    console.log(JSON.stringify(result, null, 2));
    expect(result.data[0]).toMatch(/orders_d20210501/);
    expect(result.data[0]).not.toMatch(/orders_h2021053000/);
    expect(result.data[0]).toMatch(/orders_h2021053100/);
    expect(result.data[0]).toMatch(/orders_h2021060100_uozkyaur_d004iq51/);
  });

  test('lambda partitions week', async () => {
    const query = (matchedTimeDimensionDateRange) => ({
      query: 'SELECT * FROM stb_pre_aggregations.orders_w UNION ALL SELECT * FROM stb_pre_aggregations.orders_d UNION ALL SELECT * FROM stb_pre_aggregations.orders_h',
      values: [],
      cacheKeyQueries: {
        queries: []
      },
      preAggregations: [{
        preAggregationsSchema: 'stb_pre_aggregations',
        tableName: 'stb_pre_aggregations.orders_w',
        loadSql: [
          'CREATE TABLE stb_pre_aggregations.orders_w AS SELECT * FROM public.orders WHERE timestamp >= ? AND timestamp <= ?',
          ['__FROM_PARTITION_RANGE', '__TO_PARTITION_RANGE']
        ],
        invalidateKeyQueries: [['SELECT CASE WHEN NOW() > ? THEN NOW() END as now', ['__TO_PARTITION_RANGE'], {
          renewalThreshold: 1,
          updateWindowSeconds: 86400,
          renewalThresholdOutsideUpdateWindow: 86400,
          incremental: true
        }]],
        preAggregationStartEndQueries: [
          ['SELECT MIN(timestamp) FROM orders', []],
          ['SELECT \'2021-05-31\'', []],
        ],
        external: true,
        partitionGranularity: 'week',
        timestampPrecision: 3,
        timezone: 'UTC',
        rollupLambdaId: 'orders.d_lambda',
        matchedTimeDimensionDateRange
      }, {
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
        preAggregationStartEndQueries: [
          ['SELECT MIN(timestamp) FROM orders', []],
          ['SELECT \'2021-05-31\'', []],
        ],
        external: true,
        partitionGranularity: 'day',
        timestampPrecision: 3,
        timezone: 'UTC',
        rollupLambdaId: 'orders.d_lambda',
        matchedTimeDimensionDateRange
      }, {
        preAggregationsSchema: 'stb_pre_aggregations',
        tableName: 'stb_pre_aggregations.orders_h',
        loadSql: [
          'CREATE TABLE stb_pre_aggregations.orders_h AS SELECT * FROM public.orders WHERE timestamp >= ? AND timestamp <= ?',
          ['__FROM_PARTITION_RANGE', '__TO_PARTITION_RANGE']
        ],
        invalidateKeyQueries: [['SELECT CASE WHEN NOW() > ? THEN NOW() END as now', ['__TO_PARTITION_RANGE'], {
          renewalThreshold: 1,
          updateWindowSeconds: 86400,
          renewalThresholdOutsideUpdateWindow: 86400,
          incremental: true
        }]],
        preAggregationStartEndQueries: [
          ['SELECT \'2021-05-30\'', []],
          ['SELECT MAX(timestamp) FROM orders', []],
        ],
        external: true,
        partitionGranularity: 'hour',
        timestampPrecision: 3,
        timezone: 'UTC',
        rollupLambdaId: 'orders.d_lambda',
        lastRollupLambda: true,
        matchedTimeDimensionDateRange
      }],
      requestId: 'lambda partitions',
      external: true,
    });
    const result = await queryOrchestrator.fetchQuery(query());
    console.log(JSON.stringify(result, null, 2));
    expect(result.data[0]).not.toMatch(/orders_h2021053000/);
    expect(result.data[0]).toMatch(/orders_h2021053100/);
    expect(result.data[0]).toMatch(/orders_h2021060100_uozkyaur_d004iq51/);
  });

  test('real-time sealing partitions', async () => {
    const query = (matchedTimeDimensionDateRange) => ({
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
        partitionInvalidateKeyQueries: [],
        preAggregationStartEndQueries: [
          ['SELECT MIN(timestamp) FROM orders', []],
          ['SELECT \'2021-05-31\'', []],
        ],
        external: true,
        partitionGranularity: 'day',
        timestampPrecision: 3,
        timezone: 'UTC',
        matchedTimeDimensionDateRange
      }],
      requestId: 'real-time sealing partitions',
      external: true,
    });
    const result = await queryOrchestrator.fetchQuery(query());
    console.log(JSON.stringify(result, null, 2));
    expect(externalMockDriver.tablesObj.find(t => t.tableName.indexOf('stb_pre_aggregations.orders_d20210531') !== -1).sealAt).toBe('2021-05-31T23:59:59.999Z');
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
        useCsvQuery: undefined,
        inlineTables: undefined,
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
        useCsvQuery: undefined,
        inlineTables: undefined,
      }
    ]);
  });

  test('preaggregation without temp table', async () => {
    mockDriverUnloadWithoutTempTableSupport.now = 12345000;
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
        sql: ['SELECT\n      date_trunc(\'week\', ("orders".created_at::timestamptz AT TIME ZONE \'UTC\')) "orders__created_at_week", count("orders".id) "orders__count", sum("orders".number) "orders__number"\n    FROM\n      public.orders AS "orders"\n  WHERE ("orders".created_at >= $1::timestamptz AND "orders".created_at <= $2::timestamptz) GROUP BY 1', ['2019-11-01T00:00:00Z', '2019-11-30T23:59:59Z']],
        loadSql: ['CREATE TABLE stb_pre_aggregations.orders_number_and_count20191101 AS SELECT\n      date_trunc(\'week\', ("orders".created_at::timestamptz AT TIME ZONE \'UTC\')) "orders__created_at_week", count("orders".id) "orders__count", sum("orders".number) "orders__number"\n    FROM\n      public.orders AS "orders"\n  WHERE ("orders".created_at >= $1::timestamptz AND "orders".created_at <= $2::timestamptz) GROUP BY 1', ['2019-11-01T00:00:00Z', '2019-11-30T23:59:59Z']],
        invalidateKeyQueries: [['SELECT date_trunc(\'hour\', (NOW()::timestamptz AT TIME ZONE \'UTC\')) as current_hour', []]],
        dataSource: 'mockDriverUnloadWithoutTempTableSupport',
        external: true,
      }],
      renewQuery: true,

      requestId: 'basic'
    };
    const promise = queryOrchestrator.fetchQuery(query);
    const result = await promise;
    expect(result.data[0]).toMatch(/orders_number_and_count20191101_kjypcoio_5yftl5il/);
  });

  test('streaming source tables are not dropped', async () => {
    streamingSourceMockDriver.now = 12345000;
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
        sql: ['SELECT\n      date_trunc(\'week\', ("orders".created_at::timestamptz AT TIME ZONE \'UTC\')) "orders__created_at_week", count("orders".id) "orders__count", sum("orders".number) "orders__number"\n    FROM\n      public.orders AS "orders"\n  WHERE ("orders".created_at >= $1::timestamptz AND "orders".created_at <= $2::timestamptz) GROUP BY 1', ['2019-11-01T00:00:00Z', '2019-11-30T23:59:59Z']],
        loadSql: ['CREATE TABLE stb_pre_aggregations.orders_number_and_count20191101 AS SELECT\n      date_trunc(\'week\', ("orders".created_at::timestamptz AT TIME ZONE \'UTC\')) "orders__created_at_week", count("orders".id) "orders__count", sum("orders".number) "orders__number"\n    FROM\n      public.orders AS "orders"\n  WHERE ("orders".created_at >= $1::timestamptz AND "orders".created_at <= $2::timestamptz) GROUP BY 1', ['2019-11-01T00:00:00Z', '2019-11-30T23:59:59Z']],
        invalidateKeyQueries: [['SELECT date_trunc(\'hour\', (NOW()::timestamptz AT TIME ZONE \'UTC\')) as current_hour', []]],
        dataSource: 'streaming',
        external: true,
      }],
      renewQuery: true,

      requestId: 'basic'
    };
    await queryOrchestrator.fetchQuery(query);
    expect(streamingSourceMockDriver.tables[0]).toMatch(/orders_number_and_count20191101_kjypcoio_5yftl5il/);
  });

  test('streaming receives stream offset', async () => {
    streamingSourceMockDriver.now = 12345000;
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
        sql: ['SELECT\n      date_trunc(\'week\', ("orders".created_at::timestamptz AT TIME ZONE \'UTC\')) "orders__created_at_week", count("orders".id) "orders__count", sum("orders".number) "orders__number"\n    FROM\n      public.orders AS "orders"\n  WHERE ("orders".created_at >= $1::timestamptz AND "orders".created_at <= $2::timestamptz) GROUP BY 1', ['2019-11-01T00:00:00Z', '2019-11-30T23:59:59Z']],
        loadSql: ['CREATE TABLE stb_pre_aggregations.orders_number_and_count20191101 AS SELECT\n      date_trunc(\'week\', ("orders".created_at::timestamptz AT TIME ZONE \'UTC\')) "orders__created_at_week", count("orders".id) "orders__count", sum("orders".number) "orders__number"\n    FROM\n      public.orders AS "orders"\n  WHERE ("orders".created_at >= $1::timestamptz AND "orders".created_at <= $2::timestamptz) GROUP BY 1', ['2019-11-01T00:00:00Z', '2019-11-30T23:59:59Z']],
        invalidateKeyQueries: [['SELECT date_trunc(\'hour\', (NOW()::timestamptz AT TIME ZONE \'UTC\')) as current_hour', []]],
        dataSource: 'streaming',
        external: true,
        streamOffset: 'earliest'
      }],
      renewQuery: true,

      requestId: 'basic'
    };
    await queryOrchestrator.fetchQuery(query);

    expect(streamingSourceMockDriver.loadPreAggregationIntoTableStreamOffset).toBe('earliest');
    expect(streamingSourceMockDriver.downloadTableStreamOffset).toBe('earliest');
  });

  test('streaming receives stream offset readOnly', async () => {
    streamingSourceMockDriver.now = 12345000;
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
        sql: ['SELECT\n      date_trunc(\'week\', ("orders".created_at::timestamptz AT TIME ZONE \'UTC\')) "orders__created_at_week", count("orders".id) "orders__count", sum("orders".number) "orders__number"\n    FROM\n      public.orders AS "orders"\n  WHERE ("orders".created_at >= $1::timestamptz AND "orders".created_at <= $2::timestamptz) GROUP BY 1', ['2019-11-01T00:00:00Z', '2019-11-30T23:59:59Z']],
        loadSql: ['CREATE TABLE stb_pre_aggregations.orders_number_and_count20191101 AS SELECT\n      date_trunc(\'week\', ("orders".created_at::timestamptz AT TIME ZONE \'UTC\')) "orders__created_at_week", count("orders".id) "orders__count", sum("orders".number) "orders__number"\n    FROM\n      public.orders AS "orders"\n  WHERE ("orders".created_at >= $1::timestamptz AND "orders".created_at <= $2::timestamptz) GROUP BY 1', ['2019-11-01T00:00:00Z', '2019-11-30T23:59:59Z']],
        invalidateKeyQueries: [['SELECT date_trunc(\'hour\', (NOW()::timestamptz AT TIME ZONE \'UTC\')) as current_hour', []]],
        dataSource: 'streaming',
        external: true,
        streamOffset: 'earliest',
        readOnly: true
      }],
      renewQuery: true,

      requestId: 'basic'
    };
    await queryOrchestrator.fetchQuery(query);

    expect(streamingSourceMockDriver.downloadTableStreamOffset).toBe('earliest');
  });

  test('drop without touch does not affect tables in progress', async () => {
    const firstQuery = queryOrchestratorDropWithoutTouch.fetchQuery({
      query: 'SELECT * FROM stb_pre_aggregations.orders_delay_d20181102',
      values: [],
      cacheKeyQueries: {
        renewalThreshold: 21600,
        queries: []
      },
      preAggregations: [{
        preAggregationsSchema: 'stb_pre_aggregations',
        tableName: 'stb_pre_aggregations.orders_delay_d20181102',
        loadSql: ['CREATE TABLE stb_pre_aggregations.orders_d20181102 AS SELECT * FROM public.orders_delay', []],
        invalidateKeyQueries: [['SELECT 2', []]]
      }],
      requestId: 'drop without touch does not affect tables in progress'
    });
    const promises = [firstQuery];
    for (let i = 0; i < 10; i++) {
      promises.push(queryOrchestratorDropWithoutTouch.fetchQuery({
        query: `SELECT * FROM stb_pre_aggregations.orders_d201811${i}`,
        values: [],
        cacheKeyQueries: {
          renewalThreshold: 21600,
          queries: []
        },
        preAggregations: [{
          preAggregationsSchema: 'stb_pre_aggregations',
          tableName: `stb_pre_aggregations.orders_d201811${i}`,
          loadSql: [`CREATE TABLE stb_pre_aggregations.orders_d201811${i} AS SELECT * FROM public.orders`, []],
          invalidateKeyQueries: [['SELECT 2', []]]
        }],
        requestId: 'drop without touch does not affect tables in progress'
      }));
    }
    await Promise.all(promises);
    expect(mockDriver.tables).toContainEqual(expect.stringMatching(/orders_delay/));
  });

  test('streaming simple', async () => {
    const query = (id) => ({
      query: `SELECT * FROM stb_pre_aggregations.orders_d WHERE id = ${id}`,
      values: [],
      cacheKeyQueries: {
        queries: []
      },
      preAggregations: [],
      requestId: 'streaming simple',
      persistent: true,
      aliasNameToMember: {
        query: 'Foo.query'
      }
    });
    await Promise.all([
      queryOrchestrator.fetchQuery(query(1)),
      queryOrchestrator.fetchQuery(query(2)),
      queryOrchestrator.fetchQuery(query(3)),
      queryOrchestrator.fetchQuery(query(4)),
    ].map(async streamPromise => {
      const stream = await streamPromise;
      const data = await new Promise((resolve, reject) => {
        stream.on('data', (row) => {
          resolve(row);
        });
        stream.on('error', (err) => {
          reject(err);
        });
      });
      expect(data['Foo.query']).toMatch(/orders_d/);
    }));
  });

  test('streaming two nodes', async () => {
    const query = (id) => ({
      query: `SELECT * FROM stb_pre_aggregations.orders_d WHERE id = ${id}`,
      values: [],
      cacheKeyQueries: {
        queries: []
      },
      preAggregations: [],
      requestId: 'streaming simple',
      persistent: true,
      aliasNameToMember: {
        query: 'Foo.query'
      }
    });
    const fetchLongPolling = (orchestrator, q) => orchestrator.fetchQuery(q).catch(e => {
      console.log(e.toString());
      if (e.toString().match(/Continue wait/)) {
        return fetchLongPolling(orchestrator, q);
      }
      throw e;
    });
    await Promise.all([
      fetchLongPolling(queryOrchestrator, query(1)),
      fetchLongPolling(queryOrchestrator, query(2)),
      fetchLongPolling(queryOrchestrator2, query(3)),
      fetchLongPolling(queryOrchestrator2, query(4)),
    ].map(async streamPromise => {
      const stream = await streamPromise;
      const data = await new Promise((resolve, reject) => {
        stream.on('data', (row) => {
          resolve(row);
        });
        stream.on('error', (err) => {
          reject(err);
        });
      });
      expect(data['Foo.query']).toMatch(/orders_d/);
    }));
  });

  test('drop lock', async () => {
    mockDriver.tablesDropDelay = 300;
    for (let i = 0; i < 10; i++) {
      const promises = [];
      for (let j = 0; j < 10; j++) {
        // eslint-disable-next-line no-loop-func
        promises.push((async () => {
          await mockDriver.delay(100 * j);
          await queryOrchestratorDropWithoutTouch.fetchQuery({
            query: `SELECT * FROM stb_pre_aggregations.orders_d2018110${j}`,
            values: [],
            cacheKeyQueries: {
              renewalThreshold: 21600,
              queries: []
            },
            preAggregations: [{
              preAggregationsSchema: 'stb_pre_aggregations',
              tableName: `stb_pre_aggregations.orders_d2018110${j}`,
              loadSql: [`CREATE TABLE stb_pre_aggregations.orders_d2018110${j} AS SELECT * FROM public.orders_d`, []],
              invalidateKeyQueries: [['SELECT NOW()', [], {
                renewalThreshold: 0.001
              }]],
              external: true,
            }],
            requestId: `drop lock ${i}-${j}`
          });
        })());
      }

      await Promise.all(promises);

      await mockDriver.delay(200);
    }
    // expect(mockDriver.tables).toContainEqual(expect.stringMatching(/orders_delay/));
  });

  describe('Data Source Metadata Methods', () => {
    let metadataOrchestrator;
    let metadataMockDriver;

    beforeEach(() => {
      metadataMockDriver = new MockDriver();

      // Mock metadata methods
      metadataMockDriver.getSchemas = jest.fn().mockResolvedValue([
        { schema_name: 'public' },
        { schema_name: 'analytics' },
        { schema_name: 'staging' }
      ]);

      metadataMockDriver.getTablesForSpecificSchemas = jest.fn().mockImplementation((schemas) => {
        const tables = [];
        schemas.forEach(schema => {
          if (schema.schema_name === 'public') {
            tables.push(
              { schema_name: 'public', table_name: 'users' },
              { schema_name: 'public', table_name: 'orders' },
              { schema_name: 'public', table_name: 'products' }
            );
          } else if (schema.schema_name === 'analytics') {
            tables.push(
              { schema_name: 'analytics', table_name: 'user_metrics' },
              { schema_name: 'analytics', table_name: 'sales_summary' }
            );
          }
        });
        return Promise.resolve(tables);
      });

      metadataMockDriver.getColumnsForSpecificTables = jest.fn().mockImplementation((tables) => {
        const columns = [];
        tables.forEach(table => {
          if (table.table_name === 'users') {
            columns.push(
              {
                schema_name: 'public',
                table_name: 'users',
                column_name: 'id',
                data_type: 'integer',
                attributes: ['PRIMARY_KEY']
              },
              {
                schema_name: 'public',
                table_name: 'users',
                column_name: 'name',
                data_type: 'varchar',
                attributes: []
              },
              {
                schema_name: 'public',
                table_name: 'users',
                column_name: 'email',
                data_type: 'varchar',
                attributes: ['UNIQUE']
              }
            );
          } else if (table.table_name === 'orders') {
            columns.push(
              {
                schema_name: 'public',
                table_name: 'orders',
                column_name: 'id',
                data_type: 'integer',
                attributes: ['PRIMARY_KEY']
              },
              {
                schema_name: 'public',
                table_name: 'orders',
                column_name: 'user_id',
                data_type: 'integer',
                attributes: [],
                foreign_keys: [{ target_table: 'users', target_column: 'id' }]
              },
              {
                schema_name: 'public',
                table_name: 'orders',
                column_name: 'total',
                data_type: 'decimal',
                attributes: []
              }
            );
          }
        });
        return Promise.resolve(columns);
      });

      const driverFactory = () => metadataMockDriver;

      metadataOrchestrator = new QueryOrchestrator(
        'ORCHESTRATOR_TEST_METADATA',
        driverFactory,
        console.log,
        {
          cacheAndQueueDriver: 'memory',
          continueWaitTimeout: 5,
          queryCacheOptions: {
            queueOptions: () => ({
              concurrency: 2,
              processUid: 'metadata_test',
            }),
          },
          preAggregationsOptions: {
            queueOptions: () => ({
              concurrency: 2,
              processUid: 'metadata_test',
            }),
          },
        }
      );

      jest.clearAllMocks();

      if (metadataOrchestrator && metadataOrchestrator.queryCache && metadataOrchestrator.queryCache.memoryCache) {
        metadataOrchestrator.queryCache.memoryCache.clear();
      }

      if (metadataOrchestrator && metadataOrchestrator.queryCache && metadataOrchestrator.queryCache.getCacheDriver()) {
        const cacheDriver = metadataOrchestrator.queryCache.getCacheDriver();
        if (cacheDriver.store) {
          Object.keys(cacheDriver.store).forEach(key => delete cacheDriver.store[key]);
        }
      }
    });

    afterEach(async () => {
      await metadataOrchestrator.cleanup();
    });

    describe('queryDataSourceSchemas', () => {
      test('should query and cache schemas for default datasource', async () => {
        const result = await metadataOrchestrator.queryDataSourceSchemas();

        expect(result).toEqual([
          { schema_name: 'public' },
          { schema_name: 'analytics' },
          { schema_name: 'staging' }
        ]);
      });

      test('should query schemas for specific datasource', async () => {
        const result = await metadataOrchestrator.queryDataSourceSchemas('custom');

        expect(result).toEqual([
          { schema_name: 'public' },
          { schema_name: 'analytics' },
          { schema_name: 'staging' }
        ]);
      });

      test('should use cache on second call', async () => {
        // First call
        await metadataOrchestrator.queryDataSourceSchemas();
        // Second call should use cache
        const result = await metadataOrchestrator.queryDataSourceSchemas();

        expect(result).toEqual([
          { schema_name: 'public' },
          { schema_name: 'analytics' },
          { schema_name: 'staging' }
        ]);
      });

      test('should force refresh when requested', async () => {
        // First call
        await metadataOrchestrator.queryDataSourceSchemas();
        // Second call with forceRefresh
        const result = await metadataOrchestrator.queryDataSourceSchemas('default', { forceRefresh: true });

        expect(result).toEqual([
          { schema_name: 'public' },
          { schema_name: 'analytics' },
          { schema_name: 'staging' }
        ]);
      });

      test('should pass requestId option', async () => {
        const requestId = 'test-request-123';
        await metadataOrchestrator.queryDataSourceSchemas('default', { requestId });

        expect(metadataMockDriver.getSchemas).toHaveBeenCalledTimes(1);
      });
    });

    describe('queryTablesForSchemas', () => {
      test('should query tables for given schemas', async () => {
        const schemas = [
          { schema_name: 'public' },
          { schema_name: 'analytics' }
        ];

        const result = await metadataOrchestrator.queryTablesForSchemas(schemas);

        expect(result).toEqual([
          { schema_name: 'public', table_name: 'users' },
          { schema_name: 'public', table_name: 'orders' },
          { schema_name: 'public', table_name: 'products' },
          { schema_name: 'analytics', table_name: 'user_metrics' },
          { schema_name: 'analytics', table_name: 'sales_summary' }
        ]);
        expect(metadataMockDriver.getTablesForSpecificSchemas).toHaveBeenCalledWith(schemas);
      });

      test('should cache results based on schema list', async () => {
        const schemas = [{ schema_name: 'public' }];

        // First call - will execute and store in cache
        await metadataOrchestrator.queryTablesForSchemas(schemas);

        // Add a delay to ensure the first query has completed and cached its result
        await new Promise(resolve => setTimeout(resolve, 100));

        // Clear the mock calls
        metadataMockDriver.getTablesForSpecificSchemas.mockClear();

        // Create equivalent but different object instance
        // Our hash function should handle this correctly
        const schemas2 = [{ schema_name: 'public' }];

        // Second call should use cache
        const result = await metadataOrchestrator.queryTablesForSchemas(schemas2);

        expect(result).toEqual([
          { schema_name: 'public', table_name: 'users' },
          { schema_name: 'public', table_name: 'orders' },
          { schema_name: 'public', table_name: 'products' }
        ]);

        // Verify driver wasn't called again
        expect(metadataMockDriver.getTablesForSpecificSchemas).not.toHaveBeenCalled();
      });

      test('should handle empty schema list', async () => {
        const result = await metadataOrchestrator.queryTablesForSchemas([]);

        expect(result).toEqual([]);
        expect(metadataMockDriver.getTablesForSpecificSchemas).toHaveBeenCalledWith([]);
      });

      test('should force refresh when requested', async () => {
        const schemas = [{ schema_name: 'public' }];

        await metadataOrchestrator.queryTablesForSchemas(schemas);
        await metadataOrchestrator.queryTablesForSchemas(schemas, 'default', { forceRefresh: true });

        expect(metadataMockDriver.getTablesForSpecificSchemas).toHaveBeenCalledTimes(2);
      });
    });

    describe('queryColumnsForTables', () => {
      test('should query columns for given tables', async () => {
        const tables = [
          { schema_name: 'public', table_name: 'users' },
          { schema_name: 'public', table_name: 'orders' }
        ];

        const result = await metadataOrchestrator.queryColumnsForTables(tables);

        expect(result).toEqual([
          {
            schema_name: 'public',
            table_name: 'users',
            column_name: 'id',
            data_type: 'integer',
            attributes: ['PRIMARY_KEY']
          },
          {
            schema_name: 'public',
            table_name: 'users',
            column_name: 'name',
            data_type: 'varchar',
            attributes: []
          },
          {
            schema_name: 'public',
            table_name: 'users',
            column_name: 'email',
            data_type: 'varchar',
            attributes: ['UNIQUE']
          },
          {
            schema_name: 'public',
            table_name: 'orders',
            column_name: 'id',
            data_type: 'integer',
            attributes: ['PRIMARY_KEY']
          },
          {
            schema_name: 'public',
            table_name: 'orders',
            column_name: 'user_id',
            data_type: 'integer',
            attributes: [],
            foreign_keys: [{ target_table: 'users', target_column: 'id' }]
          },
          {
            schema_name: 'public',
            table_name: 'orders',
            column_name: 'total',
            data_type: 'decimal',
            attributes: []
          }
        ]);
        expect(metadataMockDriver.getColumnsForSpecificTables).toHaveBeenCalledWith(tables);
      });

      test('should cache results based on table list', async () => {
        const tables = [{ schema_name: 'public', table_name: 'users' }];

        // First call - will execute and store in cache
        await metadataOrchestrator.queryColumnsForTables(tables);

        // Add a delay to ensure the first query has completed and cached its result
        await new Promise(resolve => setTimeout(resolve, 100));

        // Clear the mock calls
        metadataMockDriver.getColumnsForSpecificTables.mockClear();

        // Create equivalent but different object instance
        // Our hash function should handle this correctly
        const tables2 = [{ schema_name: 'public', table_name: 'users' }];

        // Second call should use cache
        const result = await metadataOrchestrator.queryColumnsForTables(tables2);

        expect(result).toEqual([
          {
            schema_name: 'public',
            table_name: 'users',
            column_name: 'id',
            data_type: 'integer',
            attributes: ['PRIMARY_KEY']
          },
          {
            schema_name: 'public',
            table_name: 'users',
            column_name: 'name',
            data_type: 'varchar',
            attributes: []
          },
          {
            schema_name: 'public',
            table_name: 'users',
            column_name: 'email',
            data_type: 'varchar',
            attributes: ['UNIQUE']
          }
        ]);

        // Verify driver wasn't called again
        expect(metadataMockDriver.getColumnsForSpecificTables).not.toHaveBeenCalled();
      });

      test('should handle empty table list', async () => {
        const result = await metadataOrchestrator.queryColumnsForTables([]);

        expect(result).toEqual([]);
        expect(metadataMockDriver.getColumnsForSpecificTables).toHaveBeenCalledWith([]);
      });

      test('should force refresh when requested', async () => {
        const tables = [{ schema_name: 'public', table_name: 'users' }];

        await metadataOrchestrator.queryColumnsForTables(tables);
        await metadataOrchestrator.queryColumnsForTables(tables, 'default', { forceRefresh: true });

        expect(metadataMockDriver.getColumnsForSpecificTables).toHaveBeenCalledTimes(2);
      });
    });

    describe('Integration Tests', () => {
      test('should handle full metadata workflow', async () => {
        // Query schemas
        const schemas = await metadataOrchestrator.queryDataSourceSchemas();
        expect(schemas).toHaveLength(3);

        // Query tables for specific schemas
        const publicSchema = schemas.filter(s => s.schema_name === 'public');
        const tables = await metadataOrchestrator.queryTablesForSchemas(publicSchema);
        expect(tables).toHaveLength(3);

        // Query columns for specific tables
        const userTable = tables.filter(t => t.table_name === 'users');
        const columns = await metadataOrchestrator.queryColumnsForTables(userTable);
        expect(columns).toHaveLength(3);
        expect(columns[0].column_name).toBe('id');
        expect(columns[0].data_type).toBe('integer');
        expect(columns[0].attributes).toContain('PRIMARY_KEY');
      });

      test('should handle concurrent metadata requests', async () => {
        const schemas = [{ schema_name: 'public' }];

        // Make concurrent requests
        const promises = [
          metadataOrchestrator.queryDataSourceSchemas(),
          metadataOrchestrator.queryDataSourceSchemas(),
          metadataOrchestrator.queryTablesForSchemas(schemas),
          metadataOrchestrator.queryTablesForSchemas(schemas)
        ];

        const results = await Promise.all(promises);

        // All requests should return the same data
        expect(results[0]).toEqual(results[1]);
        expect(results[2]).toEqual(results[3]);
      });

      test('should handle error scenarios gracefully', async () => {
        // Mock driver error
        metadataMockDriver.getSchemas.mockRejectedValueOnce(new Error('Database connection failed'));

        await expect(metadataOrchestrator.queryDataSourceSchemas('default', { forceRefresh: true })).rejects.toThrow('Database connection failed');

        // Should retry on next call
        metadataMockDriver.getSchemas.mockResolvedValueOnce([{ schema_name: 'recovered' }]);
        const result = await metadataOrchestrator.queryDataSourceSchemas('default', { forceRefresh: true });
        expect(result).toEqual([{ schema_name: 'recovered' }]);
      });
    });
  });
});
