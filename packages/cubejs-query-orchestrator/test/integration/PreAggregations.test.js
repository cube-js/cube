/* globals describe, beforeAll, afterAll, beforeEach, test, expect */
const R = require('ramda');

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

  async downloadTable(table) {
    return { rows: await this.query(`SELECT * FROM ${table}`) };
  }

  async tableColumnTypes(table) {
    return [];
  }

  async uploadTable(table, columns, tableData) {
    await this.createTable(table, columns);
  }

  createTable(quotedTableName, columns) {
    this.tables.push(quotedTableName);
  }

  readOnly() {
    return false;
  }
}

describe('PreAggregations', () => {
  let mockDriver = null;
  let mockExternalDriver = null;
  let mockDriverFactory = null;
  let mockDriverReadOnlyFactory = null;
  let mockExternalDriverFactory = null;
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
  const basicQueryExternal = R.clone(basicQuery);
  basicQueryExternal.preAggregations[0].external = true;
  const basicQueryWithRenew = R.clone(basicQuery);
  basicQueryWithRenew.renewQuery = true;
  const basicQueryExternalWithRenew = R.clone(basicQueryExternal);
  basicQueryExternalWithRenew.renewQuery = true;

  beforeEach(() => {
    mockDriver = new MockDriver();
    mockExternalDriver = new MockDriver();
    mockDriverFactory = async () => mockDriver;
    mockDriverReadOnlyFactory = async () => {
      const driver = mockDriver;
      jest.spyOn(driver, 'readOnly').mockImplementation(() => true);
      return driver;
    }
    mockExternalDriverFactory = async () => {
      const driver = mockExternalDriver;
      driver.createTable("stb_pre_aggregations.orders_number_and_count20191101_kjypcoio_5yftl5il_1593709044209");
      return driver;
    }

    jest.resetModules();
    const QueryCache = require('../../orchestrator/QueryCache');
    queryCache = new QueryCache(
      "TEST",
      mockDriverFactory,
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
      const PreAggregations = require('../../orchestrator/PreAggregations');
      preAggregations = new PreAggregations(
        "TEST",
        mockDriverFactory,
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

  describe(`loadAllPreAggregationsIfNeeded with external rollup and writable source`, () => {
    let preAggregations = null;

    beforeEach(async () => {
      const PreAggregations = require('../../orchestrator/PreAggregations');
      preAggregations = new PreAggregations(
        "TEST",
        mockDriverFactory,
        (msg, params) => {},
        queryCache,
        {
          queueOptions: {
            executionTimeout: 1
          },
          externalDriverFactory: mockExternalDriverFactory,
        },
      );
    });

    test('refresh external preaggregation with a writable source (refreshImplTempTableExternalStrategy)', async () => {
      const result = await preAggregations.loadAllPreAggregationsIfNeeded(basicQueryExternal);
      expect(result[0][1].targetTableName).toMatch(/stb_pre_aggregations.orders_number_and_count20191101_kjypcoio_5yftl5il/);
    });
  });

  describe(`loadAllPreAggregationsIfNeeded with external rollup and readonly source`, () => {
    let preAggregations = null;

    beforeEach(async () => {
      const PreAggregations = require('../../orchestrator/PreAggregations');
      preAggregations = new PreAggregations(
        "TEST",
        mockDriverReadOnlyFactory,
        (msg, params) => {},
        queryCache,
        {
          queueOptions: {
            executionTimeout: 1
          },
          externalDriverFactory: mockExternalDriverFactory,
        },
      );
    });

    test('refresh external preaggregation with a writable source (refreshImplStreamExternalStrategy)', async () => {
      const result = await preAggregations.loadAllPreAggregationsIfNeeded(basicQueryExternal);
      expect(result[0][1].targetTableName).toMatch(/stb_pre_aggregations.orders_number_and_count20191101_kjypcoio_5yftl5il/);
    });
  });

  describe(`loadAllPreAggregationsIfNeeded with externalRefresh true`, () => {
    let preAggregations = null;

    beforeEach(async () => {
      const PreAggregations = require('../../orchestrator/PreAggregations');
      preAggregations = new PreAggregations(
        "TEST",
        mockDriverFactory,
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

  describe(`loadAllPreAggregationsIfNeeded with external rollup and externalRefresh true`, () => {
    let preAggregations = null;

    beforeEach(async () => {
      const PreAggregations = require('../../orchestrator/PreAggregations');
      preAggregations = new PreAggregations(
        "TEST",
        () => { throw new Error('The source database factory should never be called when externalRefresh is true, as it will trigger testConnection'); },
        (msg, params) => {},
        queryCache,
        {
          queueOptions: {
            executionTimeout: 1
          },
          externalDriverFactory: mockExternalDriverFactory,
          externalRefresh: true,
        },
      );
    });

    test('fail if waitForRenew is also specified', async () => {
      await expect(preAggregations.loadAllPreAggregationsIfNeeded(basicQueryExternalWithRenew))
        .rejects.toThrowError(/Invalid configuration/);
    });

    test('load external preaggregation without communicating to the source database', async () => {
      const result = await preAggregations.loadAllPreAggregationsIfNeeded(basicQueryExternal);
      expect(result[0][1].targetTableName).toMatch(/stb_pre_aggregations.orders_number_and_count20191101_kjypcoio_5yftl5il/);
    });
  });

  describe(`naming_version tests`, () => {
    let preAggregations = null;
    let PreAggregations = null;

    beforeEach(async () => {
      PreAggregations = require('../orchestrator/PreAggregations');
      preAggregations = new PreAggregations(
        "TEST",
        mockDriverFactory,
        (msg, params) => {},
        queryCache,
        {
          queueOptions: {
            executionTimeout: 1
          },
          externalDriverFactory: async () => {
            const driver = mockExternalDriver;
            driver.createTable("stb_pre_aggregations.orders_number_and_count20191101_kjypcoio_5yftl5il_1593709044209");
            driver.createTable("stb_pre_aggregations.orders_number_and_count20191101_kjypcoio_5yftl5il_1fm6652");
            return driver;
          },
        },
      );
    });

    test('test for function targetTableName', () => {
      let result = PreAggregations.targetTableName({
        table_name:'orders_number_and_count20191101',
        content_version:'kjypcoio',
        structure_version:'5yftl5il',
        last_updated_at:1600329890789, 
      });
      expect(result).toEqual('orders_number_and_count20191101_kjypcoio_5yftl5il_1600329890789')

      result = PreAggregations.targetTableName({
        table_name:'orders_number_and_count20191101',
        content_version:'kjypcoio',
        structure_version:'5yftl5il',
        last_updated_at:1600329890789, 
        naming_version:2
      });
      expect(result).toEqual('orders_number_and_count20191101_kjypcoio_5yftl5il_1fm6652')
    }); 

    test('naming_version and sort by last_updated_at', async () => {
      const result = await preAggregations.loadAllPreAggregationsIfNeeded(basicQueryExternal); 
      expect(result[0][1].targetTableName).toMatch(/stb_pre_aggregations.orders_number_and_count20191101_kjypcoio_5yftl5il_1fm6652/); 
    }); 
  });

  describe(`naming_version sort tests`, () => {
    let preAggregations = null;
    let PreAggregations = null;

    beforeEach(async () => {
      PreAggregations = require('../orchestrator/PreAggregations');
      preAggregations = new PreAggregations(
        "TEST",
        mockDriverFactory,
        (msg, params) => {},
        queryCache,
        {
          queueOptions: {
            executionTimeout: 1
          },
          externalDriverFactory: async () => {
            const driver = mockExternalDriver;
            driver.createTable("stb_pre_aggregations.orders_number_and_count20191101_kjypcoio_5yftl5il_1893709044209");
            driver.createTable("stb_pre_aggregations.orders_number_and_count20191101_kjypcoio_5yftl5il_1fm6652");
            return driver;
          },
        },
      );
    });

    test('naming_version and sort by last_updated_at', async () => {
      const result = await preAggregations.loadAllPreAggregationsIfNeeded(basicQueryExternal); 
      expect(result[0][1].targetTableName).toMatch(/stb_pre_aggregations.orders_number_and_count20191101_kjypcoio_5yftl5il_1893709044209/); 
    }); 
  });

});
