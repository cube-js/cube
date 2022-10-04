/* eslint-disable no-use-before-define */
// eslint-disable-next-line global-require
import { JDBCDriverConfiguration } from '@cubejs-backend/jdbc-driver';
import { DatabricksDriver, DatabricksDriverConfiguration } from '../src/DatabricksDriver';

describe('DatabricksDriver', () => {
  const { env } = process;
  beforeEach(() => {
    process.env = { ...env };
  });
  afterEach(() => {
    jest.clearAllMocks();
    
    process.env = env;
  });

  describe('query()', () => {
    it('success', async () => {
      const rows = [{}];
      const query = 'SELECT * FROM kek LIMIT 1';
      const driver = createDatabricksDriver([{ regexp: /SELECT/, rows }]);

      const result = await driver.query(query, []);

      expect(result).toEqual(rows);
    });

    it('success with db catalog', async () => {
      const searchSchema = 'dev_pre_aggregations';
      process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA = searchSchema;
      const dbCatalog = 'main';
      const queryWhichShouldBeReplacedInFlight = `SELECT 
      line_item__l_linestatus
      line_item__l_linestatus,
      line_item__l_shipmode
      line_item__l_shipmode
        FROM 
          ( SELECT * FROM ${searchSchema}.line_item__monthly_data19920101_rsizerlt_jsof0d5e_1hjjmcc UNION ALL
            SELECT * FROM ${searchSchema}.line_item__monthly_data19930101_thmoeqep_whxsyv4m_1hjjmcc UNION ALL 
            SELECT * FROM ${searchSchema}.line_item__monthly_data19940101_n0qtbyr1_bpz5ull3_1hjjgo6 UNION ALL
            SELECT * FROM ${searchSchema}.line_item__monthly_data19950101_mbr3tcsb_s1fdp505_1hjjgo6 UNION ALL
            SELECT * FROM ${searchSchema}.line_item__monthly_data19960101_fv3vvnag_kzmbadfs_1hjjgo6 UNION ALL 
            SELECT * FROM ${searchSchema}.line_item__monthly_data19970101_jbn34lxu_1fm3g1pm_1hjjgo6 UNION ALL
            SELECT * FROM ${searchSchema}.line_item__monthly_data19980101_xtjvt42f_epa5vpcl_1hjjgo6
          ) AS \`line_item___monthly_data\`  GROUP BY \`line_item__l_linestatus\`, \`line_item__l_shipmode\` ORDER BY 1 ASC LIMIT 10000`;
         
      // such queries shouldn't be replaced in flight by catalog feature
      // these two should be ignored because they don't contain required schema
      const ignoreQuery1 = 'SELECT * FROM random_table';
      const ignoreQuery2 = 'SELECT * FROM some_other_schema.line_item__monthly_data19980101_xtjvt42f_epa5vpcl';
      // we can ignore it because it already contains catalog
      const ignoreQuery3 = `SELECT * FROM ${dbCatalog}.${searchSchema}.line_item__monthly_data19980101_xtjvt42f`;
      // we should ignore it because it contains another unity catalog
      const ignoreQuery4 = 'SELECT * FROM tpch.random_schema.table';

      const driver = createDatabricksDriver(
        [
          {
            regexp: new RegExp(escapeCharacters(
              `SELECT * FROM ${dbCatalog}\\.${searchSchema}\\.line_item__monthly_data19920101_rsizerlt_jsof0d5e_1hjjmcc UNION ALL`
            )),
            rows: [{}]
          },
          {
            regexp: new RegExp(escapeCharacters(`^${ignoreQuery1}`)),
            rows: [{}, {}]
          },
          {
            regexp: new RegExp(escapeCharacters(`^${ignoreQuery2}`)),
            rows: [{}, {}, {}]
          },
          { regexp: new RegExp(escapeCharacters(`^${ignoreQuery3}`)), rows: [{}, {}, {}, {}] },
          { regexp: new RegExp(escapeCharacters(`${ignoreQuery4}`)), rows: [{}, {}, {}, {}, {}] }
        ],
        { dbCatalog }
      );

      const res1 = await driver.query(queryWhichShouldBeReplacedInFlight, []);
      const res2 = await driver.query(ignoreQuery1, []);
      const res3 = await driver.query(ignoreQuery2, []);
      const res4 = await driver.query(ignoreQuery3, []);
      const res5 = await driver.query(ignoreQuery4, []);

      expect(res1).toHaveLength(1);
      expect(res2).toHaveLength(2);
      expect(res3).toHaveLength(3);
      expect(res4).toHaveLength(4);
      expect(res5).toHaveLength(5);
    });
  });

  it('createSchemaIfNotExists() success', async () => {
    const schemaName = 'my_schema';
    const rows = ['ok'];
    const driver = createDatabricksDriver(
      [
        { regexp: new RegExp(`^CREATE SCHEMA IF NOT EXISTS \`${schemaName}\``), rows }
      ],
    );

    const result = await driver.createSchemaIfNotExists(schemaName);

    expect(result).toEqual(rows);
  });

  it('createSchemaIfNotExists() success with db catalog', async () => {
    const dbCatalog = 'main';
    const schemaName = 'my_schema';
    const rows = ['ok'];
    const driver = createDatabricksDriver(
      [
        { regexp: new RegExp(`^CREATE SCHEMA IF NOT EXISTS \`${dbCatalog}\`\\.\`${schemaName}\``), rows }
      ],
      { dbCatalog }
    );

    const result = await driver.createSchemaIfNotExists(schemaName);

    expect(result).toEqual(rows);
  });

  describe('loadPreAggregationIntoTable()', () => {
    it('success', async () => {
      const tableName = 'my_schema.my_super_table';
      const rows = ['ok'];

      const sql = 'CREATE TABLE my_schema.my_super_table AS (SELECT * from random_table)';
      const driver = createDatabricksDriver(
        [
          { regexp: /^CREATE TABLE my_schema\.my_super_table/, rows }
        ],
      );

      const result = await driver.loadPreAggregationIntoTable(tableName, sql, [], {});

      expect(result).toEqual(rows);
    });

    it('success with db catalog', async () => {
      const dbCatalog = 'main';
      const tableName = 'my_schema.my_super_table';
      const rows = ['ok'];

      const sql = 'CREATE TABLE my_schema.my_super_table AS (SELECT * from random_table)';
      const driver = createDatabricksDriver(
        [
          { regexp: /^CREATE TABLE main\.my_schema\.my_super_table/, rows }
        ],
        { dbCatalog }
      );

      const result = await driver.loadPreAggregationIntoTable(tableName, sql, [], {});

      expect(result).toEqual(rows);
    });
  });

  describe('tableColumnTypes()', () => {
    it('success', async () => {
      const tableName = 'my_schema.my_super_table';
      const rows = [{ col_name: 'id', data_type: 'decimal(10,0)' }];

      const driver = createDatabricksDriver(
        [
          { regexp: /^DESCRIBE `my_schema`\.`my_super_table`/, rows }
        ],
      );

      const result = await driver.tableColumnTypes(tableName);

      expect(result).toEqual([{ name: 'id', type: 'bigint' }]);
    });

    it('success with db catalog', async () => {
      const dbCatalog = 'main';
      const tableName = 'my_schema.my_super_table';
      const rows = [{ col_name: 'id', data_type: 'decimal(10,0)' }];

      const driver = createDatabricksDriver(
        [
          { regexp: /^DESCRIBE `main`\.`my_schema`\.`my_super_table`/, rows }
        ],
        { dbCatalog }
      );

      const result = await driver.tableColumnTypes(tableName);

      expect(result).toEqual([{ name: 'id', type: 'bigint' }]);
    });
  });

  describe('queryColumnTypes()', () => {
    it('success', async () => {
      const sql = 'SELECT * FROM my_schema.my_super_table';
      const rows = [{ col_name: 'id', data_type: 'decimal(10,0)' }];

      const driver = createDatabricksDriver(
        [
          { regexp: /^DESCRIBE QUERY SELECT \* FROM my_schema.my_super_table/, rows }
        ],
      );

      const result = await driver.queryColumnTypes(sql, []);

      expect(result).toEqual([{ name: 'id', type: 'bigint' }]);
    });

    it('success with db catalog', async () => {
      const dbCatalog = 'main';
      const sql = 'SELECT * FROM dev_pre_aggregations.my_super_table';
      const rows = [{ col_name: 'id', data_type: 'decimal(10,0)' }];

      const driver = createDatabricksDriver(
        [
          { regexp: /^DESCRIBE QUERY SELECT \* FROM main\.dev_pre_aggregations\.my_super_table/, rows }
        ],
        { dbCatalog }
      );

      const result = await driver.queryColumnTypes(sql, []);

      expect(result).toEqual([{ name: 'id', type: 'bigint' }]);
    });
  });

  describe('getTablesQuery()', () => {
    it('success', async () => {
      const schema = 'my_schema';
      const rows = [{ database: schema, tableName: 'my_table' }];

      const driver = createDatabricksDriver(
        [
          { regexp: /^SHOW TABLES IN `my_schema`/, rows }
        ],
      );

      const result = await driver.getTablesQuery(schema);

      expect(result).toEqual([{ table_name: 'my_table' }]);
    });
    it('success with db catalog', async () => {
      const dbCatalog = 'main';
      const schema = 'my_schema';
      const rows = [{ database: schema, tableName: 'my_table' }];

      const driver = createDatabricksDriver(
        [
          { regexp: /^SHOW TABLES IN `main`\.`my_schema`/, rows }
        ],
        { dbCatalog }
      );

      const result = await driver.getTablesQuery(schema);

      expect(result).toEqual([{ table_name: 'my_table' }]);
    });
  });

  describe('tablesSchema()', () => {
    it('success when config.database exist and config.dbCatalog exist', async () => {
      const dbCatalog = 'main';
      const database = 'my_schema';
      const rows = [{ database, tableName: 'my_table' }];

      const driver = createDatabricksDriver(
        [
          { regexp: /^SHOW TABLES IN `main`\.`my_schema`/, rows },
          { regexp: /^DESCRIBE `main`\.`my_schema`\.`my_table`/, rows: [{ col_name: 'id', data_type: 'decimal(10,0)' }] }
        ],
        { dbCatalog, database }
      );

      const result = await driver.tablesSchema();

      expect(result).toEqual({ [database]: { my_table: [{ name: 'id', type: 'bigint' }] } });
    });

    it('success when config.database exist and config.dbCatalog doesn\'t exist', async () => {
      const database = 'my_schema';
      const rows = [{ database, tableName: 'my_table' }];

      const driver = createDatabricksDriver(
        [
          { regexp: /^SHOW TABLES IN `my_schema`/, rows },
          { regexp: /^DESCRIBE `my_schema`\.`my_table`/, rows: [{ col_name: 'id', data_type: 'decimal(10,0)' }] }
        ],
        { database }
      );

      const result = await driver.tablesSchema();

      expect(result).toEqual({ [database]: { my_table: [{ name: 'id', type: 'bigint' }] } });
    });

    it('success when config.database doesn\'t exist and config.dbCatalog exist', async () => {
      const dbCatalog = 'main';
      const showDatabasesRows = [{ databaseName: 'default' }, { databaseName: 'dev_pre_aggregations' }];

      const driver = createDatabricksDriver(
        [
          { regexp: /^SHOW DATABASES IN `main`/, rows: showDatabasesRows },
          { regexp: /^SHOW TABLES IN `main`\.`default`/, rows: [{ database: 'default', tableName: 'table1' }, { database: 'default', tableName: 'table2' }] },
          { regexp: /^SHOW TABLES IN `main`\.`dev_pre_aggregations`/, rows: [{ database: 'dev_pre_aggregations', tableName: 'table3' }, { database: 'dev_pre_aggregations', tableName: 'table4' }] },
          { regexp: /^DESCRIBE `main`\.`default`\.`table1`/, rows: [{ col_name: 'id', data_type: 'decimal(10,0)' }] },
          { regexp: /^DESCRIBE `main`\.`default`\.`table2`/, rows: [{ col_name: 'comment', data_type: 'text' }] },
          { regexp: /^DESCRIBE `main`\.`dev_pre_aggregations`\.`table3`/, rows: [{ col_name: 'count', data_type: 'int' }] },
          { regexp: /^DESCRIBE `main`\.`dev_pre_aggregations`\.`table4`/, rows: [{ col_name: 'status', data_type: 'varchar' }] }
        ],
        { dbCatalog }
      );

      const result = await driver.tablesSchema();

      expect(result).toEqual({
        default: {
          table1: [{ name: 'id', type: 'bigint' }],
          table2: [{ name: 'comment', type: 'text' }]
        },
        dev_pre_aggregations: {
          table3: [{ name: 'count', type: 'int' }],
          table4: [{ name: 'status', type: 'text' }]
        }
      });
    });

    it('success when config.database doesn\'t exist and config.dbCatalog doesn\'t exist', async () => {
      const showDatabasesRows = [{ databaseName: 'default' }, { databaseName: 'dev_pre_aggregations' }];

      const driver = createDatabricksDriver(
        [
          { regexp: /^SHOW DATABASES$/, rows: showDatabasesRows },
          { regexp: /^SHOW TABLES IN `default`/, rows: [{ database: 'default', tableName: 'table1' }, { database: 'default', tableName: 'table2' }] },
          { regexp: /^SHOW TABLES IN `dev_pre_aggregations`/, rows: [{ database: 'dev_pre_aggregations', tableName: 'table3' }, { database: 'dev_pre_aggregations', tableName: 'table4' }] },
          { regexp: /^DESCRIBE `default`\.`table1`/, rows: [{ col_name: 'id', data_type: 'decimal(10,0)' }] },
          { regexp: /^DESCRIBE `default`\.`table2`/, rows: [{ col_name: 'comment', data_type: 'text' }] },
          { regexp: /^DESCRIBE `dev_pre_aggregations`\.`table3`/, rows: [{ col_name: 'count', data_type: 'int' }] },
          { regexp: /^DESCRIBE `dev_pre_aggregations`\.`table4`/, rows: [{ col_name: 'status', data_type: 'varchar' }] }
        ],
      );

      const result = await driver.tablesSchema();

      expect(result).toEqual({
        default: {
          table1: [{ name: 'id', type: 'bigint' }],
          table2: [{ name: 'comment', type: 'text' }]
        },
        dev_pre_aggregations: {
          table3: [{ name: 'count', type: 'int' }],
          table4: [{ name: 'status', type: 'text' }]
        }
      });
    });
  });

  describe('dropTable()', () => {
    it('success', async () => {
      const tableName = 'my_schema.my_table';

      const driver = createDatabricksDriver(
        [
          { regexp: /^DROP TABLE my_schema\.my_table/, rows: ['ok'] }
        ],
      );

      const result = await driver.dropTable(tableName);

      expect(result).toEqual(['ok']);
    });
    
    it('success with db catalog', async () => {
      const dbCatalog = 'main';
      const tableName = 'my_schema.my_table';

      const driver = createDatabricksDriver(
        [
          { regexp: /^DROP TABLE main\.my_schema\.my_table/, rows: ['ok'] }
        ],
        { dbCatalog }
      );

      const result = await driver.dropTable(tableName);

      expect(result).toEqual(['ok']);
    });
  });

  describe('unload()', () => {
    it('success', () => {
      
    });
  });
});

function escapeCharacters(val: string) {
  return val.replace('*', '\\*');
}

type Stub = {regexp: RegExp, rows: unknown[] | null};

function createDatabricksDriver(stubs: Stub[], databricksConfig: Partial<DatabricksDriverConfiguration> = {} as DatabricksDriverConfiguration) {
  const mockStatement = {
    cancel: (cb: any) => cb(null, {}),
    setQueryTimeout: (_smth: any, cb: any) => cb(null, {}),
    execute: (query: string, cb: any) => {
      for (const s of stubs) {
        if (s.regexp.test(query)) {
          // eslint-disable-next-line consistent-return
          return cb(null, s.rows);
        }
      }

      throw new Error(`unmatched query: ${query}`);
    }
  };
  const mockConnection = { createStatement: (cb: any) => cb(null, mockStatement) };
  const mockPool = {
    acquire: () => Promise.resolve(mockConnection),
    release: () => Promise.resolve({})
  };
  class MockDatabricksDriver extends DatabricksDriver {
    public constructor(config: Partial<DatabricksDriverConfiguration>) {
      super(config);
    }

    protected getPool(_dataSource: string, _config: JDBCDriverConfiguration) {
      return mockPool as any;
    }
  }

  const driver = new MockDatabricksDriver({ url: 'random_url', ...databricksConfig });

  return driver;
}
