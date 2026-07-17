import { jest, expect, beforeAll, afterAll } from '@jest/globals';
import {
  BaseDriver,
  DownloadTableMemoryData,
  StreamTableDataWithTypes,
} from '@cubejs-backend/base-driver';
import { Environment } from '../types/Environment';
import {
  getFixtures,
  getCreateQueries,
  getSelectQueries,
  getRefreshQueries,
  getDriver,
  runEnvironment,
} from '../helpers';

const anyOrNull: any = {
  $$typeof: Symbol.for('jest.asymmetricMatcher'),
  asymmetricMatch: () => true,
  toAsymmetricMatcher: () => 'AnyOrNull',
};

export function testConnection(type: string): void {
  describe(`Raw @cubejs-backend/${type}-driver`, () => {
    jest.setTimeout(60 * 5 * 1000);

    const fixtures = getFixtures(type);
    // Pinot cannot be seeded via SQL DDL; runEnvironment ingests fixed-name
    // `<table>_pinot` tables via the controller. Every other driver uses the
    // per-suite 'driver' suffix. The SQL create/stream/drop cases are skipped for
    // Pinot through fixtures.skip (see fixtures/pinot.json).
    const suffix = type === 'pinot' ? 'pinot' : 'driver';
    let driver: BaseDriver & {
      stream?: (
        query: string,
        values: string[],
        options: { highWaterMark: number },
      ) => Promise<any>
    };
    let query: string[];
    let env: Environment;

    function execute(name: string, test: () => Promise<void>) {
      if (fixtures.skip && fixtures.skip.indexOf(name) >= 0) {
        it.skip(name, test);
      } else {
        it(name, test);
      }
    }

    beforeAll(async () => {
      env = await runEnvironment(type, suffix);
      if (env.data) {
        process.env.CUBEJS_DB_HOST = '127.0.0.1';
        process.env.CUBEJS_DB_PORT = `${env.data.port}`;
      }
      driver = (await getDriver(type)).source;
    });

    afterAll(async () => {
      await driver.release();
      await env.stop();
    });

    execute('must establish a connection', async () => {
      await driver.testConnection();
    });

    execute('must creates a data source', async () => {
      query = getCreateQueries(type, suffix);
      await Promise.all(query.map(async (q) => {
        await driver.query(q);
      }));
      // CrateDB is eventually consistent: make the freshly loaded rows visible to
      // the following SELECTs before they run.
      if (type === 'crate') {
        await Promise.all(getRefreshQueries(type, suffix).map((q) => driver.query(q)));
      }
    });

    execute('must select from the data source', async () => {
      query = getSelectQueries(type, suffix);
      const response = await Promise.all(
        query.map(async (q) => {
          const res = await driver.query(q);
          return res;
        })
      );
      expect(response.length).toBe(5);

      response[0].forEach((item: any) => {
        const i: any = {};
        Object.keys(item).forEach((key) => {
          i[key.toLowerCase()] = item[key];
        });
        expect(i).toMatchSnapshot({
          category: expect.any(String),
          product_name: expect.any(String),
          sub_category: expect.any(String),
        });
      });
      expect(response[0].length).toBe(28);

      response[1].forEach((item: any) => {
        const i: any = {};
        Object.keys(item).forEach((key) => {
          i[key.toLowerCase()] = item[key];
        });
        expect(i).toMatchSnapshot({
          customer_id: expect.any(String),
          customer_name: expect.any(String),
        });
      });
      expect(response[1].length).toBe(41);

      response[2].forEach((item: any) => {
        const i: any = {};
        Object.keys(item).forEach((key) => {
          i[key.toLowerCase()] = item[key];
        });
        expect(i).toMatchSnapshot({
          row_id: expect.anything(), // can be String or Number
          order_id: expect.any(String),
          order_date: expect.anything(), // can be String or Date
          customer_id: expect.any(String),
          city: expect.any(String),
          category: expect.any(String),
          sub_category: expect.any(String),
          product_name: expect.any(String),
          sales: expect.anything(), // can be String or Number
          quantity: expect.anything(), // can be String or Number
          discount: expect.anything(), // can be String or Number
          profit: expect.anything(), // can be String or Number
        });
      });
      expect(response[2].length).toBe(44);

      response[3].forEach((item: any) => {
        const i: any = {};
        Object.keys(item).forEach((key) => {
          i[key.toLowerCase()] = item[key];
        });
        expect(i).toMatchSnapshot({
          id: expect.anything(), // can be String or Number
          row_id: expect.anything(), // can be String or Number
          order_id: expect.any(String),
          order_date: expect.anything(), // can be String or Date
          completed_date: expect.anything(), // can be String or Date
          customer_id: expect.any(String),
          city: expect.any(String),
          category: expect.any(String),
          sub_category: expect.any(String),
          product_name: expect.any(String),
          sales: anyOrNull, // String or Number, NULL for some rows
          quantity: expect.anything(), // can be String or Number
          discount: expect.anything(), // can be String or Number
          profit: expect.anything(), // can be String or Number
          is_returning: anyOrNull, // Boolean or Number, NULL for some rows
        });
      });
      expect(response[3].length).toBe(44);

      response[4].forEach((item: any) => {
        const i: any = {};
        Object.keys(item).forEach((key) => {
          i[key.toLowerCase()] = item[key];
        });
        expect(i).toMatchSnapshot({
          date_val: expect.anything(), // can be String or Date
          retail_year_name: expect.any(String),
          retail_quarter_name: expect.any(String),
          retail_month_name: expect.any(String),
          retail_week_name: expect.any(String),
          retail_year_begin_date: expect.anything(), // can be String or Date
          retail_quarter_begin_date: expect.anything(), // can be String or Date
          retail_month_begin_date: expect.anything(), // can be String or Date
          retail_week_begin_date: expect.anything(), // can be String or Date
          retail_date_prev_month: anyOrNull, // String or Date, NULL at boundaries
          retail_date_prev_quarter: anyOrNull, // String or Date, NULL at boundaries
          retail_date_prev_year: anyOrNull, // String or Date, NULL at boundaries
        });
      });
      expect(response[4].length).toBe(456);
    });

    execute('must download query from the data source via memory', async () => {
      query = getSelectQueries(type, suffix);
      expect(driver.downloadQueryResults).toBeDefined();

      const response = await Promise.all(
        query.map(async (q) => {
          const memory = <DownloadTableMemoryData>(
            await driver.downloadQueryResults(q, [], {
              streamImport: false,
              highWaterMark: 100,
            })
          );
          return {
            types: memory.types,
            data: memory.rows,
          };
        })
      );

      expect(response.length).toBe(5);
      expect(response[0].data.length).toBe(28);
      expect(response[1].data.length).toBe(41);
      expect(response[2].data.length).toBe(44);
      expect(response[3].data.length).toBe(44);
      expect(response[4].data.length).toBe(456);
    });

    execute('must download query from the data source via stream', async () => {
      query = getSelectQueries(type, suffix);
      expect(driver.downloadQueryResults).toBeDefined();

      const response = await Promise.all(
        query.map(async (q) => {
          const stream = <StreamTableDataWithTypes>(
            await driver.downloadQueryResults(q, [], {
              streamImport: true,
              highWaterMark: 16000,
            })
          );
          const { types } = stream;
          const data: unknown[] = [];
          await new Promise((resolve) => {
            const { rowStream } = stream;
            rowStream.on('data', (row: unknown) => {
              data.push(row);
            });
            rowStream.on('end', () => {
              if (stream.release) {
                stream.release();
              }
              resolve({ types, data });
            });
          });
          return { types, data };
        })
      );

      expect(response.length).toBe(5);
      expect(response[0].data.length).toBe(28);
      expect(response[1].data.length).toBe(41);
      expect(response[2].data.length).toBe(44);
      expect(response[3].data.length).toBe(44);
      expect(response[4].data.length).toBe(456);
    });

    execute('must delete the data source', async () => {
      const tables = Object
        .keys(fixtures.tables)
        .map((key: string) => `${fixtures.tables[key]}_${suffix}`);
      await Promise.all(
        tables.map(async (t) => {
          await driver.dropTable(t);
        })
      );
    });
  });
}
