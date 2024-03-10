import { PrestoDriver } from '../src/PrestoDriver';
import { DriverTests, smartStringTrim } from '@cubejs-backend/testing-shared';
import { S3 } from '@aws-sdk/client-s3';

const path = require('path');
const { DockerComposeEnvironment, Wait } = require('testcontainers');

class PrestoDriverTest extends DriverTests {
  protected unloadTableEntryName() {
    return 'test_orders_order_status';
  }

  protected ctasUnloadTableNameTransform(tableName: string) {
    return `memory.default.${tableName}`;
  }

  protected unloadOptions(tableName: string) {
    return { maxFileSize: 64, query: { sql: `SELECT * FROM ${this.ctasUnloadTableNameTransform(tableName)}` }};
  }

  protected getExpectedCsvRows() {
    // Presto uses \N for null values
    return smartStringTrim`
      orders__status,orders__amount
      \N,500
      new,300
      processed,400
    `;
  }
}

describe('PrestoHouseDriver', () => {
  jest.setTimeout(6 * 60 * 1000);

  let env: any;
  let config: any;

  const doWithDriver = async (callback: any) => {
    const driver = new PrestoDriver(config);

    await callback(driver);
  };

  const testWithDriver = async (callback: any) => {
    let testDriverConfig = config;
    testDriverConfig.catalog = "memory";
    testDriverConfig.schema = "default";
    testDriverConfig.unloadBucket = "datalake";
    testDriverConfig.unloadPrefix = "prefix";
    testDriverConfig.unloadCatalog = "hive";
    testDriverConfig.unloadSchema = "default";
    testDriverConfig.region = "us-east-1";
    testDriverConfig.s3Client = new S3({
      region: "us-east-1",
      credentials: {
        accessKeyId: "admin",
        secretAccessKey: "admin123",
      },
      endpoint: "http://localhost:9000/",
      forcePathStyle: true,
    });
    process.env.AWS_ACCESS_KEY_ID = "admin";
    process.env.AWS_SECRET_ACCESS_KEY = "admin123";
    const testDriver = new PrestoDriverTest(
      new PrestoDriver(testDriverConfig),
      {
        expectStringFields: false,
        csvNoHeader: true,
        wrapLoadQueryWithCtas: true,
        delimiter: '\x01',
      }
    );

    await callback(testDriver);
  };

  // eslint-disable-next-line consistent-return,func-names
  beforeAll(async () => {
    const authOpts = {
      basic_auth: {
        user: 'presto',
        password: ''
      }
    };

    if (process.env.TEST_PRESTO_HOST) {
      config = {
        host: process.env.TEST_PRESTO_HOST || 'localhost',
        port: process.env.TEST_PRESTO_PORT || '8080',
        catalog: process.env.TEST_PRESTO_CATALOG || 'tpch',
        schema: 'sf1',
        ...authOpts
      };

      return;
    }

    const dc = new DockerComposeEnvironment(
      path.resolve(path.dirname(__filename), '../../'),
      'docker-compose.yml'
    );

    env = await dc
      .withStartupTimeout(240 * 1000)
      .withWaitStrategy('coordinator', Wait.forHealthCheck())
      .up();

    config = {
      host: env.getContainer('coordinator').getHost(),
      port: env.getContainer('coordinator').getMappedPort(8080),
      catalog: 'tpch',
      schema: 'sf1',
      ...authOpts
    };
  });

  // eslint-disable-next-line consistent-return,func-names
  afterAll(async () => {
    if (env) {
      await env.down();
    }
  });

  it('should construct', async () => {
    await doWithDriver(() => {
      //
    });
  });

  // eslint-disable-next-line func-names
  it('should test connection', async () => {
    await doWithDriver(async (driver: any) => {
      await driver.testConnection();
    });
  });

  // eslint-disable-next-line func-names
  it('should test informationSchemaQuery', async () => {
    await doWithDriver(async (driver: any) => {
      const informationSchemaQuery=driver.informationSchemaQuery();
      expect(informationSchemaQuery).toContain("columns.table_schema = 'sf1'");
    });
  });

  it('should test query', async () => {
    await testWithDriver(async (driverTest: PrestoDriverTest) => {
      await driverTest.testQuery();
    })
  });

  it('should test unload', async () => {
    await testWithDriver(async (driverTest: PrestoDriverTest) => {
      await driverTest.testUnload();
    })
  });

  it('should test unload empty', async () => {
    await testWithDriver(async (driverTest: PrestoDriverTest) => {
      await driverTest.testUnloadEmpty();
    })
  });
});
