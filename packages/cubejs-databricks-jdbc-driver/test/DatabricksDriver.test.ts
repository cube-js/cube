import { DatabricksDriver, DatabricksDriverConfiguration } from '../src/DatabricksDriver';
import { UnloadOptions } from '@cubejs-backend/base-driver';
import { ContainerClient, BlobServiceClient } from '@azure/storage-blob';

jest.mock('@azure/storage-blob', () => ({
  ...jest.requireActual('@azure/storage-blob'),
  generateBlobSASQueryParameters: jest.fn().mockReturnValue('test')
}));

// Capture the config the S3 client is constructed with so we can assert that
// empty OIDC credentials are not forwarded (which would yield
// `AuthorizationHeaderMalformed`) — see CUB-3000.
const s3ClientConfigs: any[] = [];
jest.mock('@aws-sdk/client-s3', () => ({
  S3: jest.fn().mockImplementation((config: any) => {
    s3ClientConfigs.push(config);
    return {
      listObjectsV2: jest.fn().mockResolvedValue({ Contents: [{ Key: 'product/part-0.csv' }] }),
    };
  }),
  GetObjectCommand: jest.fn().mockImplementation((input: any) => ({ input })),
}));
jest.mock('@aws-sdk/s3-request-presigner', () => ({
  getSignedUrl: jest.fn().mockResolvedValue('https://signed.example/product/part-0.csv'),
}));

jest.spyOn(ContainerClient.prototype, 'listBlobsFlat').mockImplementation(
  jest.fn().mockReturnValue([{ name: 'product.csv/test.csv' }])
);
jest.spyOn(BlobServiceClient.prototype, 'getUserDelegationKey').mockImplementation(
  jest.fn().mockReturnValue('mockKey')
);

// `config` is protected; widen it rather than casting so the assertions stay type-checked.
class TestDatabricksDriver extends DatabricksDriver {
  public get testConfig(): DatabricksDriverConfiguration {
    return this.config;
  }
}

describe('DatabricksDriver', () => {
  const mockTableName = 'product';
  const mockSql = `SELECT * FROM ${mockTableName}`;
  const mockParams = [1];
  const mockOptions: UnloadOptions = {
    maxFileSize: 3,
    query: {
      sql: mockSql,
      params: mockParams,
    },
  };
  let databricksDriver: DatabricksDriver;
  const mockUnloadWithSql = jest.fn().mockResolvedValue('mockType');

  beforeAll(() => {
    process.env.CUBEJS_DB_DATABRICKS_ACCEPT_POLICY = 'true';
    process.env.CUBEJS_DB_DATABRICKS_URL = 'jdbc:databricks://adb-123456789.10.azuredatabricks.net:443;httpPath=/sql/1.0/warehouses/abc123def456';
    process.env.CUBEJS_DB_EXPORT_BUCKET_TYPE = 'azure';
    process.env.CUBEJS_DB_EXPORT_BUCKET = 'wasbs://cube-export@mock.blob.core.windows.net';
    process.env.CUBEJS_DB_EXPORT_BUCKET_AZURE_KEY = 'azure-key';
    process.env.CUBEJS_DB_EXPORT_BUCKET_AZURE_TENANT_ID = 'azure-tenant-id';
    process.env.CUBEJS_DB_EXPORT_BUCKET_AZURE_CLIENT_ID = 'azure-client-id';
    process.env.CUBEJS_DB_EXPORT_BUCKET_AZURE_CLIENT_SECRET = 'azure-client-sceret';
    process.env.CUBEJS_DB_DATABRICKS_TOKEN = 'token';
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  test('should get signed URLs of unloaded csv files by azure storage shared key', async () => {
    databricksDriver = new DatabricksDriver();
    databricksDriver['unloadWithSql'] = mockUnloadWithSql;

    const result = await databricksDriver.unload(mockTableName, mockOptions);
    expect(mockUnloadWithSql).toHaveBeenCalledWith(mockTableName, mockSql, mockParams);
    expect(result.csvFile).toBeTruthy();
  });

  test('should get signed URLs of unloaded csv files by azure client secret', async () => {
    process.env.CUBEJS_DB_EXPORT_BUCKET_AZURE_KEY = '';
    databricksDriver = new DatabricksDriver();
    databricksDriver['unloadWithSql'] = mockUnloadWithSql;

    const result = await databricksDriver.unload(mockTableName, mockOptions);
    expect(mockUnloadWithSql).toHaveBeenCalledWith(mockTableName, mockSql, mockParams);
    expect(result.csvFile).toBeTruthy();
  });

  describe('connection properties', () => {
    const baseUrl = 'jdbc:databricks://adb-123456789.10.azuredatabricks.net:443;httpPath=/sql/1.0/warehouses/abc123def456';

    afterEach(() => {
      process.env.CUBEJS_DB_DATABRICKS_URL = baseUrl;
    });

    test('pins geospatial support off', () => {
      expect(new TestDatabricksDriver().testConfig.properties.EnableGeoSpatialSupport).toBe('0');
    });

    // The driver merges URL params and properties into one map and throws on a duplicate key, so
    // a URL-supplied value has to be removed rather than left to collide with the pin.
    test.each([
      ['EnableGeoSpatialSupport=1'],
      ['enablegeospatialsupport=1'],
      ['EnableGeoSpatialSupport='],
      ['EnableGeoSpatialSupport=1;EnableGeoSpatialSupport=0'],
    ])('drops "%s" from the URL and keeps the pin', (param) => {
      process.env.CUBEJS_DB_DATABRICKS_URL = `${baseUrl};${param}`;

      const { url, properties } = new TestDatabricksDriver().testConfig;

      expect(url).not.toMatch(/geospatial/i);
      expect(properties.EnableGeoSpatialSupport).toBe('0');
    });

    test('leaves the rest of the URL intact', () => {
      process.env.CUBEJS_DB_DATABRICKS_URL = `${baseUrl};EnableGeoSpatialSupport=1;ConnCatalog=main`;

      expect(new TestDatabricksDriver().testConfig.url).toBe(`${baseUrl};ConnCatalog=main`);
    });
  });

  describe('s3 export bucket', () => {
    beforeEach(() => {
      s3ClientConfigs.length = 0;
      process.env.CUBEJS_DB_EXPORT_BUCKET_TYPE = 's3';
      process.env.CUBEJS_DB_EXPORT_BUCKET = 's3://cube-export';
      process.env.CUBEJS_DB_EXPORT_BUCKET_AWS_REGION = 'us-east-1';
    });

    afterAll(() => {
      // Restore the azure bucket config the other tests rely on.
      process.env.CUBEJS_DB_EXPORT_BUCKET_TYPE = 'azure';
      process.env.CUBEJS_DB_EXPORT_BUCKET = 'wasbs://cube-export@mock.blob.core.windows.net';
      delete process.env.CUBEJS_DB_EXPORT_BUCKET_AWS_KEY;
      delete process.env.CUBEJS_DB_EXPORT_BUCKET_AWS_SECRET;
      delete process.env.CUBEJS_DB_EXPORT_BUCKET_AWS_REGION;
    });

    // CUB-3000: OIDC / workload identity — no static keys configured.
    test('omits credentials when no static keys are set so the SDK uses the default chain', async () => {
      delete process.env.CUBEJS_DB_EXPORT_BUCKET_AWS_KEY;
      delete process.env.CUBEJS_DB_EXPORT_BUCKET_AWS_SECRET;

      databricksDriver = new DatabricksDriver();
      databricksDriver['unloadWithSql'] = mockUnloadWithSql;

      const result = await databricksDriver.unload(mockTableName, mockOptions);

      expect(result.csvFile).toBeTruthy();
      expect(s3ClientConfigs).toHaveLength(1);
      expect(s3ClientConfigs[0].credentials).toBeUndefined();
      expect(s3ClientConfigs[0].region).toBe('us-east-1');
    });

    test('passes static credentials through when configured', async () => {
      process.env.CUBEJS_DB_EXPORT_BUCKET_AWS_KEY = 'AKIAEXAMPLE';
      process.env.CUBEJS_DB_EXPORT_BUCKET_AWS_SECRET = 'secretexample';

      databricksDriver = new DatabricksDriver();
      databricksDriver['unloadWithSql'] = mockUnloadWithSql;

      const result = await databricksDriver.unload(mockTableName, mockOptions);

      expect(result.csvFile).toBeTruthy();
      expect(s3ClientConfigs).toHaveLength(1);
      expect(s3ClientConfigs[0].credentials).toEqual({
        accessKeyId: 'AKIAEXAMPLE',
        secretAccessKey: 'secretexample',
      });
    });
  });
});
