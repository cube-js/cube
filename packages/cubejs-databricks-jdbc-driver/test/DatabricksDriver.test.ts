import { DatabricksDriver } from '../src/DatabricksDriver';
import { UnloadOptions } from '@cubejs-backend/base-driver';
import { ContainerClient, BlobServiceClient } from '@azure/storage-blob';

jest.mock('@azure/storage-blob', () => ({
  ...jest.requireActual('@azure/storage-blob'),
  generateBlobSASQueryParameters: jest.fn().mockReturnValue('test')
}));

jest.spyOn(ContainerClient.prototype, 'listBlobsFlat').mockImplementation(
  jest.fn().mockReturnValue([{name: 'product.csv/test.csv'}])
);
jest.spyOn(BlobServiceClient.prototype, 'getUserDelegationKey').mockImplementation(
  jest.fn().mockReturnValue('mockKey')
);

describe('DatabricksDriver', () => {
  const mockTableName = 'product';
  const mockSql = 'SELECT * FROM ' + mockTableName;
  const mockParams = [1]
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
    process.env.CUBEJS_DB_DATABRICKS_ACCEPT_POLICY='true';
    process.env.CUBEJS_DB_DATABRICKS_URL='jdbc:databricks://adb-123456789.10.azuredatabricks.net:443';
    process.env.CUBEJS_DB_EXPORT_BUCKET_TYPE='azure';
    process.env.CUBEJS_DB_EXPORT_BUCKET='wasbs://cube-export@mock.blob.core.windows.net';
    process.env.CUBEJS_DB_EXPORT_BUCKET_AZURE_KEY='azure-key';
    process.env.CUBEJS_DB_EXPORT_BUCKET_AZURE_TENANT_ID='azure-tenant-id';
    process.env.CUBEJS_DB_EXPORT_BUCKET_AZURE_CLIENT_ID='azure-client-id';
    process.env.CUBEJS_DB_EXPORT_BUCKET_AZURE_CLIENT_SECRET='azure-client-sceret'
    process.env.CUBEJS_DB_DATABRICKS_TOKEN='token';
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
    process.env.CUBEJS_DB_EXPORT_BUCKET_AZURE_KEY='';
    databricksDriver = new DatabricksDriver(); 
    databricksDriver['unloadWithSql'] = mockUnloadWithSql;

    const result = await databricksDriver.unload(mockTableName, mockOptions);
    expect(mockUnloadWithSql).toHaveBeenCalledWith(mockTableName, mockSql, mockParams);
    expect(result.csvFile).toBeTruthy();
  });
});
