import {
  BlobServiceClient,
  StorageSharedKeyCredential,
  ContainerSASPermissions,
  SASProtocol,
  generateBlobSASQueryParameters,
} from '@azure/storage-blob';
import {
  DefaultAzureCredential,
  ClientSecretCredential,
} from '@azure/identity';

/**
 * @see {@link DefaultAzureCredential} constructor options
 */
export type AzureStorageClientConfig = {
  azureKey?: string,
  sasToken?: string,
  /**
   * The client ID of a Microsoft Entra app registration.
   * In case of DefaultAzureCredential flow if it is omitted
   * the Azure library will try to use the AZURE_CLIENT_ID env
   */
  clientId?: string,
  /**
   * ID of the application's Microsoft Entra tenant. Also called its directory ID.
   * In case of DefaultAzureCredential flow if it is omitted
   * the Azure library will try to use the AZURE_TENANT_ID env
   */
  tenantId?: string,
  /**
   * Azure service principal client secret.
   * Enables authentication to Microsoft Entra ID using a client secret that was generated
   * for an App Registration. More information on how to configure a client secret can be found here:
   * https://learn.microsoft.com/entra/identity-platform/quickstart-configure-app-access-web-apis#add-credentials-to-your-web-application
   * In case of DefaultAzureCredential flow if it is omitted
   * the Azure library will try to use the AZURE_CLIENT_SECRET env
   */
  clientSecret?: string,
  /**
   * The path to a file containing a Kubernetes service account token that authenticates the identity.
   * In case of DefaultAzureCredential flow if it is omitted
   * the Azure library will try to use the AZURE_FEDERATED_TOKEN_FILE env
   */
  tokenFilePath?: string,
};

export async function extractFilesFromAzure(
  azureConfig: AzureStorageClientConfig,
  bucketName: string,
  tableName: string
): Promise<string[]> {
  const splitter = bucketName.includes('blob.core') ? '.blob.core.windows.net/' : '.dfs.core.windows.net/';
  const parts = bucketName.split(splitter);
  const account = parts[0];
  const container = parts[1].split('/')[0];
  let credential: StorageSharedKeyCredential | ClientSecretCredential | DefaultAzureCredential;
  let blobServiceClient: BlobServiceClient;
  let getSas;

  if (azureConfig.azureKey) {
    credential = new StorageSharedKeyCredential(account, azureConfig.azureKey);
    getSas = async (name: string, startsOn: Date, expiresOn: Date) => generateBlobSASQueryParameters(
      {
        containerName: container,
        blobName: name,
        permissions: ContainerSASPermissions.parse('r'),
        startsOn,
        expiresOn,
        protocol: SASProtocol.Https,
        version: '2020-08-04',
      },
      credential as StorageSharedKeyCredential
    ).toString();
  } else if (azureConfig.clientSecret && azureConfig.tenantId && azureConfig.clientId) {
    credential = new ClientSecretCredential(
      azureConfig.tenantId,
      azureConfig.clientId,
      azureConfig.clientSecret,
    );
    getSas = async (name: string, startsOn: Date, expiresOn: Date) => {
      const userDelegationKey = await blobServiceClient.getUserDelegationKey(startsOn, expiresOn);
      return generateBlobSASQueryParameters(
        {
          containerName: container,
          blobName: name,
          permissions: ContainerSASPermissions.parse('r'),
          startsOn,
          expiresOn,
          protocol: SASProtocol.Https,
          version: '2020-08-04',
        },
        userDelegationKey,
        account
      ).toString();
    };
  } else {
    const opts = {
      tenantId: azureConfig.tenantId,
      clientId: azureConfig.clientId,
      tokenFilePath: azureConfig.tokenFilePath,
    };
    credential = new DefaultAzureCredential(opts);
    getSas = async (name: string, startsOn: Date, expiresOn: Date) => {
      // getUserDelegationKey works only for authorization with Microsoft Entra ID
      const userDelegationKey = await blobServiceClient.getUserDelegationKey(startsOn, expiresOn);
      return generateBlobSASQueryParameters(
        {
          containerName: container,
          blobName: name,
          permissions: ContainerSASPermissions.parse('r'),
          startsOn,
          expiresOn,
          protocol: SASProtocol.Https,
          version: '2020-08-04',
        },
        userDelegationKey,
        account,
      ).toString();
    };
  }

  const url = `https://${account}.blob.core.windows.net`;
  blobServiceClient = azureConfig.sasToken ?
    new BlobServiceClient(`${url}?${azureConfig.sasToken}`) :
    new BlobServiceClient(url, credential);

  const csvFiles: string[] = [];
  const containerClient = blobServiceClient.getContainerClient(container);
  const blobsList = containerClient.listBlobsFlat({ prefix: `${tableName}` });
  for await (const blob of blobsList) {
    if (blob.name && (blob.name.endsWith('.csv.gz') || blob.name.endsWith('.csv'))) {
      const starts = new Date();
      const expires = new Date(starts.valueOf() + 1000 * 60 * 60);
      const sas = await getSas(blob.name, starts, expires);
      csvFiles.push(`${url}/${container}/${blob.name}?${sas}`);
    }
  }

  if (csvFiles.length === 0) {
    throw new Error('No CSV files were obtained from the bucket');
  }

  return csvFiles;
}
