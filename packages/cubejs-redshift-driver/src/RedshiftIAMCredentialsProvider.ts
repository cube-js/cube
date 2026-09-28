import { RedshiftClient, GetClusterCredentialsWithIAMCommand } from '@aws-sdk/client-redshift';
import { fromTemporaryCredentials } from '@aws-sdk/credential-providers';
import { RedshiftCredentialsProvider } from './RedshiftCredentialsProvider';

export interface RedshiftIAMCredentialProviderOptions {
  region?: string;
  assumeRoleArn?: string;
  assumeRoleExternalId?: string;
  clusterIdentifier: string;
  dbName?: string;
}

interface CachedCredentials {
  user: string;
  password: string;
  expiration: Date;
}

// Refresh 1m before expiry
const REFRESH_BUFFER_MS = 60 * 1000;

export class RedshiftIAMCredentialsProvider implements RedshiftCredentialsProvider {
  protected readonly region: string;

  protected readonly clusterIdentifier: string;

  protected readonly dbName: string;

  protected readonly awsCredentials?: ReturnType<typeof fromTemporaryCredentials>;

  protected cached: CachedCredentials | null = null;

  protected inflightRefresh: Promise<CachedCredentials> | null = null;

  public constructor(options: RedshiftIAMCredentialProviderOptions) {
    if (!options.region) {
      throw new Error('CUBEJS_DB_REDSHIFT_AWS_REGION is required for IAM authentication');
    }

    if (!options.clusterIdentifier) {
      throw new Error(
        'CUBEJS_DB_REDSHIFT_CLUSTER_IDENTIFIER is required for IAM authentication'
      );
    }

    if (!options.dbName) {
      throw new Error(
        'CUBEJS_DB_NAME is required for IAM authentication'
      );
    }

    this.region = options.region;
    this.clusterIdentifier = options.clusterIdentifier;
    this.dbName = options.dbName;

    if (options.assumeRoleArn) {
      this.awsCredentials = fromTemporaryCredentials({
        params: {
          RoleArn: options.assumeRoleArn,
          ...(options.assumeRoleExternalId && { ExternalId: options.assumeRoleExternalId }),
        },
      });
    }
  }

  public getDbName(): string {
    return this.dbName;
  }

  public async getCredentials(): Promise<{ user: string; password: string }> {
    return this.resolveCredentials();
  }

  protected isExpired(): boolean {
    if (!this.cached) {
      return true;
    }

    return (this.cached.expiration.getTime() - Date.now()) < REFRESH_BUFFER_MS;
  }

  protected async resolveCredentials(): Promise<CachedCredentials> {
    if (this.cached && !this.isExpired()) {
      return this.cached;
    }

    // Concurrency guard
    if (this.inflightRefresh) {
      return this.inflightRefresh;
    }

    this.inflightRefresh = this.refreshCredentials().finally(() => {
      this.inflightRefresh = null;
    });

    return this.inflightRefresh;
  }

  protected async refreshCredentials(): Promise<CachedCredentials> {
    const client = new RedshiftClient({
      region: this.region,
      ...(this.awsCredentials && { credentials: this.awsCredentials }),
    });

    const command = new GetClusterCredentialsWithIAMCommand({
      ClusterIdentifier: this.clusterIdentifier,
      DbName: this.dbName,
      // By default, it's 15m, 1h is a maximum time
      DurationSeconds: 1800
    });

    const response = await client.send(command);

    if (!response.DbUser || !response.DbPassword || !response.Expiration) {
      throw new Error('GetClusterCredentialsWithIAM returned incomplete response');
    }

    this.cached = {
      user: response.DbUser,
      password: response.DbPassword,
      expiration: response.Expiration,
    };

    return this.cached;
  }
}
