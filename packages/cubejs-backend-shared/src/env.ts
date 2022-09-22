/* eslint-disable no-restricted-syntax */
import { get } from 'env-var';
import { displayCLIWarning } from './cli';

export class InvalidConfiguration extends Error {
  public constructor(key: string, value: any, description: string) {
    super(`Value "${value}" is not valid for ${key}. ${description}`);
  }
}

export function convertTimeStrToMs(
  input: string,
  envName: string,
  description: string = 'Must be a number in seconds or duration string (1s, 1m, 1h).',
) {
  if (/^\d+$/.test(input)) {
    return parseInt(input, 10);
  }

  if (input.length > 1) {
    // eslint-disable-next-line default-case
    switch (input.slice(-1).toLowerCase()) {
      case 'h':
        return parseInt(input.slice(0, -1), 10) * 60 * 60;
      case 'm':
        return parseInt(input.slice(0, -1), 10) * 60;
      case 's':
        return parseInt(input.slice(0, -1), 10);
    }
  }

  throw new InvalidConfiguration(envName, input, description);
}

export function asPortNumber(input: number, envName: string) {
  if (input < 0) {
    throw new InvalidConfiguration(envName, input, 'Should be a positive integer.');
  }

  if (input > 65535) {
    throw new InvalidConfiguration(envName, input, 'Should be lower or equal than 65535.');
  }

  return input;
}

/**
 * Multiple data sources cache.
 */
let dataSourcesCache: string[];

/**
 * Determines whether multiple data sources were declared or not.
 */
function isMultipleDataSources(): boolean {
  // eslint-disable-next-line no-use-before-define
  dataSourcesCache = dataSourcesCache || getEnv('dataSources');
  return dataSourcesCache.length > 0;
}

/**
 * Returns the specified data source if assertions are passed, throws
 * an error otherwise.
 * @param dataSource The data source to assert.
 */
export function assertDataSource(dataSource = 'default'): string {
  if (!isMultipleDataSources()) {
    return dataSource;
  } else if (dataSourcesCache.indexOf(dataSource) >= 0) {
    return dataSource;
  } else {
    throw new Error(
      `The ${
        dataSource
      } data source is missing in the declared CUBEJS_DATASOURCES.`
    );
  }
}

/**
 * Returns data source specific environment variable name.
 * @param origin Origin environment variable name.
 * @param dataSource Data source name.
 */
function keyByDataSource(origin: string, dataSource: string): string {
  if (!isMultipleDataSources()) {
    return origin;
  } else {
    const s = origin.split('CUBEJS_');
    if (s.length === 2) {
      return `CUBEJS_DS_${dataSource.toUpperCase()}_${s[1]}`;
    } else {
      throw new Error(
        `The ${
          origin
        } environment variable can not be converted for the ${
          dataSource
        } data source.`
      );
    }
  }
}

function asPortOrSocket(input: string, envName: string): number | string {
  if (/^-?\d+$/.test(input)) {
    return asPortNumber(parseInt(input, 10), envName);
  }

  // @todo Can we check that path for socket is valid?
  return input;
}

function asFalseOrPort(input: string, envName: string): number | false {
  if (input.toLowerCase() === 'false' || input === '0' || input === undefined) {
    return false;
  }

  return asPortNumber(parseInt(input, 10), envName);
}

function asBoolOrTime(input: string, envName: string): number | boolean {
  if (input.toLowerCase() === 'true') {
    return true;
  }

  if (input.toLowerCase() === 'false' || input === '0') {
    return false;
  }

  return convertTimeStrToMs(
    input,
    envName,
    'Should be boolean or number (in seconds) or string in time format (1s, 1m, 1h)'
  );
}

let legacyRedisPasswordAlerted: boolean = false;
let legacyRedisUrlAlerted: boolean = false;
let legacyRedisTlsAlerted: boolean = false;

const variables: Record<string, (...args: any) => any> = {
  devMode: () => get('CUBEJS_DEV_MODE')
    .default('false')
    .asBoolStrict(),
  port: () => asPortOrSocket(process.env.PORT || '4000', 'PORT'),
  tls: () => get('CUBEJS_ENABLE_TLS')
    .default('false')
    .asBoolStrict(),
  webSockets: () => get('CUBEJS_WEB_SOCKETS')
    .default('false')
    .asBoolStrict(),
  rollupOnlyMode: () => get('CUBEJS_ROLLUP_ONLY')
    .default('false')
    .asBoolStrict(),
  refreshWorkerMode: () => {
    const refreshWorkerMode = get('CUBEJS_REFRESH_WORKER').asBool();
    if (refreshWorkerMode !== undefined) {
      return refreshWorkerMode;
    }

    // @deprecated Please use CUBEJS_REFRESH_WORKER
    const scheduledRefresh = get('CUBEJS_SCHEDULED_REFRESH').asBool();
    if (scheduledRefresh !== undefined) {
      return scheduledRefresh;
    }

    // @deprecated Please use CUBEJS_REFRESH_WORKER
    if (process.env.CUBEJS_SCHEDULED_REFRESH_TIMER) {
      return asBoolOrTime(process.env.CUBEJS_SCHEDULED_REFRESH_TIMER, 'CUBEJS_SCHEDULED_REFRESH_TIMER');
    }

    // It's true by default for development
    return process.env.NODE_ENV !== 'production';
  },
  preAggregationsBuilder: () => get('CUBEJS_PRE_AGGREGATIONS_BUILDER').asBool(),
  gracefulShutdown: () => get('CUBEJS_GRACEFUL_SHUTDOWN')
    .asIntPositive(),
  dockerImageVersion: () => get('CUBEJS_DOCKER_IMAGE_VERSION')
    .asString(),
  concurrency: () => get('CUBEJS_CONCURRENCY').asInt(),
  // It's only excepted for CI, nothing else.
  internalExceptions: () => get('INTERNAL_EXCEPTIONS_YOU_WILL_BE_FIRED')
    .default('false')
    .asEnum(['exit', 'log', 'false']),
  preAggregationsSchema: () => get('CUBEJS_PRE_AGGREGATIONS_SCHEMA')
    .asString(),
  dbPollTimeout: () => {
    const value = process.env.CUBEJS_DB_POLL_TIMEOUT;
    if (value) {
      return convertTimeStrToMs(value, 'CUBEJS_DB_POLL_TIMEOUT');
    } else {
      return null;
    }
  },
  dbQueryTimeout: () => {
    const value = process.env.CUBEJS_DB_QUERY_TIMEOUT || '10m';
    return convertTimeStrToMs(value, 'CUBEJS_DB_QUERY_TIMEOUT');
  },
  maxPartitionsPerCube: () => get('CUBEJS_MAX_PARTITIONS_PER_CUBE')
    .default('10000')
    .asInt(),

  /** ****************************************************************
   * Common db options                                               *
   ***************************************************************** */

  /**
   * Configured datasources.
   */
  dataSources: (): string[] => {
    const dataSources = get('CUBEJS_DATASOURCES').asString();
    if (dataSources) {
      return dataSources.trim().split(',');
    }
    return [];
  },

  /**
   * Driver type.
   */
  dbType: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    get(keyByDataSource('CUBEJS_DB_TYPE', dataSource)).asString()
  ),
  
  /**
   * Database (schema) name.
   */
  dbName: ({
    required,
    dataSource,
  }: {
    dataSource: string,
    required?: boolean,
  }) => (
    get(
      keyByDataSource('CUBEJS_DB_NAME', dataSource)
    ).required(required).asString()
  ),

  /**
   * Max polling interval. Currenly used in BigQuery and Databricks.
   * TODO: clarify this env.
   */
  dbPollMaxInterval: ({
    dataSource,
  }: {
    dataSource: string,
  }) => {
    const key = keyByDataSource('CUBEJS_DB_POLL_MAX_INTERVAL', dataSource);
    const value = get(key).asString() || '5s';
    return convertTimeStrToMs(value, key);
  },

  /** ****************************************************************
   * Export Bucket options                                           *
   ***************************************************************** */
  
  /**
   * Export bucket storage type.
   */
  dbExportBucketType: ({
    supported,
    dataSource,
  }: {
    supported: ('s3' | 'gcp' | 'azure')[],
    dataSource: string,
  }) => (
    get(
      keyByDataSource('CUBEJS_DB_EXPORT_BUCKET_TYPE', dataSource)
    ).asEnum(supported)
  ),

  /**
   * Export bucket storage URI.
   */
  dbExportBucket: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    get(
      keyByDataSource('CUBEJS_DB_EXPORT_BUCKET', dataSource)
    ).asString()
  ),

  /**
   * Mounted export bucket directory for the cases, when the storage
   * mounted to the datasource.
   */
  dbExportBucketMountDir: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    get(
      keyByDataSource('CUBEJS_DB_EXPORT_BUCKET_MOUNT_DIR', dataSource)
    ).asString()
  ),

  /**
   * AWS Key for the AWS based export bucket srorage.
   */
  dbExportBucketAwsKey: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    get(
      keyByDataSource('CUBEJS_DB_EXPORT_BUCKET_AWS_KEY', dataSource)
    ).asString()
  ),

  /**
   * AWS Secret for the AWS based export bucket srorage.
   */
  dbExportBucketAwsSecret: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    get(
      keyByDataSource('CUBEJS_DB_EXPORT_BUCKET_AWS_SECRET', dataSource)
    ).asString()
  ),

  /**
   * AWS Region for the AWS based export bucket srorage.
   */
  dbExportBucketAwsRegion: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    get(
      keyByDataSource('CUBEJS_DB_EXPORT_BUCKET_AWS_REGION', dataSource)
    ).asString()
  ),

  /**
   * Azure Key for the Azure based export bucket srorage.
   */
  dbExportBucketAzureKey: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    get(
      keyByDataSource('CUBEJS_DB_EXPORT_BUCKET_AZURE_KEY', dataSource)
    ).asString()
  ),

  /**
   * Export bucket options for Integration based.
   */
  dbExportIntegration: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    get(
      keyByDataSource('CUBEJS_DB_EXPORT_INTEGRATION', dataSource)
    ).asString()
  ),

  /**
   * Export bucket options for GCS.
   */
  dbExportGCSCredentials: ({
    dataSource,
  }: {
    dataSource: string,
  }) => {
    const credentials = get(
      keyByDataSource('CUBEJS_DB_EXPORT_GCS_CREDENTIALS', dataSource)
    ).asString();
    if (credentials) {
      return JSON.parse(
        Buffer.from(credentials, 'base64').toString('utf8')
      );
    }
    return undefined;
  },

  /** ****************************************************************
   * Databricks Driver                                               *
   ***************************************************************** */

  /**
   * Databricks jdbc-connection url.
   */
  databrickUrl: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    get(
      keyByDataSource('CUBEJS_DB_DATABRICKS_URL', dataSource)
    ).required().asString()
  ),

  /**
   * Databricks jdbc-connection token.
   */
  databrickToken: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    get(
      keyByDataSource('CUBEJS_DB_DATABRICKS_TOKEN', dataSource)
    ).asString()
  ),

  /**
   * Accept Databricks policy flag.
   */
  databrickAcceptPolicy: () => (
    get('CUBEJS_DB_DATABRICKS_ACCEPT_POLICY').asBoolStrict()
  ),

  /** ****************************************************************
   * Redshift Driver                                                 *
   ***************************************************************** */

  dbExportBucketRedshiftArn: () => get('CUBEJS_DB_EXPORT_BUCKET_REDSHIFT_ARN')
    .asString(),

  // BigQuery Driver
  bigQueryLocation: () => get('CUBEJS_DB_BQ_LOCATION')
    .asString(),

  // Cube Store
  cubeStoreHost: () => get('CUBEJS_CUBESTORE_HOST')
    .asString(),
  cubeStorePort: () => get('CUBEJS_CUBESTORE_PORT')
    .asPortNumber(),
  cubeStoreUser: () => get('CUBEJS_CUBESTORE_USER')
    .asString(),
  cubeStorePass: () => get('CUBEJS_CUBESTORE_PASS')
    .asString(),

  // Redis
  redisPoolMin: () => get('CUBEJS_REDIS_POOL_MIN')
    .default('2')
    .asInt(),
  redisPoolMax: () => get('CUBEJS_REDIS_POOL_MAX')
    .default('1000')
    .asInt(),
  redisUseIORedis: () => get('CUBEJS_REDIS_USE_IOREDIS')
    .default('false')
    .asBoolStrict(),
  allowUngroupedWithoutPrimaryKey: () => get('CUBEJS_ALLOW_UNGROUPED_WITHOUT_PRIMARY_KEY')
    .default('false')
    .asBoolStrict(),
  redisPassword: () => {
    const redisPassword = get('CUBEJS_REDIS_PASSWORD')
      .asString();
    if (redisPassword) {
      return redisPassword;
    }

    const legacyRedisPassword = get('REDIS_PASSWORD')
      .asString();
    if (legacyRedisPassword) {
      if (!legacyRedisPasswordAlerted) {
        displayCLIWarning('REDIS_PASSWORD is deprecated and will be removed, please use CUBEJS_REDIS_PASSWORD.');

        legacyRedisPasswordAlerted = true;
      }

      return legacyRedisPassword;
    }

    return undefined;
  },
  redisUrl: () => {
    const redisUrl = get('CUBEJS_REDIS_URL')
      .asString();
    if (redisUrl) {
      return redisUrl;
    }

    const legacyRedisUrl = get('REDIS_URL')
      .asString();
    if (legacyRedisUrl) {
      if (!legacyRedisUrlAlerted) {
        displayCLIWarning('REDIS_URL is deprecated and will be removed, please use CUBEJS_REDIS_URL.');

        legacyRedisUrlAlerted = true;
      }

      return legacyRedisUrl;
    }

    return undefined;
  },
  redisTls: () => {
    const redisTls = get('CUBEJS_REDIS_TLS')
      .asBoolStrict();
    if (redisTls) {
      return redisTls;
    }

    const legacyRedisTls = get('REDIS_TLS')
      .asBoolStrict();
    if (legacyRedisTls) {
      if (!legacyRedisTlsAlerted) {
        displayCLIWarning('REDIS_TLS is deprecated and will be removed, please use CUBEJS_REDIS_TLS.');

        legacyRedisTlsAlerted = true;
      }

      return legacyRedisTls;
    }

    return false;
  },
  dbSsl: () => get('CUBEJS_DB_SSL')
    .default('false')
    .asBoolStrict(),
  dbSslRejectUnauthorized: () => get('CUBEJS_DB_SSL_REJECT_UNAUTHORIZED')
    .default('false')
    .asBoolStrict(),
  nodeEnv: () => get('NODE_ENV')
    .asString(),
  cacheAndQueueDriver: () => get('CUBEJS_CACHE_AND_QUEUE_DRIVER')
    .asString(),
  jwkUrl: () => get('CUBEJS_JWK_URL')
    .asString(),
  jwtKey: () => get('CUBEJS_JWT_KEY')
    .asString(),
  jwtAlgorithms: () => get('CUBEJS_JWT_ALGS')
    .asArray(','),
  jwtAudience: () => get('CUBEJS_JWT_AUDIENCE')
    .asString(),
  jwtIssuer: () => get('CUBEJS_JWT_ISSUER')
    .asArray(','),
  jwtSubject: () => get('CUBEJS_JWT_SUBJECT')
    .asString(),
  jwtClaimsNamespace: () => get('CUBEJS_JWT_CLAIMS_NAMESPACE')
    .asString(),
  playgroundAuthSecret: () => get('CUBEJS_PLAYGROUND_AUTH_SECRET')
    .asString(),
  agentFrameSize: () => get('CUBEJS_AGENT_FRAME_SIZE')
    .default('200')
    .asInt(),
  agentEndpointUrl: () => get('CUBEJS_AGENT_ENDPOINT_URL')
    .asString(),
  agentFlushInterval: () => get('CUBEJS_AGENT_FLUSH_INTERVAL')
    .default(1000)
    .asInt(),
  agentMaxSockets: () => get('CUBEJS_AGENT_MAX_SOCKETS')
    .default(100)
    .asInt(),
  instanceId: () => get('CUBEJS_INSTANCE_ID')
    .asString(),
  telemetry: () => get('CUBEJS_TELEMETRY')
    .default('true')
    .asBool(),
  // SQL Interface
  sqlPort: () => {
    const port = asFalseOrPort(process.env.CUBEJS_SQL_PORT || 'false', 'CUBEJS_SQL_PORT');
    if (port) {
      return port;
    }

    return undefined;
  },
  pgSqlPort: () => {
    const port = asFalseOrPort(process.env.CUBEJS_PG_SQL_PORT || 'false', 'CUBEJS_PG_SQL_PORT');
    if (port) {
      return port;
    }

    return undefined;
  },
  sqlNonce: () => {
    if (process.env.CUBEJS_SQL_NONCE) {
      if (process.env.CUBEJS_SQL_NONCE.length < 14) {
        throw new InvalidConfiguration('CUBEJS_SQL_NONCE', process.env.CUBEJS_SQL_NONCE, 'Is too short. It should be 14 chars at least.');
      }

      return process.env.CUBEJS_SQL_NONCE;
    }

    return undefined;
  },
  sqlUser: () => get('CUBEJS_SQL_USER').asString(),
  sqlPassword: () => get('CUBEJS_SQL_PASSWORD').asString(),
  sqlSuperUser: () => get('CUBEJS_SQL_SUPER_USER').asString(),
  // Experiments & Preview flags
  livePreview: () => get('CUBEJS_LIVE_PREVIEW')
    .default('true')
    .asBoolStrict(),
  preAggregationsQueueEventsBus: () => get('CUBEJS_PRE_AGGREGATIONS_QUEUE_EVENTS_BUS')
    .default('false')
    .asBoolStrict(),
  externalDefault: () => get('CUBEJS_EXTERNAL_DEFAULT')
    .default('true')
    .asBoolStrict(),
  scheduledRefreshDefault: () => get(
    'CUBEJS_SCHEDULED_REFRESH_DEFAULT'
  ).default('true').asBoolStrict(),
  previewFeatures: () => get('CUBEJS_PREVIEW_FEATURES')
    .default('false')
    .asBoolStrict(),
  batchingRowSplitCount: () => get('CUBEJS_BATCHING_ROW_SPLIT_COUNT')
    .default(256 * 1024)
    .asInt(),
  maxSourceRowLimit: () => get('CUBEJS_MAX_SOURCE_ROW_LIMIT')
    .default(200000)
    .asInt()
};

type Vars = typeof variables;

export function getEnv<T extends keyof Vars>(key: T, opts?: Parameters<Vars[T]>): ReturnType<Vars[T]> {
  if (key in variables) {
    return variables[key](opts);
  }

  throw new Error(
    `Unsupported env variable: "${key}"`,
  );
}

export function isDockerImage(): boolean {
  return Boolean(process.env.CUBEJS_DOCKER_IMAGE_TAG);
}
