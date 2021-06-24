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
  description: string = 'Must be number (in seconds) or string in time format (1s, 1m, 1h).',
) {
  if (/^\d+$/.test(input)) {
    return parseInt(input, 10);
  }

  if (input.length > 1) {
    // eslint-disable-next-line default-case
    switch (input.substr(-1).toLowerCase()) {
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

function asPortOrSocket(input: string, envName: string): number|string {
  if (/^-?\d+$/.test(input)) {
    return asPortNumber(parseInt(input, 10), envName);
  }

  // @todo Can we check that path for socket is valid?
  return input;
}

function asBoolOrTime(input: string, envName: string): number|boolean {
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
  refreshTimer: () => {
    if (process.env.CUBEJS_SCHEDULED_REFRESH_TIMER) {
      return asBoolOrTime(process.env.CUBEJS_SCHEDULED_REFRESH_TIMER, 'CUBEJS_SCHEDULED_REFRESH_TIMER');
    }

    // Refresh timer is true by default for development
    return process.env.NODE_ENV !== 'production';
  },
  scheduledRefresh: () => get('CUBEJS_SCHEDULED_REFRESH')
    .asBool(),
  gracefulShutdown: () => get('CUBEJS_GRACEFUL_SHUTDOWN')
    .asIntPositive(),
  dockerImageVersion: () => get('CUBEJS_DOCKER_IMAGE_VERSION')
    .asString(),
  // It's only excepted for CI, nothing else.
  internalExceptions: () => get('INTERNAL_EXCEPTIONS_YOU_WILL_BE_FIRED')
    .default('false')
    .asEnum(['exit', 'log', 'false']),
  preAggregationsSchema: () => get('CUBEJS_PRE_AGGREGATIONS_SCHEMA')
    .asString(),
  dbPollTimeout: () => {
    const value = process.env.CUBEJS_DB_POLL_TIMEOUT || '15m';
    return convertTimeStrToMs(value, 'CUBEJS_DB_POLL_TIMEOUT');
  },
  dbPollMaxInterval: () => {
    const value = process.env.CUBEJS_DB_POLL_MAX_INTERVAL || '5s';
    return convertTimeStrToMs(value, 'CUBEJS_DB_POLL_MAX_INTERVAL');
  },
  // Common db options
  dbName: ({ required }: { required?: boolean }) => get('CUBEJS_DB_NAME')
    .required(required)
    .asString(),
  // Export Bucket options
  dbExportBucketType: ({ supported }: { supported: ('s3' | 'gcp' | 'azure')[] }) => get('CUBEJS_DB_EXPORT_BUCKET_TYPE')
    .asEnum(supported),
  dbExportBucket: () => get('CUBEJS_DB_EXPORT_BUCKET')
    .asString(),
  // Export bucket options for AWS S3
  dbExportBucketAwsKey: () => get('CUBEJS_DB_EXPORT_BUCKET_AWS_KEY')
    .asString(),
  dbExportBucketAwsSecret: () => get('CUBEJS_DB_EXPORT_BUCKET_AWS_SECRET')
    .asString(),
  dbExportBucketAwsRegion: () => get('CUBEJS_DB_EXPORT_BUCKET_AWS_REGION')
    .asString(),
  // Export bucket options for Integration based
  dbExportIntegration: () => get('CUBEJS_DB_EXPORT_INTEGRATION')
    .asString(),
  // Export bucket options for GCS
  dbExportGCSCredentials: () => {
    const credentials = get('CUBEJS_DB_EXPORT_GCS_CREDENTIALS')
      .asString();
    if (credentials) {
      return JSON.parse(Buffer.from(credentials, 'base64').toString('utf8'));
    }

    return undefined;
  },
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
  // Databricks
  databrickUrl: () => get('CUBEJS_DB_DATABRICKS_URL')
    .required()
    .asString(),
  databrickAcceptPolicy: () => get('CUBEJS_DB_DATABRICKS_ACCEPT_POLICY')
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
  telemetry: () => get('CUBEJS_TELEMETRY')
    .default('true')
    .asBool(),
  // Experiments & Preview flags
  livePreview: () => get('CUBEJS_LIVE_PREVIEW')
    .default('false')
    .asBoolStrict(),
  externalDefault: () => get('CUBEJS_EXTERNAL_DEFAULT')
    .default('false')
    .asBoolStrict(),
  scheduledRefreshDefault: () => get('CUBEJS_SCHEDULED_REFRESH_DEFAULT')
    .default('false')
    .asBoolStrict(),
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
