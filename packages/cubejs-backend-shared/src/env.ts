/* eslint-disable no-restricted-syntax */
import { get } from 'env-var';
import { displayCLIWarning } from './cli';
import { isNativeSupported } from './platform';

export class InvalidConfiguration extends Error {
  public constructor(key: string, value: any, description: string) {
    super(`Value "${value}" is not valid for ${key}. ${description}`);
  }
}

export function convertTimeStrToSeconds(
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
 * Determines whether multiple data sources were declared or not.
 */
function isMultipleDataSources(): boolean {
  // eslint-disable-next-line no-use-before-define
  return getEnv('dataSources').length > 0;
}

/**
 * Returns the specified data source if assertions are passed, throws
 * an error otherwise.
 * @param dataSource The data source to assert.
 */
export function assertDataSource(dataSource = 'default'): string {
  if (!isMultipleDataSources()) {
    return dataSource;
    // eslint-disable-next-line no-use-before-define
  } else if (getEnv('dataSources').indexOf(dataSource) >= 0) {
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
export function keyByDataSource(origin: string, dataSource?: string): string {
  if (dataSource) assertDataSource(dataSource);
  if (!isMultipleDataSources() || dataSource === 'default') {
    return origin;
  } else if (!dataSource) {
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

  return convertTimeStrToSeconds(
    input,
    envName,
    'Should be boolean or number (in seconds) or string in time format (1s, 1m, 1h)'
  );
}

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
  serverHeadersTimeout: () => get('CUBEJS_SERVER_HEADERS_TIMEOUT')
    .asInt(),
  serverKeepAliveTimeout: () => get('CUBEJS_SERVER_KEEP_ALIVE_TIMEOUT')
    .asInt(),
  rollupOnlyMode: () => get('CUBEJS_ROLLUP_ONLY')
    .default('false')
    .asBoolStrict(),
  schemaPath: () => get('CUBEJS_SCHEMA_PATH')
    .default('model')
    .asString(),
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
  scheduledRefreshQueriesPerAppId: () => {
    const refreshQueries = get('CUBEJS_SCHEDULED_REFRESH_QUERIES_PER_APP_ID').asIntPositive();

    if (refreshQueries) {
      return refreshQueries;
    }

    const refreshConcurrency = get('CUBEJS_SCHEDULED_REFRESH_CONCURRENCY').asIntPositive();

    if (refreshConcurrency) {
      console.warn(
        'The CUBEJS_SCHEDULED_REFRESH_CONCURRENCY is deprecated. Please, use the CUBEJS_SCHEDULED_REFRESH_QUERIES_PER_APP_ID instead.'
      );
    }

    return refreshConcurrency;
  },
  refreshWorkerConcurrency: () => get('CUBEJS_REFRESH_WORKER_CONCURRENCY')
    .asIntPositive(),
  // eslint-disable-next-line consistent-return
  scheduledRefreshTimezones: () => {
    const timezones = get('CUBEJS_SCHEDULED_REFRESH_TIMEZONES').asString();

    if (timezones) {
      return timezones.split(',').map(t => t.trim());
    }
  },
  preAggregationsBuilder: () => get('CUBEJS_PRE_AGGREGATIONS_BUILDER').asBool(),
  gracefulShutdown: () => get('CUBEJS_GRACEFUL_SHUTDOWN')
    .asIntPositive(),
  dockerImageVersion: () => get('CUBEJS_DOCKER_IMAGE_VERSION')
    .asString(),
  concurrency: ({
    dataSource,
  }: {
    dataSource: string,
  }) => get(keyByDataSource('CUBEJS_CONCURRENCY', dataSource)).asInt(),
  // It's only excepted for CI, nothing else.
  internalExceptions: () => get('INTERNAL_EXCEPTIONS_YOU_WILL_BE_FIRED')
    .default('false')
    .asEnum(['exit', 'log', 'false']),
  preAggregationsSchema: () => get('CUBEJS_PRE_AGGREGATIONS_SCHEMA')
    .asString(),
  maxPartitionsPerCube: () => get('CUBEJS_MAX_PARTITIONS_PER_CUBE')
    .default('10000')
    .asInt(),
  scheduledRefreshBatchSize: () => get('CUBEJS_SCHEDULED_REFRESH_BATCH_SIZE')
    .default('1')
    .asInt(),
  nativeSqlPlanner: () => get('CUBEJS_TESSERACT_SQL_PLANNER').default('false').asBool(),
  nativeSqlPlannerPreAggregations: () => get('CUBEJS_TESSERACT_PRE_AGGREGATIONS').default('false').asBool(),
  nativeOrchestrator: () => get('CUBEJS_TESSERACT_ORCHESTRATOR')
    .default('true')
    .asBoolStrict(),
  transpilationWorkerThreads: () => get('CUBEJS_TRANSPILATION_WORKER_THREADS')
    .default('false')
    .asBoolStrict(),
  allowNonStrictDateRangeMatching: () => get('CUBEJS_PRE_AGGREGATIONS_ALLOW_NON_STRICT_DATE_RANGE_MATCH')
    .default('true')
    .asBoolStrict(),
  transpilationWorkerThreadsCount: () => get('CUBEJS_TRANSPILATION_WORKER_THREADS_COUNT')
    .default('0')
    .asInt(),
  // This one takes precedence over CUBEJS_TRANSPILATION_WORKER_THREADS
  transpilationNative: () => get('CUBEJS_TRANSPILATION_NATIVE')
    .default('false')
    .asBoolStrict(),
  nestedFoldersDelimiter: () => get('CUBEJS_NESTED_FOLDERS_DELIMITER')
    .default('')
    .asString(),

  /** ****************************************************************
   * Common db options                                               *
   ***************************************************************** */

  /**
   * Configured datasources.
   */
  dataSources: (): string[] => {
    const dataSources = process.env.CUBEJS_DATASOURCES;
    if (dataSources) {
      return dataSources.trim().split(',').map(ds => ds.trim());
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
    process.env[keyByDataSource('CUBEJS_DB_TYPE', dataSource)]
  ),

  /**
   * Use SSL connection flag.
   */
  dbSsl: ({
    dataSource,
  }: {
    dataSource: string,
  }) => {
    const val = process.env[
      keyByDataSource('CUBEJS_DB_SSL', dataSource)
    ] || 'false';
    if (val.toLocaleLowerCase() === 'true') {
      return true;
    } else if (val.toLowerCase() === 'false') {
      return false;
    } else {
      throw new TypeError(
        `The ${
          keyByDataSource('CUBEJS_DB_SSL', dataSource)
        } must be either 'true' or 'false'.`
      );
    }
  },

  /**
   * Reject unauthorized SSL connection flag.
   */
  dbSslRejectUnauthorized: ({
    dataSource,
  }: {
    dataSource: string,
  }) => {
    const val = process.env[
      keyByDataSource('CUBEJS_DB_SSL_REJECT_UNAUTHORIZED', dataSource)
    ] || 'false';
    if (val.toLocaleLowerCase() === 'true') {
      return true;
    } else if (val.toLowerCase() === 'false') {
      return false;
    } else {
      throw new TypeError(
        `The ${
          keyByDataSource('CUBEJS_DB_SSL_REJECT_UNAUTHORIZED', dataSource)
        } must be either 'true' or 'false'.`
      );
    }
  },

  /**
   * Database URL.
   */
  dbUrl: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_URL', dataSource)
    ]
  ),

  /**
   * Database host.
   */
  dbHost: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_HOST', dataSource)
    ]
  ),

  /**
   * Use `SELECT 1` query for testConnection.
   * It might be used in any driver where there is a specific testConnection
   * like a REST call, but for some reason it's not possible to use it in
   * deployment environment.
   */
  dbUseSelectTestConnection: ({
    dataSource,
  }: {
    dataSource: string,
  }) => {
    const val = process.env[
      keyByDataSource('CUBEJS_DB_USE_SELECT_TEST_CONNECTION', dataSource)
    ] || 'false';
    if (val.toLocaleLowerCase() === 'true') {
      return true;
    } else if (val.toLowerCase() === 'false') {
      return false;
    } else {
      throw new TypeError(
        `The ${
          keyByDataSource('CUBEJS_DB_USE_SELECT_TEST_CONNECTION', dataSource)
        } must be either 'true' or 'false'.`
      );
    }
  },

  /**
   * Kafka host for direct downloads from ksqlDb
   */
  dbKafkaHost: ({ dataSource }: {
    dataSource: string,
  }) => (
    process.env[keyByDataSource('CUBEJS_DB_KAFKA_HOST', dataSource)]
  ),

  /**
   * Kafka user for direct downloads from ksqlDb
   */
  dbKafkaUser: ({ dataSource }: {
    dataSource: string,
  }) => (
    process.env[keyByDataSource('CUBEJS_DB_KAFKA_USER', dataSource)]
  ),

  /**
   * Kafka password for direct downloads from ksqlDb
   */
  dbKafkaPass: ({ dataSource }: {
    dataSource: string,
  }) => (
    process.env[keyByDataSource('CUBEJS_DB_KAFKA_PASS', dataSource)]
  ),

  /**
   * `true` if Kafka should use SASL_SSL for direct downloads from ksqlDb
   */
  dbKafkaUseSsl: ({ dataSource }: {
    dataSource: string,
  }) => (
    get(keyByDataSource('CUBEJS_DB_KAFKA_USE_SSL', dataSource))
      .default('false')
      .asBool()
  ),

  /**
   * Database domain.
   */
  dbDomain: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    process.env[keyByDataSource('CUBEJS_DB_DOMAIN', dataSource)]
  ),

  /**
   * Database port.
   */
  dbPort: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    process.env[keyByDataSource('CUBEJS_DB_PORT', dataSource)]
      ? parseInt(
        `${
          process.env[keyByDataSource('CUBEJS_DB_PORT', dataSource)]
        }`,
        10,
      )
      : undefined
  ),

  /**
   * Database socket path.
   */
  dbSocketPath: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    process.env[keyByDataSource('CUBEJS_DB_SOCKET_PATH', dataSource)]
  ),

  /**
   * Database user.
   */
  dbUser: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    process.env[keyByDataSource('CUBEJS_DB_USER', dataSource)]
  ),

  /**
   * Database pass.
   */
  dbPass: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    process.env[keyByDataSource('CUBEJS_DB_PASS', dataSource)]
  ),

  /**
   * Database name.
   */
  dbName: ({
    required,
    dataSource,
  }: {
    dataSource: string,
    required?: boolean,
  }) => {
    const val = process.env[
      keyByDataSource('CUBEJS_DB_NAME', dataSource)
    ];
    if (required && !val) {
      throw new Error(
        `The ${
          keyByDataSource('CUBEJS_DB_NAME', dataSource)
        } is required and missing.`
      );
    }
    return val;
  },

  /**
   * Database name.
   * @deprecated
   */
  dbSchema: ({
    required,
    dataSource,
  }: {
    dataSource: string,
    required?: boolean,
  }) => {
    console.warn(
      `The ${
        keyByDataSource('CUBEJS_DB_SCHEMA', dataSource)
      } is deprecated. Please, use the ${
        keyByDataSource('CUBEJS_DB_NAME', dataSource)
      } instead.`
    );
    const val = process.env[
      keyByDataSource('CUBEJS_DB_SCHEMA', dataSource)
    ];
    if (required && !val) {
      throw new Error(
        `The ${
          keyByDataSource('CUBEJS_DB_SCHEMA', dataSource)
        } is required and missing.`
      );
    }
    return val;
  },

  /**
   * Database name.
   * @deprecated
   */
  dbDatabase: ({
    required,
    dataSource,
  }: {
    dataSource: string,
    required?: boolean,
  }) => {
    console.warn(
      `The ${
        keyByDataSource('CUBEJS_DATABASE', dataSource)
      } is deprecated. Please, use the ${
        keyByDataSource('CUBEJS_DB_NAME', dataSource)
      } instead.`
    );
    const val = process.env[
      keyByDataSource('CUBEJS_DATABASE', dataSource)
    ];
    if (required && !val) {
      throw new Error(
        `The ${
          keyByDataSource('CUBEJS_DATABASE', dataSource)
        } is required and missing.`
      );
    }
    return val;
  },

  /**
   * Database max pool size.
   */
  dbMaxPoolSize: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    process.env[keyByDataSource('CUBEJS_DB_MAX_POOL', dataSource)]
      ? parseInt(
        `${
          process.env[
            keyByDataSource('CUBEJS_DB_MAX_POOL', dataSource)
          ]
        }`,
        10,
      )
      : undefined
  ),

  /**
   * Max polling interval. Currently used in BigQuery and Databricks.
   * TODO: clarify this env.
   */
  dbPollMaxInterval: ({
    dataSource,
  }: {
    dataSource: string,
  }) => {
    const key = keyByDataSource('CUBEJS_DB_POLL_MAX_INTERVAL', dataSource);
    const value = process.env[key] || '5s';
    return convertTimeStrToSeconds(value, key);
  },

  /**
   * Polling timeout. Currently used in BigQuery, Dremio and Athena.
   * TODO: clarify this env.
   */
  dbPollTimeout: ({
    dataSource,
  }: {
    dataSource: string,
  }) => {
    const key = keyByDataSource('CUBEJS_DB_POLL_TIMEOUT', dataSource);
    const value = process.env[key];
    if (value) {
      return convertTimeStrToSeconds(value, key);
    } else {
      return null;
    }
  },

  /**
   * Query timeout. Currently used in BigQuery, ClickHouse, Dremio, Postgres, Snowflake
   * and Athena drivers and the orchestrator (queues, pre-aggs). For the
   * orchestrator this variable did not split by the datasource.
   *
   * TODO (buntarb): check the possibility to split this for the
   * orchestrator. This will allows us to make dataSource required.
   */
  dbQueryTimeout: ({
    dataSource,
  }: {
    dataSource?: string,
  } = {}) => {
    const key = keyByDataSource('CUBEJS_DB_QUERY_TIMEOUT', dataSource);
    const value = process.env[key] || '10m';
    return convertTimeStrToSeconds(value, key);
  },

  /**
   * Max limit which can be specified in the incoming query.
   */
  dbQueryLimit: (): number => get('CUBEJS_DB_QUERY_LIMIT')
    .default(50000)
    .asInt(),

  /**
   * Query limit wich will be used in the query to the data source if
   * limit property was not specified in the query.
   */
  dbQueryDefaultLimit: (): number => get('CUBEJS_DB_QUERY_DEFAULT_LIMIT')
    .default(10000)
    .asInt(),

  /**
   * Query stream `highWaterMark` value.
   */
  dbQueryStreamHighWaterMark: (): number => get('CUBEJS_DB_QUERY_STREAM_HIGH_WATER_MARK')
    .default(8192)
    .asInt(),

  /**
   * Max number of elements
   */
  usedPreAggregationCacheMaxCount: (): number => get('CUBEJS_USED_PRE_AGG_CACHE_MAX_COUNT')
    .default(8192)
    .asInt(),

  /**
   * Max number of elements
   */
  touchPreAggregationCacheMaxCount: (): number => get('CUBEJS_TOUCH_PRE_AGG_CACHE_MAX_COUNT')
    .default(8192)
    .asInt(),

  /**
   * Max cache
   */
  touchPreAggregationCacheMaxAge: (): number => {
    // eslint-disable-next-line no-use-before-define
    const touchPreAggregationTimeout = getEnv('touchPreAggregationTimeout');

    const maxAge = get('CUBEJS_TOUCH_PRE_AGG_CACHE_MAX_AGE')
      .default(Math.round(touchPreAggregationTimeout / 2))
      .asIntPositive();

    if (maxAge > touchPreAggregationTimeout) {
      throw new InvalidConfiguration(
        'CUBEJS_TOUCH_PRE_AGG_CACHE_MAX_AGE',
        maxAge,
        `Must be less or equal then CUBEJS_TOUCH_PRE_AGG_TIMEOUT (${touchPreAggregationTimeout}).`
      );
    }

    return maxAge;
  },

  /**
   * Expire time for touch records
   */
  touchPreAggregationTimeout: (): number => get('CUBEJS_TOUCH_PRE_AGG_TIMEOUT')
    .default(60 * 60 * 24)
    .asIntPositive(),

  /**
   * Expire time for touch records
   */
  dropPreAggregationsWithoutTouch: (): boolean => get('CUBEJS_DROP_PRE_AGG_WITHOUT_TOUCH')
    .default('true')
    .asBoolStrict(),

  /**
   * Fetch Columns by Ordinal Position
   *
   * Currently defaults to 'false' as changing this in a live deployment could break existing pre-aggregations.
   * This will eventually default to true.
   */
  fetchColumnsByOrdinalPosition: (): boolean => get('CUBEJS_DB_FETCH_COLUMNS_BY_ORDINAL_POSITION')
    .default('true')
    .asBoolStrict(),

  /** ****************************************************************
   * JDBC options                                                    *
   ***************************************************************** */

  /**
   * JDBC URL.
   */
  jdbcUrl: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    process.env[keyByDataSource('CUBEJS_JDBC_URL', dataSource)]
  ),

  /**
   * JDBC driver.
   */
  jdbcDriver: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    process.env[keyByDataSource('CUBEJS_JDBC_DRIVER', dataSource)]
  ),

  /** ****************************************************************
   * Export Bucket options                                           *
   ***************************************************************** */

  /**
   * Export bucket CSV escape symbol.
   */
  dbExportBucketCsvEscapeSymbol: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    process.env[keyByDataSource('CUBEJS_DB_EXPORT_BUCKET_CSV_ESCAPE_SYMBOL', dataSource)]
  ),

  /**
   * Export bucket storage type.
   */
  dbExportBucketType: ({
    supported,
    dataSource,
  }: {
    supported: ('s3' | 'gcp' | 'azure')[],
    dataSource: string,
  }) => {
    const val = process.env[
      keyByDataSource('CUBEJS_DB_EXPORT_BUCKET_TYPE', dataSource)
    ];
    if (
      val &&
      supported &&
      supported.indexOf(<'s3' | 'gcp' | 'azure'>val) === -1
    ) {
      throw new TypeError(
        `The ${
          keyByDataSource('CUBEJS_DB_EXPORT_BUCKET_TYPE', dataSource)
        } must be one of the [${supported.join(', ')}].`
      );
    }
    return val;
  },

  /**
   * Export bucket storage URI.
   */
  dbExportBucket: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    process.env[keyByDataSource('CUBEJS_DB_EXPORT_BUCKET', dataSource)]
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
    process.env[
      keyByDataSource('CUBEJS_DB_EXPORT_BUCKET_MOUNT_DIR', dataSource)
    ]
  ),

  /**
   * AWS Key for the AWS based export bucket storage.
   */
  dbExportBucketAwsKey: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_EXPORT_BUCKET_AWS_KEY', dataSource)
    ]
  ),

  /**
   * AWS Secret for the AWS based export bucket storage.
   */
  dbExportBucketAwsSecret: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_EXPORT_BUCKET_AWS_SECRET', dataSource)
    ]
  ),

  /**
   * AWS Region for the AWS based export bucket storage.
   */
  dbExportBucketAwsRegion: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_EXPORT_BUCKET_AWS_REGION', dataSource)
    ]
  ),

  /**
   * Azure Key for the Azure based export bucket storage.
   */
  dbExportBucketAzureKey: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_EXPORT_BUCKET_AZURE_KEY', dataSource)
    ]
  ),

  /**
   * Azure SAS Token for the Azure based export bucket storage.
   */
  dbExportAzureSasToken: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_EXPORT_BUCKET_AZURE_SAS_TOKEN', dataSource)
    ]
  ),

  /**
   * Azure Client ID for the Azure based export bucket storage.
   */
  dbExportBucketAzureClientId: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_EXPORT_BUCKET_AZURE_CLIENT_ID', dataSource)
    ]
  ),

  /**
    * Client Secret for the Azure based export bucket storage.
    */
  dbExportBucketAzureClientSecret: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_EXPORT_BUCKET_AZURE_CLIENT_SECRET', dataSource)
    ]
  ),

  /**
   * Azure Federated Token File Path for the Azure based export bucket storage.
   */
  dbExportBucketAzureTokenFilePAth: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_EXPORT_BUCKET_AZURE_FEDERATED_TOKEN_FILE', dataSource)
    ]
  ),

  /**
   * Azure Tenant ID for the Azure based export bucket storage.
   */
  dbExportBucketAzureTenantId: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_EXPORT_BUCKET_AZURE_TENANT_ID', dataSource)
    ]
  ),

  /**
   * Export bucket options for Integration based.
   */
  dbExportIntegration: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_EXPORT_INTEGRATION', dataSource)
    ]
  ),

  /**
   * Export bucket options for GCS.
   */
  dbExportGCSCredentials: ({
    dataSource,
  }: {
    dataSource: string,
  }) => {
    const credentials = process.env[
      keyByDataSource('CUBEJS_DB_EXPORT_GCS_CREDENTIALS', dataSource)
    ];
    if (credentials) {
      return JSON.parse(
        Buffer.from(credentials, 'base64').toString('utf8')
      );
    }
    return undefined;
  },

  /** ****************************************************************
   * MySQL Driver                                                    *
   ***************************************************************** */

  /**
   * Use timezone names for date/time conversions.
   * Defaults to FALSE, meaning that numeric offsets for timezone will be used.
   * @see https://dev.mysql.com/doc/refman/8.4/en/date-and-time-functions.html#function_convert-tz
   * @see https://dev.mysql.com/doc/refman/8.4/en/time-zone-support.html
   */
  mysqlUseNamedTimezones: ({ dataSource }: { dataSource: string }) => {
    const val = process.env[
      keyByDataSource(
        'CUBEJS_DB_MYSQL_USE_NAMED_TIMEZONES',
        dataSource,
      )
    ];

    if (val) {
      if (val.toLocaleLowerCase() === 'true') {
        return true;
      } else if (val.toLowerCase() === 'false') {
        return false;
      } else {
        throw new TypeError(
          `The ${
            keyByDataSource(
              'CUBEJS_DB_MYSQL_USE_NAMED_TIMEZONES',
              dataSource,
            )
          } must be either 'true' or 'false'.`
        );
      }
    } else {
      return false;
    }
  },

  /** ****************************************************************
   * Databricks Driver                                               *
   ***************************************************************** */

  /**
   * Databricks jdbc-connection url.
   */
  databricksUrl: ({
    dataSource,
  }: {
    dataSource: string,
  }) => {
    const val = process.env[
      keyByDataSource('CUBEJS_DB_DATABRICKS_URL', dataSource)
    ];
    if (!val) {
      throw new Error(
        `The ${
          keyByDataSource('CUBEJS_DB_DATABRICKS_URL', dataSource)
        } is required and missing.`
      );
    }
    return val;
  },

  /**
   * Databricks jdbc-connection token.
   */
  databricksToken: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_DATABRICKS_TOKEN', dataSource)
    ]
  ),

  /**
   * Databricks catalog name.
   * https://www.databricks.com/product/unity-catalog
   */
  databricksCatalog: ({
    dataSource,
  }: {
    dataSource: string,
  }) => process.env[
    keyByDataSource('CUBEJS_DB_DATABRICKS_CATALOG', dataSource)
  ],

  /**
   * Databricks OAuth Client ID (Same as the service principal UUID)
   */
  databricksOAuthClientId: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_DATABRICKS_OAUTH_CLIENT_ID', dataSource)
    ]
  ),

  /**
   * Databricks OAuth Client Secret.
   */
  databricksOAuthClientSecret: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_DATABRICKS_OAUTH_CLIENT_SECRET', dataSource)
    ]
  ),

  /** ****************************************************************
   * Athena Driver                                                   *
   ***************************************************************** */

  /**
   * Athena AWS key.
   */
  athenaAwsKey: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    // TODO (buntarb): this name is a common. Deprecate and replace?
    process.env[keyByDataSource('CUBEJS_AWS_KEY', dataSource)]
  ),

  /**
   * Athena AWS secret.
   */
  athenaAwsSecret: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    // TODO (buntarb): this name is a common. Deprecate and replace?
    process.env[keyByDataSource('CUBEJS_AWS_SECRET', dataSource)]
  ),

  /**
   * Athena AWS region.
   */
  athenaAwsRegion: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    // TODO (buntarb): this name is a common. Deprecate and replace?
    process.env[keyByDataSource('CUBEJS_AWS_REGION', dataSource)]
  ),

  /**
   * Athena AWS S3 output location.
   */
  athenaAwsS3OutputLocation: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    // TODO (buntarb): this name is a common. Deprecate and replace?
    process.env[
      keyByDataSource('CUBEJS_AWS_S3_OUTPUT_LOCATION', dataSource)
    ]
  ),

  /**
   * Athena AWS workgroup.
   */
  athenaAwsWorkgroup: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    // TODO (buntarb): Deprecate and replace?
    process.env[
      keyByDataSource('CUBEJS_AWS_ATHENA_WORKGROUP', dataSource)
    ]
  ),

  /**
   * Athena AWS Catalog.
   */
  athenaAwsCatalog: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    // TODO (buntarb): Deprecate and replace?
    process.env[
      keyByDataSource('CUBEJS_AWS_ATHENA_CATALOG', dataSource)
    ]
  ),

  /**
   * Athena AWS Assume Role ARN.
   */
  athenaAwsAssumeRoleArn: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_AWS_ATHENA_ASSUME_ROLE_ARN', dataSource)
    ]
  ),

  /**
   * Athena AWS Assume Role External ID.
   */
  athenaAwsAssumeRoleExternalId: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_AWS_ATHENA_ASSUME_ROLE_EXTERNAL_ID', dataSource)
    ]
  ),

  /** ****************************************************************
   * BigQuery Driver                                                 *
   ***************************************************************** */

  /**
   * BigQuery project ID.
   */
  bigqueryProjectId: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[keyByDataSource('CUBEJS_DB_BQ_PROJECT_ID', dataSource)]
  ),

  /**
   * BigQuery Key file.
   */
  bigqueryKeyFile: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[keyByDataSource('CUBEJS_DB_BQ_KEY_FILE', dataSource)]
  ),

  /**
   * BigQuery credentials.
   */
  bigqueryCredentials: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_BQ_CREDENTIALS', dataSource)
    ]
  ),

  /**
   * BigQuery location.
   */
  bigqueryLocation: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[keyByDataSource('CUBEJS_DB_BQ_LOCATION', dataSource)]
  ),

  /**
   * BigQuery export bucket.
   * @deprecated
   */
  bigqueryExportBucket: ({
    dataSource
  }: {
    dataSource: string,
  }) => {
    console.warn(
      'The CUBEJS_DB_BQ_EXPORT_BUCKET is deprecated. ' +
      'Please, use the CUBEJS_DB_EXPORT_BUCKET instead.'
    );
    return process.env[
      keyByDataSource('CUBEJS_DB_BQ_EXPORT_BUCKET', dataSource)
    ];
  },

  /** ****************************************************************
   * ClickHouse Driver                                               *
   ***************************************************************** */

  /**
   * ClickHouse read only flag.
   */
  clickhouseReadOnly: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    get(keyByDataSource('CUBEJS_DB_CLICKHOUSE_READONLY', dataSource))
      .default('false')
      .asBool()
  ),

  /**
   * ClickHouse compression flag.
   */
  clickhouseCompression: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    get(keyByDataSource('CUBEJS_DB_CLICKHOUSE_COMPRESSION', dataSource))
      .default('false')
      .asBool()
  ),

  /** ****************************************************************
   * ElasticSearch Driver                                            *
   ***************************************************************** */

  /**
   * ElasticSearch API Id.
   */
  elasticApiId: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_ELASTIC_APIKEY_ID', dataSource)
    ]
  ),

  /**
   * ElasticSearch API Key.
   */
  elasticApiKey: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_ELASTIC_APIKEY_KEY', dataSource)
    ]
  ),

  /**
   * ElasticSearch OpenDistro flag.
   */
  elasticOpenDistro: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_ELASTIC_OPENDISTRO', dataSource)
    ]
  ),

  /**
   * ElasticSearch query format.
   */
  elasticQueryFormat: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_ELASTIC_QUERY_FORMAT', dataSource)
    ]
  ),

  /** ****************************************************************
   * Firebolt Driver                                                 *
   ***************************************************************** */

  /**
   * Firebolt API endpoint.
   */
  fireboltApiEndpoint: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_FIREBOLT_API_ENDPOINT', dataSource)
    ]
  ),

  /**
   * Firebolt engine name.
   */
  fireboltEngineName: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_FIREBOLT_ENGINE_NAME', dataSource)
    ]
  ),

  /**
   * Firebolt engine endpoint.
   */
  fireboltEngineEndpoint: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_FIREBOLT_ENGINE_ENDPOINT', dataSource)
    ]
  ),

  /**
   * Firebolt account name.
   */
  fireboltAccount: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_FIREBOLT_ACCOUNT', dataSource)
    ]
  ),

  /** ****************************************************************
   * Hive Driver                                                     *
   ***************************************************************** */

  /**
   * Hive type.
   */
  hiveType: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_HIVE_TYPE', dataSource)
    ]
  ),

  /**
   * Hive version.
   */
  hiveVer: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_HIVE_VER', dataSource)
    ]
  ),

  /**
   * Hive thrift version.
   */
  hiveThriftVer: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_HIVE_THRIFT_VER', dataSource)
    ]
  ),

  /**
   * Hive CDH version.
   */
  hiveCdhVer: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_HIVE_CDH_VER', dataSource)
    ]
  ),

  /** ****************************************************************
   * Aurora Driver                                                   *
   ***************************************************************** */

  /**
   * Aurora secret ARN.
   */
  auroraSecretArn: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DATABASE_SECRET_ARN', dataSource)
    ]
  ),

  /**
   * Aurora cluster ARN.
   */
  auroraClusterArn: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DATABASE_CLUSTER_ARN', dataSource)
    ]
  ),

  /** ****************************************************************
   * Redshift Driver                                                 *
   ***************************************************************** */

  /**
   * Redshift export bucket unload ARN.
   */
  redshiftUnloadArn: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_EXPORT_BUCKET_REDSHIFT_ARN', dataSource)
    ]
  ),

  /** ****************************************************************
   * Materialize Driver                                              *
   ***************************************************************** */

  /**
   * Materialize cluster.
   */
  materializeCluster: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_MATERIALIZE_CLUSTER', dataSource)
    ]
  ),

  /** ****************************************************************
   * Snowflake Driver                                                *
   ***************************************************************** */

  /**
   * Snowflake account.
   */
  snowflakeAccount: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_SNOWFLAKE_ACCOUNT', dataSource)
    ]
  ),

  /**
   * Snowflake region.
   */
  snowflakeRegion: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_SNOWFLAKE_REGION', dataSource)
    ]
  ),

  /**
   * Snowflake warehouse.
   */
  snowflakeWarehouse: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_SNOWFLAKE_WAREHOUSE', dataSource)
    ]
  ),

  /**
   * Snowflake role.
   */
  snowflakeRole: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_SNOWFLAKE_ROLE', dataSource)
    ]
  ),

  /**
   * Snowflake session keep alive flag.
   */
  snowflakeSessionKeepAlive: ({
    dataSource
  }: {
    dataSource: string,
  }) => {
    const val = process.env[
      keyByDataSource(
        'CUBEJS_DB_SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE',
        dataSource,
      )
    ];
    if (val) {
      if (val.toLocaleLowerCase() === 'true') {
        return true;
      } else if (val.toLowerCase() === 'false') {
        return false;
      } else {
        throw new TypeError(
          `The ${
            keyByDataSource(
              'CUBEJS_DB_SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE',
              dataSource,
            )
          } must be either 'true' or 'false'.`
        );
      }
    } else {
      return true;
    }
  },

  /**
   * Snowflake authenticator.
   */
  snowflakeAuthenticator: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_SNOWFLAKE_AUTHENTICATOR', dataSource)
    ]
  ),

  /**
   * Snowflake OAuth token (string).
   */
  snowflakeOAuthToken: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_SNOWFLAKE_OAUTH_TOKEN', dataSource)
    ]
  ),

  /**
   * Snowflake OAuth token path.
   */
  snowflakeOAuthTokenPath: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_SNOWFLAKE_OAUTH_TOKEN_PATH', dataSource)
    ]
  ),

  /**
   * Snowflake host.
   */
  snowflakeHost: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_SNOWFLAKE_HOST', dataSource)
    ]
  ),

  /**
   * Snowflake private key.
   */
  snowflakePrivateKey: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY', dataSource)
    ]
  ),

  /**
   * Snowflake private key path.
   */
  snowflakePrivateKeyPath: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PATH', dataSource)
    ]
  ),

  /**
   * Snowflake private key pass.
   */
  snowflakePrivateKeyPass: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PASS', dataSource)
    ]
  ),

  /**
   * Snowflake case sensitivity for identifiers (like database columns).
   */
  snowflakeQuotedIdentIgnoreCase: ({
    dataSource
  }: {
    dataSource: string,
  }) => {
    const val = process.env[
      keyByDataSource(
        'CUBEJS_DB_SNOWFLAKE_QUOTED_IDENTIFIERS_IGNORE_CASE',
        dataSource,
      )
    ];
    if (val) {
      if (val.toLocaleLowerCase() === 'true') {
        return true;
      } else if (val.toLowerCase() === 'false') {
        return false;
      } else {
        throw new TypeError(
          `The ${
            keyByDataSource(
              'CUBEJS_DB_SNOWFLAKE_QUOTED_IDENTIFIERS_IGNORE_CASE',
              dataSource,
            )
          } must be either 'true' or 'false'.`
        );
      }
    } else {
      return false;
    }
  },
  /** ****************************************************************
   * Presto Driver                                                   *
   ***************************************************************** */

  /**
   * Presto catalog.
   */
  dbCatalog: ({
    dataSource,
  }: {
    dataSource: string,
  }) => {
    console.warn(
      'The CUBEJS_DB_CATALOG is deprecated. ' +
      'Please, use the CUBEJS_DB_PRESTO_CATALOG instead.'
    );
    return process.env[
      keyByDataSource('CUBEJS_DB_CATALOG', dataSource)
    ];
  },

  /** ****************************************************************
   * duckdb                                                         *
   ***************************************************************** */

  duckdbMotherDuckToken: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_DUCKDB_MOTHERDUCK_TOKEN', dataSource)
    ]
  ),

  duckdbDatabasePath: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_DUCKDB_DATABASE_PATH', dataSource)
    ]
  ),

  duckdbS3Region: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_DUCKDB_S3_REGION', dataSource)
    ]
  ),

  duckdbS3AccessKeyId: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_DUCKDB_S3_ACCESS_KEY_ID', dataSource)
    ]
  ),

  duckdbS3SecretAccessKeyId: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_DUCKDB_S3_SECRET_ACCESS_KEY', dataSource)
    ]
  ),

  duckdbS3Endpoint: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_DUCKDB_S3_ENDPOINT', dataSource)
    ]
  ),

  duckdbMemoryLimit: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_DUCKDB_MEMORY_LIMIT', dataSource)
    ]
  ),

  duckdbSchema: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_DUCKDB_SCHEMA', dataSource)
    ]
  ),

  duckdbS3UseSsl: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_DUCKDB_S3_USE_SSL', dataSource)
    ]
  ),

  duckdbS3UrlStyle: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_DUCKDB_S3_URL_STYLE', dataSource)
    ]
  ),

  duckdbS3SessionToken: ({
    dataSource
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_DUCKDB_S3_SESSION_TOKEN', dataSource)
    ]
  ),

  duckdbExtensions: ({
    dataSource
  }: {
    dataSource: string,
  }) => {
    const extensions = process.env[
      keyByDataSource('CUBEJS_DB_DUCKDB_EXTENSIONS', dataSource)
    ];
    if (extensions) {
      return extensions.split(',').map(e => e.trim());
    }
    return [];
  },

  duckdbCommunityExtensions: ({
    dataSource
  }: {
    dataSource: string,
  }) => {
    const extensions = process.env[
      keyByDataSource('CUBEJS_DB_DUCKDB_COMMUNITY_EXTENSIONS', dataSource)
    ];
    if (extensions) {
      return extensions.split(',').map(e => e.trim());
    }
    return [];
  },

  duckdbS3UseCredentialChain: ({
    dataSource
  }: {
    dataSource: string,
  }) => {
    const val = process.env[
      keyByDataSource('CUBEJS_DB_DUCKDB_S3_USE_CREDENTIAL_CHAIN', dataSource)
    ];

    if (val) {
      if (val.toLocaleLowerCase() === 'true') {
        return true;
      } else if (val.toLowerCase() === 'false') {
        return false;
      } else {
        throw new TypeError(
          `The ${
            keyByDataSource(
              'CUBEJS_DB_DUCKDB_S3_USE_CREDENTIAL_CHAIN',
              dataSource,
            )
          } must be either 'true' or 'false'.`
        );
      }
    } else {
      return false;
    }
  },

  /** ***************************************************************
   * Presto/Trino Driver                                                  *
   **************************************************************** */

  /**
   * Presto catalog.
   */
  prestoCatalog: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_PRESTO_CATALOG', dataSource)
    ]
  ),

  /**
   * Presto/Trino Auth Token
   */
  prestoAuthToken: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_PRESTO_AUTH_TOKEN', dataSource)
    ]
  ),

  /** ***************************************************************
   * Pinot Driver                                                  *
   **************************************************************** */

  /**
   * Pinot/Startree Auth Token
   */
  pinotAuthToken: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_PINOT_AUTH_TOKEN', dataSource)
    ]
  ),

  /**
   * Pinot / Startree Null value support
   */

  pinotNullHandling: ({ dataSource }: { dataSource: string }) => {
    const val = process.env[
      keyByDataSource('CUBEJS_DB_PINOT_NULL_HANDLING', dataSource)
    ];

    if (val) {
      if (val.toLocaleLowerCase() === 'true') {
        return true;
      } else if (val.toLowerCase() === 'false') {
        return false;
      } else {
        throw new TypeError(
          `The ${
            keyByDataSource(
              'CUBEJS_DB_PINOT_NULL_HANDLING',
              dataSource,
            )
          } must be either 'true' or 'false'.`
        );
      }
    } else {
      return false;
    }
  },

  /** ****************************************************************
   * Dremio Driver                                                   *
   ***************************************************************** */

  /**
   * Dremio Auth Token
   */
  dremioAuthToken: ({
    dataSource,
  }: {
    dataSource: string,
  }) => (
    process.env[
      keyByDataSource('CUBEJS_DB_DREMIO_AUTH_TOKEN', dataSource)
    ]
  ),

  /** ****************************************************************
   * Cube Store Driver                                               *
   ***************************************************************** */

  cubeStoreHost: () => get('CUBEJS_CUBESTORE_HOST')
    .asString(),
  cubeStorePort: () => get('CUBEJS_CUBESTORE_PORT')
    .asPortNumber(),
  cubeStoreUser: () => get('CUBEJS_CUBESTORE_USER')
    .asString(),
  cubeStorePass: () => get('CUBEJS_CUBESTORE_PASS')
    .asString(),
  cubeStoreMaxConnectRetries: () => get('CUBEJS_CUBESTORE_MAX_CONNECT_RETRIES')
    .default('20')
    .asInt(),
  cubeStoreNoHeartBeatTimeout: () => get('CUBEJS_CUBESTORE_NO_HEART_BEAT_TIMEOUT')
    .default('30')
    .asInt(),
  cubeStoreRollingWindowJoin: () => get('CUBEJS_CUBESTORE_ROLLING_WINDOW_JOIN')
    .default('false')
    .asBoolStrict(),
  allowUngroupedWithoutPrimaryKey: () => get('CUBEJS_ALLOW_UNGROUPED_WITHOUT_PRIMARY_KEY')
    .default(get('CUBESQL_SQL_PUSH_DOWN').default('true').asString())
    .asBoolStrict(),
  nodeEnv: () => get('NODE_ENV')
    .asString(),
  cacheAndQueueDriver: () => get('CUBEJS_CACHE_AND_QUEUE_DRIVER')
    .asString(),
  defaultApiScope: () => get('CUBEJS_DEFAULT_API_SCOPES')
    .asArray(','),
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
  nativeApiGatewayPort: () => {
    if (process.env.CUBEJS_NATIVE_API_GATEWAY_PORT === 'false') {
      return undefined;
    }

    const port = asFalseOrPort(process.env.CUBEJS_NATIVE_API_GATEWAY_PORT || 'false', 'CUBEJS_NATIVE_API_GATEWAY_PORT');
    if (port) {
      return port;
    }

    return undefined;
  },
  pgSqlPort: () => {
    if (process.env.CUBEJS_PG_SQL_PORT === 'false') {
      return undefined;
    }

    const port = asFalseOrPort(process.env.CUBEJS_PG_SQL_PORT || 'false', 'CUBEJS_PG_SQL_PORT');
    if (port) {
      return port;
    }

    const isDevMode = get('CUBEJS_DEV_MODE')
      .default('false')
      .asBoolStrict();

    if (isDevMode) {
      if (isNativeSupported()) {
        return 15432;
      } else {
        displayCLIWarning(
          'Native module is not supported on your platform. Please use official docker image as a recommended way'
        );

        return false;
      }
    }

    return undefined;
  },
  sqlUser: () => get('CUBEJS_SQL_USER').asString(),
  sqlPassword: () => get('CUBEJS_SQL_PASSWORD').asString(),
  sqlSuperUser: () => get('CUBEJS_SQL_SUPER_USER').asString(),
  // Internal testing, please don't enable it. It's not ready for public preview
  nativeApiGateway: () => get('CUBE_JS_NATIVE_API_GATEWAY_INTERNAL')
    .asBool(),
  // Experiments & Preview flags
  livePreview: () => get('CUBEJS_LIVE_PREVIEW')
    .default('true')
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
    .asInt(),
  convertTzForRawTimeDimension: () => get('CUBESQL_SQL_PUSH_DOWN').default('true').asBoolStrict(),
  // Deprecated section

  // Support for Redis as queue & cache driver was removed in 0.36
  // This code is used to detect Redis and throw an error
  // TODO(ovr): Remove in after 1.0 + LTS
  redisUseIORedis: () => get('CUBEJS_REDIS_USE_IOREDIS')
    .default('false')
    .asBoolStrict(),
  // TODO(ovr): Remove in after 1.0 + LTS
  redisUrl: () => {
    const redisUrl = get('CUBEJS_REDIS_URL')
      .asString();
    if (redisUrl) {
      return redisUrl;
    }

    const legacyRedisUrl = get('REDIS_URL')
      .asString();
    if (legacyRedisUrl) {
      return legacyRedisUrl;
    }

    return undefined;
  },
  fastReload: () => get('CUBEJS_FAST_RELOAD_ENABLED')
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
