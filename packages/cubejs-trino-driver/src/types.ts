import type { ConnectionOptions as TLSConnectionOptions } from 'tls';

/**
 * Linear backoff used only before the first non-empty data page.
 * Each interval is held for `triesBeforeIncrement` empty polls before stepping up.
 * After the first rows arrive, polling uses `drainInterval` (default 0).
 */
export type PollBackoffConfig = {
  /** First wait before nextUri while still waiting for rows (default: 50) */
  initialInterval?: number;
  /** Added to the wait after triesBeforeIncrement empty polls (default: 50) */
  incrementStep?: number;
  /** Cap on wait between status polls while waiting (default: 500) */
  maxInterval?: number;
  /**
   * Empty wait-polls at the current interval before increasing (default: 10).
   * Chosen so initialInterval × triesBeforeIncrement ≈ 500ms of fine polling.
   */
  triesBeforeIncrement?: number;
};

export type TrinoDriverExportBucket = {
  exportBucket?: string;
  bucketType?: 'gcs' | 's3';
  credentials?: any;
  accessKeyId?: string;
  secretAccessKey?: string;
  exportBucketRegion?: string;
  exportBucketS3AdvancedFS?: boolean;
  exportBucketCsvEscapeSymbol?: string;
};

export type TrinoDriverConfiguration = TrinoDriverExportBucket & {
  host?: string;
  port?: string | number;
  catalog?: string;
  schema?: string;
  user?: string;
  // eslint-disable-next-line camelcase
  custom_auth?: string;
  // eslint-disable-next-line camelcase
  basic_auth?: { user: string; password: string };
  ssl?: boolean | TLSConnectionOptions;
  dataSource?: string;
  queryTimeout?: number;
  preAggregations?: boolean;
  useSelectTestConnection?: boolean;
  // @see https://trino.io/docs/current/develop/client-protocol.html
  headers?: Record<string, string>;
  /**
   * Source identifier sent as `X-Trino-Source`.
   * Default: `nodejs-client` (same as the previous `presto-client` default).
   */
  source?: string;
  /**
   * Session timezone sent as `X-Trino-Time-Zone`.
   */
  timezone?: string;
  /**
   * Extra Trino session properties. `queryTimeout` is always merged in as
   * `query_max_run_time` when set.
   */
  session?: string | Record<string, string>;
  /**
   * Progressive backoff while waiting for the first rows. Constructor values
   * override environment variables.
   */
  pollBackoff?: PollBackoffConfig;
  /**
   * Sleep (ms) between nextUri polls after the first rows arrive. Default 0
   * (drain as fast as Trino produces pages).
   */
  drainInterval?: number;
  /**
   * Legacy `presto-client` poll interval (ms). If set and `pollBackoff` is
   * omitted, wait-phase and drain polling both use this constant interval.
   */
  checkInterval?: number;
  /**
   * HTTP keep-alive for coordinator connections. Default: true.
   */
  keepAlive?: boolean;
  /**
   * Retries for 502/503/504 on nextUri polls. Default: 5.
   */
  maxTransientRetries?: number;
};

export type TrinoColumn = {
  name: string;
  type: string;
};

export type TrinoQueryResponse = {
  id?: string;
  infoUri?: string;
  nextUri?: string;
  columns?: TrinoColumn[];
  data?: any[][];
  stats?: {
    state: string;
    queued?: boolean;
    scheduled?: boolean;
    nodes?: number;
    totalSplits?: number;
    queuedSplits?: number;
    runningSplits?: number;
    completedSplits?: number;
  };
  error?: {
    message: string;
    errorCode?: number;
    errorName?: string;
    errorType?: string;
    failureInfo?: any;
  };
};
