import type { Options as PoolConfiguration } from 'generic-pool';

export interface ConnectionConfig {
  /**
   * The hostname of the database you are connecting to. (Default: localhost)
   */
  host?: string;

  /**
   * The port number to connect to. (Default: 3306)
   */
  port?: number;

  /**
   * The user to authenticate as
   */
  user?: string;

  /**
   * The password of that MySQL user
   */
  password?: string;

  /**
   * Pool options
   */
  pool?: PoolConfiguration;
}
