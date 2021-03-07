export interface ConnectionConfig {
  /**
   * Cube Store web socket URL
   */
  url?: string;
  /**
   * The hostname of the database you are connecting to. (Default: localhost)
   */
  host?: string;

  /**
   * The port number to connect to. (Default: 3030)
   */
  port?: number;

  /**
   * The user to authenticate as
   */
  user?: string;

  /**
   * The password
   */
  password?: string;
}
