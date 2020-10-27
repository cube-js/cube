interface ConnectionConfig {
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
}

declare module "@cubejs-backend/cubestore-driver" {
  export default class CubeStoreDriver {
    constructor(options?: ConnectionConfig);
  }
}
