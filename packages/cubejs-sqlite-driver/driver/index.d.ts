declare module "@cubejs-backend/sqlite-driver" {
  interface SqliteOptions {
    database: string;
  }

  export default class SqliteDriver {
    constructor(options?: SqliteOptions);
  }
}
