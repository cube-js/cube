declare module "@cubejs-backend/sqlite-driver" {
  interface SqliteOptions {
    database: string;
  }

  class SqliteDriver {
    constructor(options?: SqliteOptions);
  }
  export = SqliteDriver;
}
