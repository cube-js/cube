import { ConnectionConfig } from "mysql";

declare module "@cubejs-backend/mysql-driver" {
  export default class MySqlDriver {
    constructor(options?: ConnectionConfig);
  }
}
