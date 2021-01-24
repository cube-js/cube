import { ConnectionOptions } from "mysql2";

declare module "@cubejs-backend/mongobi-driver" {
  export default class MongoBIDriver {
    constructor(options?: ConnectionOptions);
  }
}
