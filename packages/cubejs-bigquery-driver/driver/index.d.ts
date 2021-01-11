import { BigQueryOptions } from "@google-cloud/bigquery";

declare module "@cubejs-backend/bigquery-driver" {
  interface BigQueryDriverOptions extends BigQueryOptions {
    readOnly?: boolean
    pollTimeout?: number,
    pollMaxInterval?: number,
  }

  export default class BigQueryDriver {
    constructor(options?: BigQueryDriverOptions);
  }
}
