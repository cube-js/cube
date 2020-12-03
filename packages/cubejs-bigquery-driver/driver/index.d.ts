import { BigQueryOptions } from "@google-cloud/bigquery";

declare module "@cubejs-backend/bigquery-driver" {
  interface BigQueryDriverOptions extends BigQueryOptions {
    readOnly?: boolean
  }

  export default class BigQueryDriver {
    constructor(options?: BigQueryDriverOptions);
  }
}
