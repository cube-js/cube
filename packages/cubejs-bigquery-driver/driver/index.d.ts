import { BigQueryOptions } from "@google-cloud/bigquery";

declare module "@cubejs-backend/bigquery-driver" {
  class BigQueryDriver {
    constructor(options?: BigQueryOptions);
  }
  export = BigQueryDriver;
}
