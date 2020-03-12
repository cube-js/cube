import { BigQueryOptions } from "@google-cloud/bigquery";

declare module "@cubejs-backend/bigquery-driver" {
  export default class BigQueryDriver {
    constructor(options?: BigQueryOptions);
  }
}
