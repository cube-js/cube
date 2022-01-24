import * as AWS from "@aws-sdk/client-athena";
import * as stream from 'stream';
import { AthenaQueryId } from "./AthenaDriver";
import { checkNonNullable } from "@cubejs-backend/shared";

export class HydrationStream extends stream.Readable {
  public constructor(
      private athena: AWS.Athena,
      private qid: AthenaQueryId
  ) {
    super();
  }

  public async hydrate() {
    let columnInfo;
    for (
      let results: AWS.GetQueryResultsCommandOutput | undefined = await this.athena.getQueryResults(this.qid);
      results;
      results = results.NextToken
        ? (await this.athena.getQueryResults({ ...this.qid, NextToken: results.NextToken }))
        : undefined
    ) {
      if (columnInfo === undefined) {
        columnInfo = results.ResultSet?.ResultSetMetadata?.ColumnInfo?.map(info => ({ Name: checkNonNullable('Name', info.Name) }));
      }

      const rows = results.ResultSet?.Rows ?? [];
      for (let i = 0; i < rows.length; i++) {
        this.push(rows[i]);
      }
    }
  }
}
