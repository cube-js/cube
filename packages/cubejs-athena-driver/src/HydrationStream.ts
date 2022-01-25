import * as AWS from "@aws-sdk/client-athena";
import * as stream from 'stream';
import { checkNonNullable } from "@cubejs-backend/shared";
import { AthenaQueryId } from "./AthenaDriver";

async function* hydrationGenerator(athena: AWS.Athena, qid: AthenaQueryId) {
  let columnInfo: { Name: string }[] | undefined;
  for (
    let results: AWS.GetQueryResultsCommandOutput | undefined = await athena.getQueryResults(qid);
    results;
    results = results.NextToken
      ? (await athena.getQueryResults({ ...qid, NextToken: results.NextToken }))
      : undefined
  ) {
    if (!columnInfo) {
      columnInfo = /SHOW COLUMNS/.test(query) // Fix for getColumns method
        ? [{ Name: 'column' }]
        : results.ResultSet?.ResultSetMetadata?.ColumnInfo?.map(info => ({ Name: checkNonNullable('Name', info.Name) }));
    }

    const rows = results.ResultSet?.Rows ?? [];
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      const fields: Record<string, any> = {};
      checkNonNullable('ColumnInfo', columnInfo)
        .forEach((c, j) => {
          fields[c.Name] = row.Data?.[j].VarCharValue;
        });
      yield fields;
    }
  }
}

export function hydrationStream(athena: AWS.Athena, qid: AthenaQueryId) {
  return stream.Readable.from(hydrationGenerator(athena, qid));
}
