import stream, { TransformCallback } from 'stream';
import * as moment from 'moment';

//  ClickHouse returns DateTime as strings in format "YYYY-DD-MM HH:MM:SS"
//  cube.js expects them in format "YYYY-DD-MMTHH:MM:SS.000", so translate them based on the metadata returned
//
//  ClickHouse returns some number types as js numbers, others as js string, normalise them all to strings
export function transformRow(row: Record<string, any>, meta: any) {
  for (const [fieldName, value] of Object.entries(row)) {
    if (value !== null) {
      const metaForField = meta[fieldName];
      if (metaForField.type.includes('DateTime64')) {
        row[fieldName] = moment.utc(value).format(moment.HTML5_FMT.DATETIME_LOCAL_MS);
      } else if (metaForField.type.includes('DateTime')) {
        row[fieldName] = moment.utc(value).format(moment.HTML5_FMT.DATETIME_LOCAL_MS);
      } else if (metaForField.type.includes('Date')) {
        row[fieldName] = `${value}T00:00:00.000`;
      } else if (metaForField.type.includes('Int')
        || metaForField.type.includes('Float')
        || metaForField.type.includes('Decimal')
      ) {
        // convert all numbers into strings
        row[fieldName] = `${value}`;
      }
    }
  }
}

export class HydrationStream extends stream.Transform {
  public constructor(meta: any) {
    super({
      objectMode: true,
      transform(row: any[], encoding: BufferEncoding, callback: TransformCallback) {
        transformRow(row, meta);

        this.push(row);
        callback();
      }
    });
  }
}
