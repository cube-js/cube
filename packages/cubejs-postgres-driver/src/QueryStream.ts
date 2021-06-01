import Stream from 'pg-query-stream';
import type { FieldDef } from 'pg';

export class QueryStream extends Stream {
  public fields(): Promise<FieldDef[]> {
    return new Promise((resolve, reject) => {
      this.cursor.read(100, (err: Error, rows: any[], result: any) => {
        if (err) {
          // https://nodejs.org/api/stream.html#stream_errors_while_reading
          this.destroy(err);
          reject(err);
        } else {
          // eslint-disable-next-line no-restricted-syntax
          for (const row of rows) {
            this.push(row);
          }

          if (rows.length < 1) {
            this.push(null);
          }

          resolve(result.fields);
        }
      });
    });
  }
}
