import Stream from 'pg-query-stream';
import type { FieldDef } from 'pg';
import { TableStructure } from '@cubejs-backend/base-driver';

export class QueryStream extends Stream {
  public fields(mapFieldsFn: (def: FieldDef[]) => TableStructure): Promise<TableStructure> {
    return new Promise((resolve, reject) => {
      const errorListener = (e: Error) => {
        reject(e);

        this.removeListener('error', errorListener);
      };

      this.on('error', errorListener);

      this.cursor.read(100, (err: Error, rows: any[], result: any) => {
        if (err) {
          /**
           * https://nodejs.org/api/stream.html#stream_errors_while_reading
           * This will populate error and change status to the stream
           *
           * stream._readableState.destroyed
           */
          this.destroy(err);
        } else {
          // eslint-disable-next-line no-restricted-syntax
          for (const row of rows) {
            this.push(row);
          }

          if (rows.length < 1) {
            this.push(null);
          }

          this.removeListener('error', errorListener);

          resolve(mapFieldsFn(result.fields));
        }
      });
    });
  }
}
