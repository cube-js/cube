/* eslint-disable no-restricted-syntax */
import stream, { TransformCallback } from 'stream';

export type HydrationMap = Record<string, any>;

export class HydrationStream extends stream.Transform {
  public constructor(meta: any) {
    super({
      objectMode: true,
      transform(row: any[], encoding: BufferEncoding, callback: TransformCallback) {
        for (const [index, value] of Object.entries(row)) {
          if (value !== null) {
            const metaForField = meta[index];
            if (metaForField.type.includes('DateTime')) {
              row[<any>index] = `${value.substring(0, 10)}T${value.substring(11, 22)}.000`;
            } else if (metaForField.type.includes('Date')) {
              row[<any>index] = `${value}T00:00:00.000`;
            } else if (metaForField.type.includes('Int')
              || metaForField.type.includes('Float')
              || metaForField.type.includes('Decimal')
            ) {
              // convert all numbers into strings
              row[<any>index] = `${value}`;
            }
          }
        }

        this.push(row);
        callback();
      }
    });
  }
}
