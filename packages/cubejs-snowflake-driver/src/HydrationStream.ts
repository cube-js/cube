import stream, { TransformCallback } from 'stream';

export type HydrationMap = Record<string, any>;

export class HydrationStream extends stream.Transform {
  public constructor(hydrationMap: HydrationMap) {
    super({
      objectMode: true,
      transform(row: any, encoding: BufferEncoding, callback: TransformCallback) {
        // eslint-disable-next-line no-restricted-syntax
        for (const [field, toValue] of Object.entries(hydrationMap)) {
          if (row.hasOwnProperty(field)) {
            row[field] = toValue(row[field]);
          }
        }

        this.push(row);
        callback();
      }
    });
  }
}
