import stream, { TransformCallback } from 'stream';

export class HydrationStream extends stream.Transform {
  public constructor() {
    super({
      objectMode: true,
      transform(row: any, encoding: BufferEncoding, callback: TransformCallback) {
        for (const [field, value] of Object.entries(row)) {
          if (typeof value === 'number') {
            row[field] = value.toString();
          } else if (Object.prototype.toString.call(value) === '[object Date]') {
            row[field] = (value as any).toISOString();
          }
        }

        this.push(row);
        callback();
      }
    });
  }
}
