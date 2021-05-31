import stream, { TransformCallback } from 'stream';
import R from 'ramda';

export class HydrationStream extends stream.Transform {
  public constructor() {
    super({
      objectMode: true,
      transform(row: any, encoding: BufferEncoding, callback: TransformCallback) {
        const transformed = R.map(value => (value && value.value && typeof value.value === 'string' ? value.value : value), row);

        this.push(transformed);

        callback();
      }
    });
  }
}
