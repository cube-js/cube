import stream, { TransformCallback } from 'stream';
import R from 'ramda';

export class HydrationStream extends stream.Transform {
  public constructor() {
    super({
      objectMode: true,
      transform(row: any, encoding: BufferEncoding, callback: TransformCallback) {
        const transformed = R.map(
          (value) => {
            if (value && value.value && typeof value.value === 'string') {
              return value.value;
            }

            if (Buffer.isBuffer(value)) {
              return value.toString('base64');
            }

            return value;
          },
          row
        );

        this.push(transformed);

        callback();
      }
    });
  }
}
