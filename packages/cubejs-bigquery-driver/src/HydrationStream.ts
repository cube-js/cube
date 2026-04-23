import stream, { TransformCallback } from 'stream';
import R from 'ramda';

import type { Big } from 'big.js';

// instanceof doesn't work with instance of probably due to deps resolving?
function isBig(value: unknown): value is Big {
  if (typeof value === 'object' && value !== null) {
    return value.constructor.name === 'Big';
  }

  return false;
}

export function transformRow(row: any) {
  return R.map(
    (value) => {
      if (value && value.value && typeof value.value === 'string') {
        return value.value;
      }

      if (typeof value === 'number') {
        if (Number.isFinite(value)) {
          return value.toString();
        }

        return value;
      }

      if (isBig(value)) {
        return value.toString();
      }

      if (Buffer.isBuffer(value)) {
        return value.toString('base64');
      }

      return value;
    },
    row
  );
}

export class HydrationStream extends stream.Transform {
  public constructor() {
    super({
      objectMode: true,
      transform(row: any, _encoding: BufferEncoding, callback: TransformCallback) {
        const transformed = transformRow(row);

        this.push(transformed);

        callback();
      }
    });
  }
}
