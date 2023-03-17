import stream, { TransformCallback } from 'stream';
import { displayCLIWarning } from '@cubejs-backend/shared';

const THRESHOLD_LIMIT = 100_000;

export class LargeStreamWarning extends stream.Transform {
  public constructor(preAggregationName: string, onWarning: (msg: string) => void) {
    let count = 0;

    super({
      objectMode: true,
      transform(row: any, encoding: BufferEncoding, callback: TransformCallback) {
        count++;

        if (count === THRESHOLD_LIMIT) {
          const msg = `The pre-aggregation "${preAggregationName}" has more than ${THRESHOLD_LIMIT} rows. Please consider using an export bucket.`;
          displayCLIWarning(msg);
          onWarning(msg);
        }

        this.push(row);
        callback();
      }
    });
  }
}
