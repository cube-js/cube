import stream, { TransformCallback } from 'stream';
import { displayCLIWarning } from '@cubejs-backend/shared';

const THREASHOLD_LIMIT = 100_000;

export class LargeStreamWarning extends stream.Transform {
  public constructor(preAggregationName: string) {
    let count = 0;

    super({
      objectMode: true,
      transform(row: any, encoding: BufferEncoding, callback: TransformCallback) {
        count++;

        if (count === THREASHOLD_LIMIT) {
          displayCLIWarning(
            `The pre-aggregation "${preAggregationName}" has more then ${THREASHOLD_LIMIT} rows. Consider exporting this pre-aggregation.`
          );
        }

        this.push(row);
        callback();
      }
    });
  }
}
