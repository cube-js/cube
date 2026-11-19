import stream, { pipeline, TransformCallback } from 'stream';
import { displayCLIWarning } from '@cubejs-backend/shared';

const THRESHOLD_LIMIT = 100_000;

export class LargeStreamWarning extends stream.Transform {
  public constructor(preAggregationName: string, onWarning: (msg: string) => void) {
    let count = 0;

    super({
      objectMode: true,
      transform(row: unknown, encoding: BufferEncoding, callback: TransformCallback) {
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

export function withLargeStreamWarning(
  rowStream: NodeJS.ReadableStream,
  preAggregationName: string,
  onWarning: (msg: string) => void
): LargeStreamWarning {
  const warning = new LargeStreamWarning(preAggregationName, onWarning);

  // Not pipe(): it doesn't forward source errors, so a mid-stream driver
  // error was left unhandled and crashed the process.
  pipeline(rowStream, warning, () => {
    // The error reaches the consumer through the returned stream
  });

  return warning;
}
