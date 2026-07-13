import * as stream from 'stream';

/**
 * Marker set on every batch object emitted by `ColumnarResponse`. Downstream consumers
 * (the query orchestrator's `QueryStream` and `cubejs-backend-native`) key on this to tell
 * a columnar batch apart from a plain row object.
 */
export const COLUMNAR_RESPONSE_TYPE = 'ColumnarResponse';

/**
 * A single columnar batch as it flows through the object-mode stream.
 * `members[j]` names `columns[j]`; every `columns[j]` has the same length (the batch's
 * row count). Matches the `{ members, columns }` shape decoded on the Rust side
 * (`JsonColumnarValueObject`).
 */
export interface ColumnarBatch {
  $type: typeof COLUMNAR_RESPONSE_TYPE;
  members: string[];
  columns: any[][];
}

/**
 * Object-mode Transform that pivots position-indexed array rows (e.g. pg `rowMode: 'array'`)
 * into columnar batches. Buffering by column avoids allocating a per-row object per row and
 * moves the row→columnar pivot out of `cubejs-backend-native`, so the SQL-API streaming path
 * can hand columnar batches straight to Rust.
 *
 * Input: `unknown[]` rows whose cells are ordered to match `members`.
 * Output: `ColumnarBatch` objects, one every `batchSize` rows plus a final partial batch.
 */
export class ColumnarResponse extends stream.Transform {
  private readonly members: string[];

  private readonly batchSize: number;

  private columns: any[][];

  private count = 0;

  public constructor({ members, batchSize }: { members: string[]; batchSize: number }) {
    super({ objectMode: true });
    this.members = members;
    this.batchSize = batchSize;
    this.columns = members.map(() => []);
  }

  public _transform(row: unknown[], _encoding: BufferEncoding, callback: stream.TransformCallback) {
    for (let j = 0; j < this.members.length; j++) {
      this.columns[j].push(row[j]);
    }
    this.count++;
    if (this.count >= this.batchSize) {
      this.flushBatch();
    }
    callback();
  }

  public _flush(callback: stream.TransformCallback) {
    if (this.count > 0) {
      this.flushBatch();
    }
    callback();
  }

  private flushBatch() {
    const batch: ColumnarBatch = {
      $type: COLUMNAR_RESPONSE_TYPE,
      members: this.members,
      columns: this.columns,
    };
    this.push(batch);
    this.columns = this.members.map(() => []);
    this.count = 0;
  }
}
