import { Readable } from 'stream';
import { ColumnarResponse, COLUMNAR_RESPONSE_TYPE } from '../../src';

async function collect(members: string[], batchSize: number, rows: unknown[][]) {
  const source = Readable.from(rows, { objectMode: true });
  const columnar = new ColumnarResponse({ members, batchSize });
  const batches: any[] = [];
  source.pipe(columnar);
  for await (const batch of columnar) {
    batches.push(batch);
  }
  return batches;
}

describe('ColumnarResponse', () => {
  const members = ['a', 'b'];

  test('emits a batch at every batchSize boundary and a final partial batch', async () => {
    const rows = [
      [1, 'x'],
      [2, 'y'],
      [3, 'z'],
      [4, 'w'],
      [5, 'v'],
    ];

    const batches = await collect(members, 2, rows);

    expect(batches).toHaveLength(3); // 2 + 2 + 1
    batches.forEach((batch) => {
      expect(batch.$type).toBe(COLUMNAR_RESPONSE_TYPE);
      expect(batch.members).toEqual(members);
    });

    expect(batches[0].columns).toEqual([[1, 2], ['x', 'y']]);
    expect(batches[1].columns).toEqual([[3, 4], ['z', 'w']]);
    expect(batches[2].columns).toEqual([[5], ['v']]);
  });

  test('transposes positional array rows into columns', async () => {
    const rows = [
      [1, 'x'],
      [2, 'y'],
    ];

    const batches = await collect(members, 100, rows);

    expect(batches).toHaveLength(1);
    expect(batches[0]).toEqual({
      $type: COLUMNAR_RESPONSE_TYPE,
      members,
      columns: [[1, 2], ['x', 'y']],
    });
  });

  test('does not emit anything for an empty stream', async () => {
    const batches = await collect(members, 2, []);
    expect(batches).toHaveLength(0);
  });

  test('reuses fresh column arrays per batch (no shared references)', async () => {
    const rows = [
      [1, 'x'],
      [2, 'y'],
      [3, 'z'],
      [4, 'w'],
    ];

    const batches = await collect(members, 2, rows);

    expect(batches).toHaveLength(2);
    // First batch's columns must not be mutated by the second batch.
    expect(batches[0].columns).toEqual([[1, 2], ['x', 'y']]);
    expect(batches[1].columns).toEqual([[3, 4], ['z', 'w']]);
    expect(batches[0].columns).not.toBe(batches[1].columns);
  });
});
