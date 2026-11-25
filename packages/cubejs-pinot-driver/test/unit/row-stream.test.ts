import { PinotRow, PinotRowStream } from '../../src/PinotRowStream';

// Reads from a microtask queued during `push()`, the way an `await`-driven consumer does.
class MicrotaskConsumedStream extends PinotRowStream {
  public readonly consumed: PinotRow[] = [];

  public override push(row: PinotRow | null): boolean {
    const wantsMore = super.push(row);

    queueMicrotask(() => {
      const next = this.read();
      if (next !== null) {
        this.consumed.push(next);
      }
    });

    return wantsMore;
  }
}

describe('PinotRowStream', () => {
  it('does not stall when a read arrives right after the last row of a page', async () => {
    const pages: (PinotRow[] | null)[] = [[{ id: 1 }], [{ id: 2 }], [{ id: 3 }], null];
    const stream = new MicrotaskConsumedStream(async () => <PinotRow[] | null> pages.shift(), 1);

    const ended = new Promise((resolve, reject) => {
      stream.once('end', () => resolve('end'));
      stream.once('error', reject);
    });
    const stalled = new Promise((resolve) => {
      setTimeout(() => resolve('stalled'), 500).unref();
    });

    stream.read();

    expect(await Promise.race([ended, stalled])).toBe('end');
    expect(stream.consumed).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }]);
  });

  it('stops pushing a page once the stream is destroyed', async () => {
    const rows = [{ id: 1 }, { id: 2 }, { id: 3 }];
    let pushed = 0;

    class CountingStream extends PinotRowStream {
      public override push(row: PinotRow | null): boolean {
        pushed += 1;
        return super.push(row);
      }
    }

    const pages: (PinotRow[] | null)[] = [rows, null];
    const stream = new CountingStream(async () => <PinotRow[] | null> pages.shift(), 10);

    await new Promise((resolve) => {
      stream.once('close', resolve);
      stream.once('data', () => stream.destroy());
    });

    expect(pushed).toBe(1);
  });
});
