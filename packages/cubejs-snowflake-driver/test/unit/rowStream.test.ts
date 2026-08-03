import { describe, expect, test } from 'vitest';
import { streamToArray } from '@cubejs-backend/shared';
import { Readable } from 'stream';

import { SnowflakeDriver } from '../../src/SnowflakeDriver';
import type { HydrationMap } from '../../src/HydrationStream';

const pause = (ms: number) => new Promise((resolve) => { setTimeout(resolve, ms); });

// Also connectionless: the point is what happens when the SDK's row stream
// fails, which a live server does not do on request.
describe('stream() row stream wiring', () => {
  class TestSnowflakeDriver extends SnowflakeDriver {
    public constructor() {
      super({});
    }

    public buildRowStreamForTest(sourceStream: Readable, hydrationMap: HydrationMap): Readable {
      return this.buildRowStream(sourceStream, hydrationMap);
    }
  }

  // The SDK's RowStream reports failures with a bare emit('error') instead of
  // destroy(err), so an unlistened 'error' throws right at the emit call.
  class FakeRowStream extends Readable {
    public constructor() {
      super({ objectMode: true });
    }

    public _read(): void {
      // rows are pushed by the test
    }

    public fail(err: Error): void {
      this.emit('error', err);
    }
  }

  const hydrationMap: HydrationMap = { n: (value: string) => `${value}!` };

  test('hands back the source itself when there is nothing to hydrate', () => {
    const driver = new TestSnowflakeDriver();
    const sourceStream = new FakeRowStream();

    expect(driver.buildRowStreamForTest(sourceStream, {})).toBe(sourceStream);
  });

  test('surfaces a source failure to the consumer of the hydrated stream', async () => {
    const driver = new TestSnowflakeDriver();
    const sourceStream = new FakeRowStream();
    const rowStream = driver.buildRowStreamForTest(sourceStream, hydrationMap);

    expect(rowStream).not.toBe(sourceStream);

    const rows = streamToArray(rowStream as any);
    sourceStream.push({ n: '1' });
    // A chunk download failing mid-stream: it must reach the consumer rather
    // than escape as an uncaught exception.
    sourceStream.fail(new Error('chunk download failed'));

    await expect(rows).rejects.toThrow('chunk download failed');
  });

  test('stays quiet when the source fails after release() tore it down', async () => {
    const driver = new TestSnowflakeDriver();
    const sourceStream = new FakeRowStream();
    const rowStream = driver.buildRowStreamForTest(sourceStream, hydrationMap);

    const seen: unknown[] = [];
    rowStream.on('error', (err) => seen.push(err));

    // What release() does when the consumer bailed out early.
    sourceStream.destroy();
    rowStream.destroy();

    // An in-flight chunk can still fail after that; nobody is left to act on it.
    sourceStream.fail(new Error('too late'));
    await pause(10);

    expect(seen).toEqual([]);
  });
});
