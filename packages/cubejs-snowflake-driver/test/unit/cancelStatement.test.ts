import {
  afterEach, beforeEach, describe, expect, test, vi
} from 'vitest';
import type { RowStatement } from 'snowflake-sdk';

import { CANCEL_ACK_TIMEOUT, CANCEL_RETRY_INTERVAL, SnowflakeDriver } from '../../src/SnowflakeDriver';

// No Snowflake connection here: the point is what the driver does when the SDK
// never answers, which no live server reproduces on demand.
class TestSnowflakeDriver extends SnowflakeDriver {
  public logged: [string, any][] = [];

  public constructor() {
    super({});
    this.setLogger((msg, params) => this.logged.push([msg, params]));
  }

  public cancelStatementForTest(stmt: RowStatement, isStatementSettled = () => false): Promise<void> {
    return this.cancelStatement(stmt, isStatementSettled);
  }
}

// A statement wedged the way a black-holed socket wedges one: `cancel()` takes
// the callback and only calls it if the test says so.
const wedgedStatement = (onCancel?: (cb: (err: any) => void) => void) => <RowStatement>(<unknown>{
  cancel: (cb: (err: any) => void) => onCancel?.(cb),
  getQueryId: () => 'query-id',
});

describe('cancelStatement()', () => {
  // Fake timers keep the real CANCEL_ACK_TIMEOUT out of the wall clock and make
  // the bound exact: nothing but an advance past it can end the wait, and
  // nothing but the SDK's callback can end the others.
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  test('gives up waiting when the SDK never acknowledges, and ignores a late one', async () => {
    const driver = new TestSnowflakeDriver();

    let acknowledge: (() => void) | undefined;
    let settled = false;
    const cancelling = driver
      .cancelStatementForTest(
        wedgedStatement((cb) => { acknowledge = () => cb(new Error('too late')); }),
      )
      .then(() => { settled = true; });

    await vi.advanceTimersByTimeAsync(CANCEL_ACK_TIMEOUT - 1);
    expect(settled).toBe(false);

    await vi.advanceTimersByTimeAsync(1);
    await cancelling;
    expect(settled).toBe(true);

    // The wait has expired; a late callback must neither resolve a second time
    // nor report an error nobody can act on any more.
    acknowledge?.();

    expect(driver.logged).toEqual([
      ['Snowflake statement cancel timeout', { queryId: 'query-id' }],
    ]);
  });

  test('reports an acknowledged failure without waiting out the bound', async () => {
    const driver = new TestSnowflakeDriver();

    // No timer is advanced, so resolving can only have come from the callback.
    await driver.cancelStatementForTest(
      wedgedStatement((cb) => cb(new Error('cancel refused'))),
    );

    expect(driver.logged).toEqual([
      [
        'Snowflake statement cancel error',
        { queryId: 'query-id', error: expect.stringContaining('cancel refused') },
      ],
    ]);
    // And the bound it no longer needs is gone rather than left to fire later.
    expect(vi.getTimerCount()).toBe(0);
  });

  test('reports a cancel that throws instead of calling back', async () => {
    const driver = new TestSnowflakeDriver();

    await driver.cancelStatementForTest(
      wedgedStatement(() => { throw new Error('cancel threw'); }),
    );

    expect(driver.logged).toEqual([
      [
        'Snowflake statement cancel error',
        { queryId: 'query-id', error: expect.stringContaining('cancel threw') },
      ],
    ]);
    expect(vi.getTimerCount()).toBe(0);
  });

  test('resolves when the SDK acknowledges a successful cancel', async () => {
    const driver = new TestSnowflakeDriver();

    await driver.cancelStatementForTest(wedgedStatement((cb) => cb(undefined)));

    expect(driver.logged).toEqual([]);
    expect(vi.getTimerCount()).toBe(0);
  });

  // What Snowflake answers when the abort outruns the statement it targets.
  const notExecuting = () => Object.assign(new Error('Identified SQL statement is not currently executing.'), {
    code: '000605',
  });

  test('repeats an abort that outran its statement until Snowflake accepts it', async () => {
    const driver = new TestSnowflakeDriver();

    const answers = [notExecuting(), notExecuting(), undefined];
    let attempts = 0;
    let settled = false;
    const cancelling = driver
      .cancelStatementForTest(wedgedStatement((cb) => cb(answers[attempts++])))
      .then(() => { settled = true; });

    expect(attempts).toBe(1);

    await vi.advanceTimersByTimeAsync(CANCEL_RETRY_INTERVAL);
    expect(attempts).toBe(2);
    expect(settled).toBe(false);

    await vi.advanceTimersByTimeAsync(CANCEL_RETRY_INTERVAL);
    await cancelling;
    expect(attempts).toBe(3);

    expect(driver.logged).toEqual([]);
    expect(vi.getTimerCount()).toBe(0);
  });

  test('stops repeating once the statement finished on its own', async () => {
    const driver = new TestSnowflakeDriver();

    let attempts = 0;
    let statementSettled = false;
    const cancelling = driver.cancelStatementForTest(
      wedgedStatement((cb) => { attempts++; cb(notExecuting()); }),
      () => statementSettled,
    );

    await vi.advanceTimersByTimeAsync(CANCEL_RETRY_INTERVAL);
    expect(attempts).toBe(2);

    statementSettled = true;
    await vi.advanceTimersByTimeAsync(CANCEL_RETRY_INTERVAL);
    await cancelling;

    expect(attempts).toBe(3);
    expect(driver.logged).toEqual([]);
    expect(vi.getTimerCount()).toBe(0);
  });

  test('gives up repeating at the bound', async () => {
    const driver = new TestSnowflakeDriver();

    const cancelling = driver.cancelStatementForTest(wedgedStatement((cb) => cb(notExecuting())));

    await vi.advanceTimersByTimeAsync(CANCEL_ACK_TIMEOUT);
    await cancelling;

    expect(driver.logged).toEqual([
      ['Snowflake statement cancel timeout', { queryId: 'query-id' }],
    ]);
    expect(vi.getTimerCount()).toBe(0);
  });
});
