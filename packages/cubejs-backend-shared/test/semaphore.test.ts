import { pausePromise, Semaphore } from '../src';

describe('Semaphore', () => {
  it('sem use queue', async () => {
    const sem = new Semaphore(1);
    await sem.acquire();

    const next1 = sem.acquire();
    const next2 = sem.acquire();
    const next3 = sem.acquire();
    sem.release();

    await next1;
    sem.release();

    await next2;
    sem.release();

    await next3;
    sem.release();
  });

  it('execute', async () => {
    const semaphore = new Semaphore(1);

    const executed: number[] = [];
    let resolved = 0;

    const callbackTick = (id: number) => async () => {
      executed.push(id);
      resolved++;
    };

    await semaphore.execute<void>(callbackTick(1));
    expect(resolved).toEqual(1);

    await semaphore.execute<void>(callbackTick(2));
    await semaphore.execute<void>(callbackTick(3));
    await semaphore.execute<void>(callbackTick(4));

    expect(resolved).toEqual(4);
    expect(executed).toEqual([
      1,
      2,
      3,
      4
    ]);
  });

  async function concurrencyTest(expectedConcurrency: number) {
    const semaphore = new Semaphore(expectedConcurrency);

    let concurrency = 0;
    let maxConcurrency = 0;

    const promises = [];

    const intervalId = setInterval(() => {
      maxConcurrency = Math.max(concurrency, maxConcurrency);
    }, 10);

    for (let i = 0; i < (25 * expectedConcurrency); i++) {
      // eslint-disable-next-line no-loop-func
      promises.push(semaphore.execute(async () => {
        concurrency++;

        await pausePromise(25);

        concurrency--;
      }));
    }

    await Promise.all(promises);
    clearInterval(intervalId);

    expect(maxConcurrency).toEqual(expectedConcurrency);
  }

  it('concurrency(1)', async () => {
    jest.setTimeout(5 * 1000);
    await concurrencyTest(1);
  });

  it('concurrency(2)', async () => {
    jest.setTimeout(5 * 1000);
    await concurrencyTest(2);
  });

  it('concurrency(5)', async () => {
    jest.setTimeout(5 * 1000);
    await concurrencyTest(5);
  });
});
