import {
  createCancelablePromise,
  createCancelableInterval,
  pausePromise,
  retryWithTimeout,
  withTimeout,
  withTimeoutRace,
} from '../src';

test('createCancelablePromise', async () => {
  let canceled = false;

  const promise = createCancelablePromise(async (token) => {
    await pausePromise(250);

    if (token.isCanceled()) {
      canceled = true;

      return;
    }

    await pausePromise(250);
  });
  await promise.cancel();

  await pausePromise(250);

  expect(canceled).toBe(true);
});

test('createCancelablePromise(defer async)', async () => {
  let finished = false;
  let canceled = false;

  const promise = createCancelablePromise(async (token) => {
    token.defer(async () => {
      canceled = true;
    });

    await pausePromise(250);

    finished = true;
  });
  await promise.cancel();

  expect(canceled).toBe(true);
  expect(finished).toBe(true);
});

test('createCancelablePromise(defer async + with)', async () => {
  let finished = false;
  let canceled = false;

  const promise = createCancelablePromise(async (token) => {
    token.defer(async () => {
      canceled = true;
    });

    // This pause promise will be canceled by resolving
    token.with(pausePromise(10000 * 1000));

    finished = true;
  });
  await promise.cancel();

  expect(canceled).toBe(true);
  expect(finished).toBe(true);
});

test('createCancelableInterval(handle too fast execution)', async () => {
  let started = 0;
  let finished = 0;

  const interval = createCancelableInterval(async (token) => {
    started++;

    await pausePromise(25);

    finished++;
  }, 10);

  await pausePromise(25 * 2 + 5);
  await interval.cancel(true);

  expect(started).toEqual(2);
  expect(finished).toEqual(2);
});

test('createCancelableInterval(simple interval)', async () => {
  let started = 0;
  let finished = 0;
  let canceled = false;

  const interval = createCancelableInterval(async (token) => {
    started++;

    await pausePromise(25);

    if (token.isCanceled()) {
      console.log('canceling');

      canceled = true;

      return;
    }

    await pausePromise(25);

    finished++;
  }, 100);

  await pausePromise(100 + 25 + 25 + 10);

  expect(started).toEqual(1);
  expect(finished).toEqual(1);

  await pausePromise(50);

  await interval.cancel();

  expect(canceled).toEqual(true);
  expect(started).toEqual(2);
  expect(finished).toEqual(1);
});

test('createCancelableInterval(cancel should wait latest execution)', async () => {
  let started = 0;
  let finished = 0;

  const interval = createCancelableInterval(async (token) => {
    started++;

    await pausePromise(250);

    finished++;
  }, 100);

  await pausePromise(100);

  await interval.cancel();

  expect(started).toEqual(1);
  expect(finished).toEqual(1);
});

test('withTimeoutRace(ok)', async () => {
  let canceled = false;

  const result = await withTimeoutRace(
    createCancelablePromise(async (token) => {
      token.defer(async () => {
        canceled = true;
      });

      return 256;
    }),
    250
  );

  expect(result).toEqual(256);
  expect(canceled).toEqual(false);
});

test('withTimeoutRace(timeout)', async () => {
  let started = false;
  let canceled = false;
  let finished = false;
  let throwed = false;

  try {
    await withTimeoutRace(
      createCancelablePromise(async (token) => {
        started = true;

        token.defer(async () => {
          canceled = true;
        });

        await pausePromise(10000);

        finished = true;
      }),
      250
    );
  } catch (e) {
    throwed = true;
    expect(e.message).toEqual('Timeout reached after 250ms');
  }

  expect(throwed).toEqual(true);
  expect(started).toEqual(true);
  expect(canceled).toEqual(true);
  expect(finished).toEqual(false);
});

test('retryWithTimeout', async () => {
  let iterations = 0;

  const result = await retryWithTimeout(
    async (token) => {
      iterations++;

      if (iterations === 10) {
        return 256;
      }

      return null;
    },
    { timeout: 1000, intervalPause: () => 10 }
  );

  expect(result).toEqual(256);
  expect(iterations).toEqual(10);
});
