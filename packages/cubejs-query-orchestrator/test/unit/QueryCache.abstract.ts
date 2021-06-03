import crypto from 'crypto';
import { createCancelablePromise, pausePromise } from '@cubejs-backend/shared';

import { QueryCache } from '../../src';

export const QueryCacheTest = (name: string, options?: any) => {
  describe(`QueryQueue${name}`, () => {
    const cache = new QueryCache(
      crypto.randomBytes(16).toString('hex'),
      jest.fn(() => {
        throw new Error('It`s not implemented mock...');
      }),
      jest.fn(),
      options,
    );

    afterAll(async () => {
      await cache.cleanup();
    });

    it('withLock', async () => {
      const RANDOM_KEY_CACHE = crypto.randomBytes(16).toString('hex');

      const testLock = async () => {
        let started = 0;
        let finished = 0;

        const doLock = (sleep: number) => cache.withLock(
          RANDOM_KEY_CACHE,
          60 * 10,
          async () => {
            started++;

            await pausePromise(sleep);

            finished++;
          },
        );

        const locks: Promise<boolean>[] = [
          doLock(1000)
        ];

        await pausePromise(25);

        locks.push(doLock(1000));
        locks.push(doLock(1000));

        const results = await Promise.all(locks);
        expect(results[0]).toEqual(true);
        expect(results[1]).toEqual(false);
        expect(results[2]).toEqual(false);

        expect(started).toEqual(1);
        expect(finished).toEqual(1);
      };

      await testLock();

      await pausePromise(500);

      await testLock();
    });

    it('withLock + cancel (test free of lock + cancel inheritance)', async () => {
      const RANDOM_KEY_CACHE = crypto.randomBytes(16).toString('hex');

      const lockPromise = cache.withLock(
        RANDOM_KEY_CACHE,
        60 * 10,
        () => createCancelablePromise(async (tkn) => {
          await tkn.with(
            // This timeout is useful to test that withLock.cancel use callback as tkn.with
            // If doesn't use it, test will fail with timeout
            pausePromise(60 * 60 * 1000)
          );
        }),
      );

      await lockPromise.cancel(true);
      await lockPromise;

      let callbackWasExecuted = false;

      // withLock return boolean, where true success execution & lock
      const statusOfResolve = await cache.withLock(
        RANDOM_KEY_CACHE,
        60 * 10,
        async () => {
          callbackWasExecuted = true;
        },
      );

      expect(statusOfResolve).toEqual(true);
      expect(callbackWasExecuted).toEqual(true);
    });
  });
};
