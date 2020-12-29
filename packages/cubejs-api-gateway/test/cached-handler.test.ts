import { cachedHandler } from '../src/cached-handler';

const createAsyncLock = (timeout) => new Promise(
  (resolve) => {
    setTimeout(
      resolve,
      timeout
    );
  }
);

describe('cachedHandler', () => {
  test('works', async () => {
    let reqPassed = 0;
    let resPassed = 0;

    const TEST_TIMEOUT = 25;
    const TEST_LIFETIME = 100;

    const handler = cachedHandler(async (req, res) => {
      reqPassed++;

      await createAsyncLock(TEST_TIMEOUT);

      res.status(200).json('heh');
    }, {
      lifetime: TEST_LIFETIME
    });

    const req: any = {};

    const res: any = {
      status: (code) => {
        expect(code).toEqual(200);

        return res;
      },
      json: (content) => {
        resPassed++;

        expect(content).toEqual('heh');

        return res;
      }
    };

    const next = () => {
      // nothing to do
    };

    handler(req, res, next);

    expect(reqPassed).toEqual(1);
    expect(resPassed).toEqual(0);

    handler(req, res, next);
    handler(req, res, next);

    expect(resPassed).toEqual(0);

    await createAsyncLock(TEST_TIMEOUT + 10);

    expect(resPassed).toEqual(3);
    expect(reqPassed).toEqual(1);

    // cache will be expired
    await createAsyncLock(TEST_LIFETIME);

    handler(req, res, next);

    expect(reqPassed).toEqual(2);
  });
});
