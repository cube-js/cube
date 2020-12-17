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

    const handler = cachedHandler(async (req, res, next) => {
      reqPassed++;

      await createAsyncLock(125);

      res.status(200).json('heh');
    }, {
      lifetime: 250
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

    await createAsyncLock(125);

    expect(resPassed).toEqual(3);
    expect(reqPassed).toEqual(1);

    // cache will be expired
    await createAsyncLock(250);

    handler(req, res, next);

    expect(reqPassed).toEqual(2);
  });
});
