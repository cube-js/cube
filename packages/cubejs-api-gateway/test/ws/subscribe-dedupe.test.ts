import crypto from 'crypto';
import { testLogger } from '@cubejs-backend/shared';
import { ResultWrapper } from '@cubejs-backend/native';

import { ApiGateway } from '../../src';
import type { SQLServer } from '../../src/sql-server';
import { compilerApi, AdapterApiMock, DataSourceStorageMock } from '../mocks';

class TestApiGateway extends ApiGateway {
  // The SQL server needs the native binary, which subscribe() doesn't touch.
  protected createSQLServerInstance(): SQLServer {
    return {} as unknown as SQLServer;
  }

  public subscriptionResultHash(result: { message: any, opts: any }): string {
    return super.subscriptionResultHash(result);
  }
}

describe('ApiGateway.subscribe result dedupe', () => {
  let gw: TestApiGateway;
  let state: any;
  let res: jest.Mock;

  const subscriptionState = async () => state;
  const subscribe = async (s: any) => {
    state = s;
  };

  const tick = () => gw.subscribe({
    query: {},
    context: {},
    res,
    subscribe,
    subscriptionState,
    queryType: 'regularQuery' as any,
    apiType: 'ws' as any,
  });

  const stubLoad = (message: any, opts?: any) => {
    jest.spyOn(gw, 'load').mockImplementation(async ({ res: loadRes }: any) => {
      await loadRes(message, opts);
    });
  };

  beforeEach(() => {
    state = undefined;
    res = jest.fn();
    gw = new TestApiGateway('secret', compilerApi, async () => new AdapterApiMock(), testLogger, {
      standalone: true,
      dataSourceStorage: new DataSourceStorageMock(),
      basePath: '/cubejs-api',
      refreshScheduler: {},
    });
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  test('sends the first result and stores only its hash', async () => {
    stubLoad(new ResultWrapper([{ a: 1 }]));
    await tick();

    expect(res).toHaveBeenCalledTimes(1);
    expect(Object.keys(state)).toEqual(['resultHash']);
    expect(state.resultHash).toMatch(/^[0-9a-f]{40}$/);
  });

  test('hash equals sha1 of the previous comparison string', () => {
    const r = { message: new ResultWrapper([{ a: 1 }]), opts: undefined };

    expect(gw.subscriptionResultHash(r))
      .toEqual(crypto.createHash('sha1').update(JSON.stringify(r)).digest('hex'));
  });

  test('does not resend an unchanged result', async () => {
    stubLoad(new ResultWrapper([{ a: 1 }]));
    await tick();
    stubLoad(new ResultWrapper([{ a: 1 }]));
    await tick();

    expect(res).toHaveBeenCalledTimes(1);
  });

  test('resends when rows change', async () => {
    stubLoad(new ResultWrapper([{ a: 1 }]));
    await tick();
    const firstHash = state.resultHash;
    stubLoad(new ResultWrapper([{ a: 2 }]));
    await tick();

    expect(res).toHaveBeenCalledTimes(2);
    expect(state.resultHash).not.toEqual(firstHash);
  });

  test('resends when only opts.status changes', async () => {
    stubLoad(new ResultWrapper([{ a: 1 }]), { status: 200 });
    await tick();
    stubLoad(new ResultWrapper([{ a: 1 }]), { status: 202 });
    await tick();

    expect(res).toHaveBeenCalledTimes(2);
  });

  test('always sends errors and does not store a hash for them', async () => {
    stubLoad({ error: 'boom' }, { status: 400 });
    await tick();
    await tick();

    expect(res).toHaveBeenCalledTimes(2);
    expect(state.resultHash).toBeUndefined();
  });

  test('sends the first good result after an error, even if it equals the pre-error result', async () => {
    stubLoad(new ResultWrapper([{ a: 1 }]));
    await tick();
    stubLoad({ error: 'boom' }, { status: 400 });
    await tick();
    stubLoad(new ResultWrapper([{ a: 1 }]));
    await tick();

    expect(res).toHaveBeenCalledTimes(3);
  });
});
