import { TestBed } from '@angular/core/testing';
import { firstValueFrom, BehaviorSubject, Subject } from 'rxjs';
import { Meta, ResultSet, SqlQuery } from '@cubejs-client/core';

import { CubeClient } from '../src/client';
import { CubeClientModule } from '../src/module';

import { metaResponse } from './meta-fixture';

const loadResponse = {
  queryType: 'regularQuery',
  pivotQuery: { measures: ['Orders.count'], dimensions: [], queryType: 'regularQuery' },
  results: [
    {
      query: { measures: ['Orders.count'], dimensions: [] },
      data: [{ 'Orders.count': '10' }],
      annotation: {
        measures: { 'Orders.count': { title: 'Orders Count', type: 'number' } },
        dimensions: {},
        segments: {},
        timeDimensions: {},
      },
    },
  ],
};

const responses: Record<string, any> = {
  load: loadResponse,
  sql: { sql: { sql: ['SELECT 1', []] } },
  'dry-run': { queryType: 'regularQuery', normalizedQueries: [] },
  meta: metaResponse,
};

const noop = () => undefined;

class StubTransport {
  public readonly calls: string[] = [];

  public request(method: string, _params: any) {
    this.calls.push(method);

    return {
      subscribe: (cb: any) => Promise.resolve(
        cb(
          { status: 200, text: async () => JSON.stringify(responses[method]) },
          noop
        )
      ),
    };
  }
}

function setup(config: any) {
  TestBed.configureTestingModule({
    imports: [CubeClientModule.forRoot(config)],
  });

  return TestBed.inject(CubeClient);
}

describe('CubeClient requests', () => {
  let transport: StubTransport;

  beforeEach(() => {
    transport = new StubTransport();
  });

  afterEach(() => {
    TestBed.resetTestingModule();
  });

  describe.each([
    ['a plain config object', (t: StubTransport) => ({ token: 'token', options: { transport: t } })],
    [
      'a BehaviorSubject config',
      (t: StubTransport) => new BehaviorSubject({ token: 'token', options: { transport: t } }),
    ],
  ])('with %s', (_name, makeConfig) => {
    test('load resolves a ResultSet', async () => {
      const client = setup(makeConfig(transport));

      const resultSet = await firstValueFrom(client.load({ measures: ['Orders.count'] }));

      expect(resultSet).toBeInstanceOf(ResultSet);
      expect(resultSet.rawData()).toEqual([{ 'Orders.count': '10' }]);
      expect(transport.calls).toEqual(['load']);
    });

    test('sql resolves a SqlQuery', async () => {
      const client = setup(makeConfig(transport));

      const sqlQuery = await firstValueFrom(client.sql({ measures: ['Orders.count'] }));

      expect(sqlQuery).toBeInstanceOf(SqlQuery);
      expect(sqlQuery.sql()).toBe('SELECT 1');
      expect(transport.calls).toEqual(['sql']);
    });

    test('dryRun resolves the response', async () => {
      const client = setup(makeConfig(transport));

      await expect(
        firstValueFrom(client.dryRun({ measures: ['Orders.count'] }))
      ).resolves.toEqual(responses['dry-run']);
      expect(transport.calls).toEqual(['dry-run']);
    });

    test('meta resolves a Meta', async () => {
      const client = setup(makeConfig(transport));

      const meta = await firstValueFrom(client.meta());

      expect(meta).toBeInstanceOf(Meta);
      expect(meta.cubes.map((cube) => cube.name)).toEqual(['Orders']);
      expect(transport.calls).toEqual(['meta']);
    });

    test('watch emits a ResultSet per query emission', async () => {
      const client = setup(makeConfig(transport));
      const query = new Subject<any>();

      const emitted = firstValueFrom(client.watch(query));
      query.next({ measures: ['Orders.count'] });

      expect(await emitted).toBeInstanceOf(ResultSet);
      expect(transport.calls).toEqual(['load']);
    });

    test('the api instance is created once and reused', async () => {
      const client = setup(makeConfig(transport));

      await firstValueFrom(client.load({ measures: ['Orders.count'] }));
      await firstValueFrom(client.load({ measures: ['Orders.count'] }));

      expect(transport.calls).toEqual(['load', 'load']);
    });
  });

  // A bare Subject never replays, so the subscription apiInstance() opens on the
  // first request misses the config that was already emitted. Nothing creates the
  // api instance and every request method throws.
  describe('with a bare Subject config', () => {
    test('requests throw even after the config has emitted', () => {
      const config = new Subject<any>();
      const client = setup(config);

      config.next({ token: 'token', options: { transport } });

      expect(() => client.load({ measures: ['Orders.count'] })).toThrow(TypeError);
      expect(transport.calls).toEqual([]);
    });
  });
});
