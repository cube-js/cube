import express from 'express';
import request from 'supertest';
import { ResultWrapper } from '@cubejs-backend/native';
import { testLogger } from '@cubejs-backend/shared';
import { ApiGateway } from '../src';
import { compilerApi, AdapterApiMock, DataSourceStorageMock } from './mocks';

const AUTH = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M';

// The shared mock leaves `Foo.time` untyped; the result transform only formats `time` members.
const timeCompilerApi = jest.fn().mockImplementation(async (...args: any[]) => {
  const api = await compilerApi(...args);
  return {
    ...api,
    async getSql(...sqlArgs: any[]) {
      const sql = await api.getSql(...sqlArgs);
      return {
        ...sql,
        aliasNameToMember: { ...sql.aliasNameToMember, foo__time_day: 'Foo.time.day' },
      };
    },
    async metaConfig(...metaArgs: any[]) {
      const meta = await api.metaConfig(...metaArgs);
      const cubes = Array.isArray(meta) ? meta : meta.cubes;

      for (const cube of cubes) {
        for (const dimension of cube.config.dimensions) {
          if (dimension.name === 'Foo.time') {
            dimension.type = 'time';
          }
        }
      }
      return meta;
    },
  };
});

class TimeAdapterApiMock extends AdapterApiMock {
  public async executeQuery(): Promise<any> {
    return {
      data: [{
        foo__time: '2026-09-16 10:40:35.694',
        foo__time_day: '2026-09-16T00:00:00.000',
      }],
    };
  }
}

function createGateway() {
  return new ApiGateway('secret', timeCompilerApi, async () => new TimeAdapterApiMock(), testLogger, {
    standalone: true,
    dataSourceStorage: new DataSourceStorageMock(),
    basePath: '/cubejs-api',
    refreshScheduler: {},
  });
}

async function load(responseFormat?: string) {
  const app = express();
  app.use(express.json());
  createGateway().initApp(app);

  const query = {
    dimensions: ['Foo.time'],
    timeDimensions: [{ dimension: 'Foo.time', granularity: 'day' }],
    timezone: 'Europe/Amsterdam',
    ...(responseFormat ? { responseFormat } : {}),
  };
  const res = await request(app)
    .get(`/cubejs-api/v1/load?query=${encodeURIComponent(JSON.stringify(query))}`)
    .set('Authorization', AUTH)
    .expect(200);

  return res.body;
}

describe('CUBEJS_TIMESTAMP_WITH_OFFSET', () => {
  afterEach(() => {
    delete process.env.CUBEJS_TIMESTAMP_WITH_OFFSET;
    jest.restoreAllMocks();
  });

  test('time values stay naive by default', async () => {
    const body = await load();

    expect(body.data[0]['Foo.time']).toBe('2026-09-16T10:40:35.694');
    expect(body.data[0]['Foo.time.day']).toBe('2026-09-16T00:00:00.000');
  });

  test('time values carry an offset when enabled', async () => {
    process.env.CUBEJS_TIMESTAMP_WITH_OFFSET = 'true';

    const body = await load();

    // Raw time dimensions are UTC instants
    expect(body.data[0]['Foo.time']).toBe('2026-09-16T10:40:35.694Z');
    // Time dimensions with a granularity are wall-clock time in the query timezone
    expect(body.data[0]['Foo.time.day']).toBe('2026-09-16T00:00:00.000+02:00');
  });

  test('compact response format carries an offset when enabled', async () => {
    process.env.CUBEJS_TIMESTAMP_WITH_OFFSET = 'true';

    const body = await load('compact');
    const { members, dataset } = body.data;

    expect(dataset[0][members.indexOf('Foo.time')]).toBe('2026-09-16T10:40:35.694Z');
    expect(dataset[0][members.indexOf('Foo.time.day')]).toBe('2026-09-16T00:00:00.000+02:00');
  });

  // cubesql parses these values back, so the SQL API keeps the naive format
  test('sqlApiLoad never asks for offsets', async () => {
    process.env.CUBEJS_TIMESTAMP_WITH_OFFSET = 'true';
    const setTransformData = jest.spyOn(ResultWrapper.prototype, 'setTransformData');

    let response: any;
    await createGateway().sqlApiLoad({
      query: { dimensions: ['Foo.time'] },
      streaming: false,
      memberExpressions: true,
      context: { requestId: 'timestamp-with-offset-test', securityContext: {} } as any,
      res: (r: any) => {
        response = r;
      },
      apiType: 'sql',
    } as any);

    expect(response.error).toBeUndefined();
    expect(setTransformData).toHaveBeenCalled();

    for (const [params] of setTransformData.mock.calls) {
      expect(params.timestampWithOffset).toBe(false);
    }
  });
});
