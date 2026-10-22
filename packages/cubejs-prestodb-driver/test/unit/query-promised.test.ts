import { EventEmitter } from 'events';

import { PrestoDriver } from '../../src/PrestoDriver';

const mockHttpRequest = jest.fn();

jest.mock('follow-redirects/http', () => ({
  Agent: class {},
  request: (opts: any, onResponse: any) => mockHttpRequest('http:', opts, onResponse),
}));

jest.mock('follow-redirects/https', () => ({
  Agent: class {},
  request: (opts: any, onResponse: any) => mockHttpRequest('https:', opts, onResponse),
}));

describe('PrestoDriver queryPromised', () => {
  beforeEach(() => {
    mockHttpRequest.mockReset();

    let pollNumber = 0;

    mockHttpRequest.mockImplementation(
      (protocol: string, opts: any, onResponse: (res: any) => void) => {
        const res: any = new EventEmitter();
        res.statusCode = 200;
        res.setEncoding = () => res;

        const req: any = new EventEmitter();
        req.write = () => true;
        req.destroy = () => req;

        req.end = () => {
          process.nextTick(() => {
            onResponse(res);

            let body;

            if (opts.method === 'POST') {
              body = {
                id: 'q1',
                infoUri: 'http://coordinator.local:8080/v1/query/q1',
                nextUri: 'http://coordinator.local:8080/v1/statement/q1/1',
                stats: { state: 'QUEUED' },
              };
            } else {
              pollNumber += 1;

              if (pollNumber === 1) {
                body = {
                  id: 'q1',
                  infoUri: 'http://coordinator.local:8080/v1/query/q1',
                  nextUri: 'http://coordinator.local:8080/v1/statement/q1/2',
                  stats: { state: 'RUNNING' },
                  columns: [{ name: 'one', type: 'integer' }],
                  data: [[1], [2]],
                };
              } else {
                body = {
                  id: 'q1',
                  infoUri: 'http://coordinator.local:8080/v1/query/q1',
                  stats: { state: 'FINISHED' },
                  columns: [{ name: 'one', type: 'integer' }],
                  data: [[3], [4]],
                };
              }
            }

            res.emit('data', JSON.stringify(body));
            res.emit('end');
          });
        };

        return req;
      }
    );
  });

  it('preserves the order of rows across multiple result batches', async () => {
    const driver = new PrestoDriver({
      host: 'coordinator.local',
      port: '8080',
      catalog: 'test',
      schema: 'default',
      dataSource: 'default',
      checkInterval: 1,
    } as any);

    const rows = await driver.query('SELECT 1', []);

    expect(rows).toEqual([
      { one: 1 },
      { one: 2 },
      { one: 3 },
      { one: 4 },
    ]);
  });
});