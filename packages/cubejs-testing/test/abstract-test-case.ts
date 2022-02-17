// eslint-disable-next-line import/no-extraneous-dependencies
import { jest, expect, beforeAll, afterAll } from '@jest/globals';
// eslint-disable-next-line import/no-extraneous-dependencies
import cubejs, { Query, CubejsApi } from '@cubejs-client/core';
import WebSocketTransport from '@cubejs-client/ws-transport';
import { BirdBox } from '../src';

type QueryTestOptions = {
  name: string;
  ws?: true,
};

const asserts: [options: QueryTestOptions, query: Query][] = [
  [
    {
      name: '#1 Orders.totalAmount',
      ws: true,
    },
    {
      measures: [
        'Orders.totalAmount'
      ],
      timeDimensions: [],
      order: {}
    },
  ],
  [
    {
      name: '#2 Events.count',
      ws: true,
    },
    {
      measures: [
        'Events.count'
      ],
      timeDimensions: [],
      order: {},
      dimensions: []
    },
  ],
  [
    {
      name: '#3 Events.count with Events.type order by Events.count DESC',
      ws: true,
    },
    {
      measures: [
        'Events.count'
      ],
      timeDimensions: [],
      order: {
        'Events.count': 'desc'
      },
      dimensions: [
        'Events.type'
      ]
    }
  ],
  [
    {
      name: 'Dbt orders count'
    },
    {
      measures: [
        'OrdersFiltered.ordersCount'
      ]
    }
  ],
];

// eslint-disable-next-line import/prefer-default-export
export function createBirdBoxTestCase(name: string, entrypoint: () => Promise<BirdBox>) {
  describe(name, () => {
    jest.setTimeout(60 * 5 * 1000);

    let birdbox: BirdBox;
    let wsTransport: WebSocketTransport;
    let httpClient: CubejsApi;
    let wsClient: CubejsApi;

    // eslint-disable-next-line consistent-return
    beforeAll(async () => {
      // Fail fast
      try {
        birdbox = await entrypoint();

        // http clients
        httpClient = cubejs(async () => 'test', {
          apiUrl: birdbox.configuration.apiUrl,
        });

        // ws transports
        wsTransport = new WebSocketTransport({
          apiUrl: birdbox.configuration.apiUrl,
        });

        // ws clients
        wsClient = cubejs(async () => 'test', {
          apiUrl: birdbox.configuration.apiUrl,
          transport: wsTransport,
        });
      } catch (e) {
        console.log(e);
        process.exit(1);
      }
    });

    // eslint-disable-next-line consistent-return
    afterAll(async () => {
      await wsTransport.close();
      await birdbox.stop();
    });

    describe('HTTP Transport', () => {
      // eslint-disable-next-line no-restricted-syntax
      for (const [options, query] of asserts) {
        // eslint-disable-next-line no-loop-func
        it(`${options.name}`, async () => {
          const response = await httpClient.load(query);
          expect(response.rawData()).toMatchSnapshot(options.name);
        });
      }
    });

    describe('WS Transport', () => {
      // eslint-disable-next-line no-restricted-syntax
      for (const [options, query] of asserts) {
        if (options.ws) {
          // eslint-disable-next-line no-loop-func
          it(`${options.name}`, async () => {
            const response = await wsClient.load(query);
            expect(response.rawData()).toMatchSnapshot(options.name);
          });
        }
      }
    });

    it('Failing query rewrite', async () => {
      try {
        await httpClient.load({ measures: ['Orders.toRemove'] });
        throw new Error('Should not successfully run Orders.toRemove query');
      } catch (e) {
        expect((e as Error).toString()).toContain('Query should contain either');
      }
    });

    describe('query.responseFormat', () => {
      const responses: unknown[] = [];
      let transport: WebSocketTransport;
      let http: CubejsApi;
      let ws: CubejsApi;
  
      beforeAll(async () => {
        try {
          transport = new WebSocketTransport({
            apiUrl: birdbox.configuration.apiUrl,
          });
          http = cubejs(async () => 'test', {
            apiUrl: birdbox.configuration.apiUrl,
          });
          ws = cubejs(async () => 'test', {
            apiUrl: birdbox.configuration.apiUrl,
            transport,
          });
        } catch (e) {
          console.log(e);
          process.exit(1);
        }
      });
  
      afterAll(async () => {
        await transport.close();
      });
  
      test('http+responseFormat=default', async () => {
        const response = await http.load({
          dimensions: ['Orders.status'],
          measures: ['Orders.totalAmount'],
          limit: 2,
        });
        responses.push(response);
        expect(response.rawData()).toMatchSnapshot('result-type');
      });
  
      test('http+responseFormat=compact option#1', async () => {
        const response = await http.load({
          dimensions: ['Orders.status'],
          measures: ['Orders.totalAmount'],
          limit: 2,
          responseFormat: 'compact',
        });
        responses.push(response);
        expect(response.rawData()).toMatchSnapshot('result-type');
      });
  
      test('http+responseFormat=compact option#2', async () => {
        const response = await http.load(
          {
            dimensions: ['Orders.status'],
            measures: ['Orders.totalAmount'],
            limit: 2,
          },
          undefined,
          undefined,
          'compact',
        );
        responses.push(response);
        expect(response.rawData()).toMatchSnapshot('result-type');
      });
  
      test('http+responseFormat=compact option#1+2', async () => {
        const response = await http.load(
          {
            dimensions: ['Orders.status'],
            measures: ['Orders.totalAmount'],
            limit: 2,
            responseFormat: 'compact',
          },
          undefined,
          undefined,
          'compact',
        );
        responses.push(response);
        expect(response.rawData()).toMatchSnapshot('result-type');
      });
  
      test('ws+responseFormat=default', async () => {
        const response = await ws.load({
          dimensions: ['Orders.status'],
          measures: ['Orders.totalAmount'],
          limit: 2,
        });
        responses.push(response);
        expect(response.rawData()).toMatchSnapshot('result-type');
      });
  
      test('ws+responseFormat=compact option#1', async () => {
        const response = await ws.load({
          dimensions: ['Orders.status'],
          measures: ['Orders.totalAmount'],
          limit: 2,
          responseFormat: 'compact',
        });
        responses.push(response);
        expect(response.rawData()).toMatchSnapshot('result-type');
      });
  
      test('ws+responseFormat=compact option#2', async () => {
        const response = await ws.load(
          {
            dimensions: ['Orders.status'],
            measures: ['Orders.totalAmount'],
            limit: 2,
          },
          undefined,
          undefined,
          'compact',
        );
        responses.push(response);
        expect(response.rawData()).toMatchSnapshot('result-type');
      });
  
      test('ws+responseFormat=compact option#1+2', async () => {
        const response = await ws.load(
          {
            dimensions: ['Orders.status'],
            measures: ['Orders.totalAmount'],
            limit: 2,
            responseFormat: 'compact',
          },
          undefined,
          undefined,
          'compact',
        );
        responses.push(response);
        expect(response.rawData()).toMatchSnapshot('result-type');
      });
  
      test('responses', () => {
        // @ts-ignore
        expect(responses[0].rawData()).toEqual(responses[1].rawData());
        // @ts-ignore
        expect(responses[0].rawData()).toEqual(responses[2].rawData());
        // @ts-ignore
        expect(responses[0].rawData()).toEqual(responses[3].rawData());
        // @ts-ignore
        expect(responses[0].rawData()).toEqual(responses[4].rawData());
        // @ts-ignore
        expect(responses[0].rawData()).toEqual(responses[5].rawData());
        // @ts-ignore
        expect(responses[0].rawData()).toEqual(responses[6].rawData());
        // @ts-ignore
        expect(responses[0].rawData()).toEqual(responses[7].rawData());
      });
    });
  });
}
