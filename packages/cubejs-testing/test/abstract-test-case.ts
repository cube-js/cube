import { jest, expect, beforeAll, afterAll } from '@jest/globals';
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
];

// eslint-disable-next-line import/prefer-default-export
export function createBirdBoxTestCase(name: string, entrypoint: () => Promise<BirdBox>) {
  describe(name, () => {
    jest.setTimeout(60 * 5 * 1000);

    let birdbox: BirdBox;
    let httpClient: CubejsApi;
    let wsClient: CubejsApi;
    let wsTransport: WebSocketTransport;

    // eslint-disable-next-line consistent-return
    beforeAll(async () => {
      // Fail fast
      try {
        birdbox = await entrypoint();

        httpClient = cubejs(async () => 'test', {
          apiUrl: birdbox.configuration.apiUrl,
        });

        wsTransport = new WebSocketTransport({
          apiUrl: birdbox.configuration.apiUrl,
        });
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
        expect(e.toString()).toContain('Query should contain either');
      }
    });
  });
}
