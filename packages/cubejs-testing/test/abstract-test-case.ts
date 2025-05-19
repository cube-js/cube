// eslint-disable-next-line import/no-extraneous-dependencies
import { jest, expect, beforeAll, afterAll } from '@jest/globals';
// eslint-disable-next-line import/no-extraneous-dependencies
import cubejs, { Query, CubeApi } from '@cubejs-client/core';
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
      name: '#3 Events.count with Events.type order by Events.type DESC, Events.count',
      ws: true,
    },
    {
      measures: [
        'Events.count'
      ],
      timeDimensions: [],
      order: {
        'Events.type': 'desc',
        'Events.count': 'asc'
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
  [
    {
      name: 'Different column data types'
    },
    {
      dimensions: [
        'unusualDataTypes.array',
        'unusualDataTypes.bit_column',
        'unusualDataTypes.boolean_column',
        'unusualDataTypes.cidr_column',
        'unusualDataTypes.id',
        'unusualDataTypes.inet_column',
        'unusualDataTypes.json',
        'unusualDataTypes.jsonb',
        'unusualDataTypes.mac_address',
        'unusualDataTypes.point_column',
        'unusualDataTypes.status',
        'unusualDataTypes.text_column',
        'unusualDataTypes.xml_column'
      ],
      ungrouped: true,
      order: {
        'unusualDataTypes.id': 'asc'
      }
    }
  ],
];

// eslint-disable-next-line import/prefer-default-export
export function createBirdBoxTestCase(
  name: string,
  entrypoint: () => Promise<BirdBox>,
): void {
  describe(name, () => {
    jest.setTimeout(60 * 5 * 1000);

    let birdbox: BirdBox;
    let wsTransport: WebSocketTransport;
    let httpClient: CubeApi;
    let wsClient: CubeApi;

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
        process.stderr.write(`${(e as Error).toString()}\n`);
        process.exit(1);
      }
    });

    // eslint-disable-next-line consistent-return
    afterAll(async () => {
      await wsTransport.close();
      await birdbox.stop();
    });

    it('Failing query rewrite', async () => {
      try {
        await httpClient.load({ measures: ['Orders.toRemove'] });
        throw new Error('Should not successfully run Orders.toRemove query');
      } catch (e) {
        expect((e as Error).toString()).toContain('Query should contain either');
      }
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

    describe('responseFormat', () => {
      const responses: unknown[] = [];
      let transport: WebSocketTransport;
      let http: CubeApi;
      let ws: CubeApi;

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

    describe('filters', () => {
      const containsAsserts: [options: QueryTestOptions, query: Query][] = [
        [
          {
            name: '#1 Orders.status.contains: ["e"]',
          },
          {
            measures: [
              'Orders.count'
            ],
            filters: [
              {
                member: 'Orders.status',
                operator: 'contains',
                values: ['e'],
              },
            ],
          },
        ], [
          {
            name: '#2 Orders.status.contains: ["es"]',
          },
          {
            measures: [
              'Orders.count'
            ],
            filters: [
              {
                member: 'Orders.status',
                operator: 'contains',
                values: ['es'],
              },
            ],
          },
        ], [
          {
            name: '#3 Orders.status.contains: ["es", "w"]',
          },
          {
            measures: [
              'Orders.count'
            ],
            filters: [
              {
                member: 'Orders.status',
                operator: 'contains',
                values: ['es', 'w'],
              },
            ],
          },
        ], [
          {
            name: '#3 Orders.status.contains: ["a"]',
          },
          {
            measures: [
              'Orders.count'
            ],
            filters: [
              {
                member: 'Orders.status',
                operator: 'contains',
                values: ['a'],
              },
            ],
          },
        ],
      ];
      const startsWithAsserts: [options: QueryTestOptions, query: Query][] = [
        [
          {
            name: '#1 Orders.status.startsWith: ["a"]',
          },
          {
            measures: [
              'Orders.count'
            ],
            filters: [
              {
                member: 'Orders.status',
                operator: 'startsWith',
                values: ['a'],
              },
            ],
          },
        ], [
          {
            name: '#2 Orders.status.startsWith: ["n"]',
          },
          {
            measures: [
              'Orders.count'
            ],
            filters: [
              {
                member: 'Orders.status',
                operator: 'startsWith',
                values: ['n'],
              },
            ],
          },
        ], [
          {
            name: '#3 Orders.status.startsWith: ["p"]',
          },
          {
            measures: [
              'Orders.count'
            ],
            filters: [
              {
                member: 'Orders.status',
                operator: 'startsWith',
                values: ['p'],
              },
            ],
          },
        ], [
          {
            name: '#4 Orders.status.startsWith: ["sh"]',
          },
          {
            measures: [
              'Orders.count'
            ],
            filters: [
              {
                member: 'Orders.status',
                operator: 'startsWith',
                values: ['sh'],
              },
            ],
          },
        ], [
          {
            name: '#5 Orders.status.startsWith: ["n", "p", "s"]',
          },
          {
            measures: [
              'Orders.count'
            ],
            filters: [
              {
                member: 'Orders.status',
                operator: 'startsWith',
                values: ['n', 'p', 's'],
              },
            ],
          },
        ],
      ];
      const endsWithAsserts: [options: QueryTestOptions, query: Query][] = [
        [
          {
            name: '#1 Orders.status.endsWith: ["a"]',
          },
          {
            measures: [
              'Orders.count'
            ],
            filters: [
              {
                member: 'Orders.status',
                operator: 'endsWith',
                values: ['a'],
              },
            ],
          },
        ], [
          {
            name: '#2 Orders.status.endsWith: ["w"]',
          },
          {
            measures: [
              'Orders.count'
            ],
            filters: [
              {
                member: 'Orders.status',
                operator: 'endsWith',
                values: ['w'],
              },
            ],
          },
        ], [
          {
            name: '#3 Orders.status.endsWith: ["sed"]',
          },
          {
            measures: [
              'Orders.count'
            ],
            filters: [
              {
                member: 'Orders.status',
                operator: 'endsWith',
                values: ['sed'],
              },
            ],
          },
        ], [
          {
            name: '#4 Orders.status.endsWith: ["ped"]',
          },
          {
            measures: [
              'Orders.count'
            ],
            filters: [
              {
                member: 'Orders.status',
                operator: 'endsWith',
                values: ['ped'],
              },
            ],
          },
        ], [
          {
            name: '#4 Orders.status.endsWith: ["w", "sed", "ped"]',
          },
          {
            measures: [
              'Orders.count'
            ],
            filters: [
              {
                member: 'Orders.status',
                operator: 'endsWith',
                values: ['w', 'sed', 'ped'],
              },
            ],
          },
        ],
      ];

      describe('contains', () => {
        // eslint-disable-next-line no-restricted-syntax
        for (const [options, query] of containsAsserts) {
          // eslint-disable-next-line no-loop-func
          it(`${options.name}`, async () => {
            const response = await httpClient.load(query);
            expect(response.rawData()).toMatchSnapshot(options.name);
          });
        }
      });

      describe('startsWith', () => {
        // eslint-disable-next-line no-restricted-syntax
        for (const [options, query] of startsWithAsserts) {
          // eslint-disable-next-line no-loop-func
          it(`${options.name}`, async () => {
            const response = await httpClient.load(query);
            expect(response.rawData()).toMatchSnapshot(options.name);
          });
        }
      });

      describe('endsWith', () => {
        // eslint-disable-next-line no-restricted-syntax
        for (const [options, query] of endsWithAsserts) {
          // eslint-disable-next-line no-loop-func
          it(`${options.name}`, async () => {
            const response = await httpClient.load(query);
            expect(response.rawData()).toMatchSnapshot(options.name);
          });
        }
      });
    });
  });
}
