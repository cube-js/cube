/* eslint-disable no-restricted-syntax */
import { expect } from '@jest/globals';
import cubejs from '@cubejs-client/core';
import { BirdBox } from '../src';

// eslint-disable-next-line import/prefer-default-export
export function createBirdBoxTestCase(name: string, entrypoint: () => Promise<BirdBox>) {
  describe(name, () => {
    jest.setTimeout(60 * 5 * 1000);

    let birdbox: BirdBox;

    // eslint-disable-next-line consistent-return
    beforeAll(async () => {
      // Fail fast
      try {
        birdbox = await entrypoint();
      } catch (e) {
        console.log(e);
        process.exit(1);
      }
    });

    // eslint-disable-next-line consistent-return
    afterAll(async () => {
      await birdbox.stop();
    });

    const asserts: [schema: { name: string }, query: object][] = [
      [
        {
          name: '#1 Orders.totalAmount'
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
          name: '#2 Events.count'
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
          name: '#3 Events.count with Events.type order by Events.count DESC'
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

    for (const [schema, query] of asserts) {
      // eslint-disable-next-line no-loop-func
      it(schema.name, async () => {
        const client = cubejs(async () => 'test', {
          apiUrl: birdbox.configuration.apiUrl,
        });

        const response = await client.load(query);

        expect(response.rawData()).toMatchSnapshot();
      });
    }
  });
}
