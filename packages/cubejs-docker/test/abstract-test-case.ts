import { DockerComposeEnvironment, StartedDockerComposeEnvironment } from 'testcontainers';
import * as path from 'path';
import cubejs from '@cubejs-client/core';

interface BirdBoxTestCaseOptions {
  name: string
}

// eslint-disable-next-line import/prefer-default-export
export function createBirdBoxTestCase(options: BirdBoxTestCaseOptions) {
  describe(options.name, () => {
    jest.setTimeout(60 * 5 * 1000);

    let env: StartedDockerComposeEnvironment|null = null;
    let config: { apiUrl: string, };

    // eslint-disable-next-line consistent-return
    beforeAll(async () => {
      if (process.env.TEST_CUBE_HOST) {
        const host = process.env.TEST_CUBE_HOST || 'localhost';
        const port = process.env.TEST_CUBE_PORT || '8888';

        config = {
          apiUrl: `http://${host}:${port}`,
        };

        return;
      }

      const dc = new DockerComposeEnvironment(
        path.resolve(path.dirname(__filename), '../../birdbox-fixtures/'),
        `${options.name}.yml`
      );

      env = await dc
        .withEnv('CUBEJS_VERSION', 'latest')
        .up();

      const host = '127.0.0.1';
      const port = env.getContainer('birdbox-cube').getMappedPort(4000);

      config = {
        apiUrl: `http://${host}:${port}/cubejs-api/v1`,
      };
    });

    // eslint-disable-next-line consistent-return
    afterAll(async () => {
      if (env) {
        await env.down();
      }
    });

    it('Query Orders.totalAmount', async () => {
      const client = cubejs(async () => 'test', {
        apiUrl: config.apiUrl,
      });

      const response = await client.load({
        measures: [
          'Orders.totalAmount'
        ],
        timeDimensions: [],
        order: {}
      });

      expect(response.rawData()).toEqual([
        {
          'Orders.totalAmount': '1700'
        }
      ]);
    });
  });
}
