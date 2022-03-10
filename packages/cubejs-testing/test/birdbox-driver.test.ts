import R from 'ramda';
import yargs from 'yargs/yargs';
// eslint-disable-next-line import/no-extraneous-dependencies
import cubejs, { CubejsApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import { BirdBox, startBirdBoxFromCli, startBirdBoxFromContainer } from '../src';

const SERVER_MODES = ['cli', 'docker', 'local'];
type ServerMode = typeof SERVER_MODES[number];

interface Args {
  mode: ServerMode
}

const args: Args = yargs(process.argv.slice(2))
  .exitProcess(false)
  .options(
    {
      mode: {
        choices: SERVER_MODES,
        default: 'docker',
        describe: 'how to stand up the server',
      }
    }
  )
  .argv as Args;

export function createDriverTestCase(type: string, envVars: string[]) {
  describe(type, () => {
    jest.setTimeout(60 * 5 * 1000);

    let birdbox: BirdBox;
    let httpClient: CubejsApi;
    let env = R.fromPairs(envVars.map(k => {
      const v = process.env[k];
      if (v === undefined) {
        throw new Error(`${k} is required`);
      }
      return [k, v];
    }));
    env = {
      ...env,
      CUBEJS_SCHEDULED_REFRESH_DEFAULT: 'false',
      CUBEJS_REFRESH_WORKER: 'false',
      CUBEJS_EXTERNAL_DEFAULT: 'true',
      CUBEJS_ROLLUP_ONLY: 'true',
    };

    beforeAll(async () => {
      try {
        switch (args.mode) {
          case 'cli':
          case 'local': {
            birdbox = await startBirdBoxFromCli(
              {
                cubejsConfig: 'single/cube.js',
                dbType: type,
                useCubejsServerBinary: args.mode === 'local',
                env,
              }
            );
            break;
          }

          case 'docker': {
            birdbox = await startBirdBoxFromContainer(
              {
                name: type,
                env
              }
            );
            break;
          }

          default: {
            throw new Error(`Bad serverMode ${args.mode}`);
          }
        }

        httpClient = cubejs(async () => 'test', {
          apiUrl: birdbox.configuration.apiUrl,
        });
      } catch (e) {
        console.log(e);
        process.exit(1);
      }
    });

    afterAll(async () => {
      await birdbox.stop();
    });

    it('query', async () => {
      const response = await httpClient.load(
        {
          measures: ['OrdersPA.amount2', 'OrdersPA.amount'],
          dimensions: ['OrdersPA.id2', 'OrdersPA.status2', 'OrdersPA.id', 'OrdersPA.status'],
        }
      );
      expect(response.rawData()).toMatchSnapshot('query');
    });
  });
}
