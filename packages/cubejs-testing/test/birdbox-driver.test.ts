import yargs from 'yargs/yargs';
// eslint-disable-next-line import/no-extraneous-dependencies
import cubejs, { CubejsApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import { BirdBox, startBirdBoxFromCli, startBirdBoxFromContainer } from '../src';

const SERVER_MODES = ['cli', 'docker', 'local'];
type ServerMode = typeof SERVER_MODES[number];

interface Args {
  envFile: string
  mode: ServerMode
}

const args: Args = yargs(process.argv.slice(2))
  .exitProcess(false)
  .options(
    {
      envFile: {
        alias: 'env-file',
        demandOption: true,
        describe: 'path to .env file with db config & auth env variables',
        type: 'string',
      },
      mode: {
        choices: SERVER_MODES,
        default: 'docker',
        describe: 'how to stand up the server',
      }
    }
  )
  .argv as Args;

export function createDriverTestCase(type: string) {
  describe(type, () => {
    jest.setTimeout(60 * 5 * 1000);

    let birdbox: BirdBox;
    let httpClient: CubejsApi;

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
                envFile: args.envFile,
                extraEnv: {
                  CUBEJS_SCHEDULED_REFRESH_DEFAULT: 'false',
                  CUBEJS_EXTERNAL_DEFAULT: 'true',
                }
              }
            );
            break;
          }

          case 'docker': {
            birdbox = await startBirdBoxFromContainer(
              {
                name: type,
                envFile: args.envFile
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
          measures: ['Orders.totalAmount'],
          dimensions: ['Orders.status'],
        }
      );
      expect(response.rawData()).toMatchSnapshot('query');
    });
  });
}
