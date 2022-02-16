import yargs from 'yargs/yargs';
import cubejs, { CubejsApi } from '@cubejs-client/core';
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import { BirdBox, startBirdBoxFromCli, startBirdBoxFromContainer } from '../src';

require('jest-specific-snapshot');

const DB_TYPES = ['athena', 'bigquery'];
type DbType = typeof DB_TYPES[number];

const SERVER_MODES = ['cli', 'docker', 'local'];
type ServerMode = typeof SERVER_MODES[number];

interface Args {
  type: DbType
  envFile: string
  mode: ServerMode
}

const args: Args = yargs(process.argv.slice(2))
  .exitProcess(false)
  .options(
    {
      type: {
        choices: DB_TYPES,
        demandOption: true,
        describe: 'db type',
      },
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

const name = `${args.type}`;

describe(name, () => {
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
              dbType: args.type,
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
              name,
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
      throw e;
    }
  });

  afterAll(async () => {
    await birdbox.stop();
  });

  it('Driver.query', async () => {
    const response = await httpClient.load(
      {
        measures: ['Orders.totalAmount'],
        dimensions: ['Orders.status'],
      }
    );
    // ../.. to move out of dist/test directory
    // @ts-ignore
    expect(response.rawData()).toMatchSpecificSnapshot(`../../test/__snapshots__/${name}.query.snapshot`);
  });
});
