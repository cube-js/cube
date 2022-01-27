import yargs from 'yargs/yargs';
import cubejs, { CubejsApi } from '@cubejs-client/core';
import { afterAll, beforeAll, expect, jest } from "@jest/globals";
import { BirdBox, startBirdBoxFromContainer } from "../src";

export type DbType = 'athena' | 'bigquery';

const { name, envFile } = yargs(process.argv.slice(2))
  .exitProcess(false)
  .options(
    {
      name: {
        choices: ['athena', 'bigquery'],
        demandOption: true,
        describe: 'db type',
      },
      envFile: {
        alias: 'env-file',
        demandOption: true,
        describe: 'path to .env file with db config & auth env variables',
        type: 'string',
      },
    }
  )
  .argv;

describe(name, () => {
  jest.setTimeout(60 * 5 * 1000);

  let birdbox: BirdBox;
  let httpClient: CubejsApi;

  beforeAll(async () => {
    try {
      birdbox = await startBirdBoxFromContainer({ name, envFile });

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
    expect(response.rawData()).toMatchSnapshot('Driver.query');
  });
});
