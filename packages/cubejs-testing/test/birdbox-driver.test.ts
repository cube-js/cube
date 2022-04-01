import R from 'ramda';
import yargs from 'yargs/yargs';
// eslint-disable-next-line import/no-extraneous-dependencies
import cubejs, { CubejsApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import {
  BirdBox,
  startBirdBoxFromCli,
  startBirdBoxFromContainer
} from '../src';

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
        default: 'local',
        describe: 'how to stand up the server',
      }
    }
  )
  .argv as Args;

export function createDriverTestCase(
  type: string,
  envVars: string[],
) {
  describe(`${type} driver tests`, () => {
    describe('base query engine', () => {
      jest.setTimeout(60 * 5 * 1000);
      let birdbox: BirdBox;
      let httpClient: CubejsApi;

      beforeAll(async () => {
        let env = R.fromPairs(envVars.map(k => {
          const v = process.env[k];
          if (v === undefined) {
            throw new Error(`${k} is required`);
          }
          return [k, v];
        }));
        env = {
          ...env,
          CUBEJS_DEV_MODE: 'true',
          CUBEJS_WEB_SOCKETS: 'false',
          CUBEJS_EXTERNAL_DEFAULT: 'false',
          CUBEJS_SCHEDULED_REFRESH_DEFAULT: 'false',
          // CUBEJS_REFRESH_WORKER: 'false',
          // CUBEJS_ROLLUP_ONLY: 'false',
        };
        process.stdout.write(JSON.stringify(env, undefined, 2));
        try {
          switch (args.mode) {
            case 'cli':
            case 'local': {
              birdbox = await startBirdBoxFromCli(
                {
                  cubejsConfig: 'single/cube.js',
                  dbType: type,
                  useCubejsServerBinary: args.mode === 'local',
                  cubejsOutput: 'ignore',
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
          // @ts-ignore
          process.stderr.write(e);
          process.exit(1);
        }
      });

      afterAll(async () => {
        await birdbox.stop();
      });

      test('cube without pre-aggs: query', async () => {
        const response = await httpClient.load({
          dimensions: [
            'Orders.status'
          ],
          measures: [
            'Orders.totalAmount'
          ],
        });
        expect(response.rawData()).toMatchSnapshot('query');
      });

      test('cube without pre-aggs: order', async () => {
        const asc = await httpClient.load({
          dimensions: [
            'Orders.status'
          ],
          measures: [
            'Orders.totalAmount'
          ],
          order: {
            'Orders.totalAmount': 'asc'
          },
        });
        const desc = await httpClient.load({
          dimensions: [
            'Orders.status'
          ],
          measures: [
            'Orders.totalAmount'
          ],
          order: {
            'Orders.totalAmount': 'desc'
          },
        });
        expect(asc.rawData()).toMatchSnapshot('query');
        expect(desc.rawData()).toMatchSnapshot('query');
      });

      test('cube without pre-aggs: limit', async () => {
        const response = await httpClient.load({
          dimensions: [
            'Orders.status'
          ],
          measures: [
            'Orders.totalAmount'
          ],
          limit: 2,
        });
        expect(response.rawData().length).toEqual(2);
      });

      test('cube without pre-aggs: total', async () => {
        const response = await httpClient.load({
          dimensions: [
            'Orders.status'
          ],
          measures: [
            'Orders.totalAmount'
          ],
          total: true,
        });
        expect(response.serialize().loadResponse.results[0].total)
          .toEqual(3);
      });

      test('cube with pre-aggs: query', async () => {
        const response = await httpClient.load({
          measures: [
            'OrdersPA.amount2',
            'OrdersPA.amount'
          ],
          dimensions: [
            'OrdersPA.id2',
            'OrdersPA.status2',
            'OrdersPA.id',
            'OrdersPA.status'
          ],
        });
        expect(response.rawData()).toMatchSnapshot('query');
      });

      test('cube with pre-aggs: order', async () => {
        const asc = await httpClient.load({
          measures: [
            'OrdersPA.amount2',
            'OrdersPA.amount'
          ],
          dimensions: [
            'OrdersPA.id2',
            'OrdersPA.status2',
            'OrdersPA.id',
            'OrdersPA.status'
          ],
          order: {
            'OrdersPA.amount': 'asc'
          },
        });
        const desc = await httpClient.load({
          measures: [
            'OrdersPA.amount2',
            'OrdersPA.amount'
          ],
          dimensions: [
            'OrdersPA.id2',
            'OrdersPA.status2',
            'OrdersPA.id',
            'OrdersPA.status'
          ],
          order: {
            'OrdersPA.amount': 'desc'
          },
        });
        expect(asc.rawData()).toMatchSnapshot('query');
        expect(desc.rawData()).toMatchSnapshot('query');
      });

      test('cube with pre-aggs: limit', async () => {
        const response = await httpClient.load({
          measures: [
            'OrdersPA.amount2',
            'OrdersPA.amount'
          ],
          dimensions: [
            'OrdersPA.id2',
            'OrdersPA.status2',
            'OrdersPA.id',
            'OrdersPA.status'
          ],
          limit: 2,
        });
        expect(response.rawData().length).toEqual(2);
      });

      test('cube with pre-aggs: total', async () => {
        const response = await httpClient.load({
          measures: [
            'OrdersPA.amount2',
            'OrdersPA.amount'
          ],
          dimensions: [
            'OrdersPA.id2',
            'OrdersPA.status2',
            'OrdersPA.id',
            'OrdersPA.status'
          ],
          total: true,
        });
        expect(response.serialize().loadResponse.results[0].total)
          .toEqual(5);
      });
    });

    describe.skip('pre-aggregation engine', () => {
      jest.setTimeout(60 * 5 * 1000);
      let birdbox: BirdBox;
      let httpClient: CubejsApi;
  
      beforeAll(async () => {
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
          CUBEJS_REFRESH_WORKER: 'true',
          CUBEJS_EXTERNAL_DEFAULT: 'false',
          CUBEJS_ROLLUP_ONLY: 'true',
        };
        try {
          switch (args.mode) {
            case 'cli':
            case 'local': {
              birdbox = await startBirdBoxFromCli(
                {
                  cubejsConfig: 'single/cube.js',
                  dbType: type,
                  useCubejsServerBinary: args.mode === 'local',
                  cubejsOutput: 'ignore',
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
          // @ts-ignore
          process.stderr.write(e);
          process.exit(1);
        }
      });
  
      afterAll(async () => {
        await birdbox.stop();
      });
  
      test('cube with pre-aggs: query', async () => {
        const response = await httpClient.load({
          measures: [
            'OrdersPA.amount2',
            'OrdersPA.amount'
          ],
          dimensions: [
            'OrdersPA.id2',
            'OrdersPA.status2',
            'OrdersPA.id',
            'OrdersPA.status'
          ],
        });
        expect(response.rawData()).toMatchSnapshot('query');
      });

      test('cube with pre-aggs: order', async () => {
        const asc = await httpClient.load({
          measures: [
            'OrdersPA.amount2',
            'OrdersPA.amount'
          ],
          dimensions: [
            'OrdersPA.id2',
            'OrdersPA.status2',
            'OrdersPA.id',
            'OrdersPA.status'
          ],
          order: {
            'OrdersPA.amount': 'asc'
          },
        });
        const desc = await httpClient.load({
          measures: [
            'OrdersPA.amount2',
            'OrdersPA.amount'
          ],
          dimensions: [
            'OrdersPA.id2',
            'OrdersPA.status2',
            'OrdersPA.id',
            'OrdersPA.status'
          ],
          order: {
            'OrdersPA.amount': 'desc'
          },
        });
        expect(asc.rawData()).toMatchSnapshot('query');
        expect(desc.rawData()).toMatchSnapshot('query');
      });

      test('cube with pre-aggs: limit', async () => {
        const response = await httpClient.load({
          measures: [
            'OrdersPA.amount2',
            'OrdersPA.amount'
          ],
          dimensions: [
            'OrdersPA.id2',
            'OrdersPA.status2',
            'OrdersPA.id',
            'OrdersPA.status'
          ],
          limit: 2,
        });
        expect(response.rawData().length).toEqual(2);
      });

      test('cube with pre-aggs: total', async () => {
        const response = await httpClient.load({
          measures: [
            'OrdersPA.amount2',
            'OrdersPA.amount'
          ],
          dimensions: [
            'OrdersPA.id2',
            'OrdersPA.status2',
            'OrdersPA.id',
            'OrdersPA.status'
          ],
          total: true,
        });
        expect(response.serialize().loadResponse.results[0].total)
          .toEqual(5);
      });
    });
  });
}
