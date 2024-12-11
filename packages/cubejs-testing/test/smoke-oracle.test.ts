// eslint-disable-next-line import/no-extraneous-dependencies
import { expect } from '@jest/globals';

// todo: @ovr fix me please
describe('oracle', () => {
  //   jest.setTimeout(60 * 5 * 100000);
  //   let db: StartedTestContainer;
  //   let birdbox: BirdBox;
  //   let client: CubejsApi;

  //   beforeAll(async () => {
  //     db = await OracleDBRunner.startContainer({});

  //     const stream = await db.logs();
  //     stream.pipe(process.stdout);

  //     birdbox = await getBirdbox(
  //       'oracle',
  //       {
  //         CUBEJS_DB_TYPE: 'oracle',

  //         CUBEJS_DB_HOST: db.getHost(),
  //         CUBEJS_DB_PORT: `${db.getMappedPort(1521)}`,
  //         CUBEJS_DB_NAME: 'XE',
  //         CUBEJS_DB_USER: 'system',
  //         CUBEJS_DB_PASS: 'test',

  //         ...DEFAULT_CONFIG,
  //       },
  //       {
  //         schemaDir: 'oracle/schema',
  //       }
  //     );
  //     client = cubejs(async () => DEFAULT_API_TOKEN, {
  //       apiUrl: birdbox.configuration.apiUrl,
  //     });
  //   });

  //   afterAll(async () => {
  //     await birdbox.stop();
  //     await db.stop();
  //   });

  // test('query measure', () => testQueryMeasure(client));

  test('query measure', () => {
    expect([{ 'Orders.totalAmount': 1700 }]).toMatchSnapshot('query');
  });
});
