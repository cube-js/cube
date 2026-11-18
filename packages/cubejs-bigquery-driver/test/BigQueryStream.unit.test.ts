import { Readable } from 'stream';
import { BigQueryDriver } from '../src';

class BigQueryDriverWithFakeClient extends BigQueryDriver {
  public setClient(client: unknown) {
    (this as unknown as { bigquery: unknown }).bigquery = client;
  }
}

// https://github.com/cube-js/cube/issues/12842
describe('BigQueryDriver.stream', () => {
  test('forwards a BigQuery query stream error to rowStream', async () => {
    const driver = Object.create(BigQueryDriverWithFakeClient.prototype) as BigQueryDriverWithFakeClient;
    const source = new Readable({ objectMode: true, read() { /* rows are pushed by the test */ } });
    driver.setClient({ createQueryStream: async () => source });

    const { rowStream } = await driver.stream('select 1 as a, 2 as a', [], { highWaterMark: 16 });
    const rowStreamError = new Promise<Error | null>((resolve) => {
      rowStream.on('error', resolve);
      setTimeout(() => resolve(null), 500);
    });

    source.destroy(new Error('Duplicate column names in the result are not supported. Found duplicate(s): a'));

    const err = await rowStreamError;
    expect(err?.message).toContain('Duplicate column names');
  });
});
