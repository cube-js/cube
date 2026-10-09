import { AthenaDriver } from '../../src/AthenaDriver';

type Qid = { QueryExecutionId: string };

class TestAthenaDriver extends AthenaDriver {
  public started: string[] = [];

  public stopped: string[] = [];

  public succeeded = new Set<string>();

  public constructor() {
    super({ exportBucket: 's3://bucket', pollMaxInterval: 1 });
    (this as any).athena = {
      getQueryResults: async () => ({ ResultSet: { ResultSetMetadata: { ColumnInfo: [{ Name: 'id', Type: 'integer' }] } } }),
    };
  }

  protected async startQuery(query: string, _values: unknown[]): Promise<Qid> {
    this.started.push(query);
    return { QueryExecutionId: `q${this.started.length}` };
  }

  protected async checkStatus(qid: Qid): Promise<boolean> {
    return this.succeeded.has(qid.QueryExecutionId);
  }

  protected async stopQuery(qid: Qid): Promise<void> {
    this.stopped.push(qid.QueryExecutionId);
  }
}

const waitFor = async (cond: () => boolean) => {
  while (!cond()) {
    await new Promise(resolve => setTimeout(resolve, 1));
  }
};

describe('AthenaDriver unload cancellation', () => {
  it('stops the LIMIT 0 probe and never starts the UNLOAD', async () => {
    const driver = new TestAthenaDriver();
    const promise = driver.unloadFromQuery('SELECT 1', [], {} as any);
    await waitFor(() => driver.started.length === 1);

    await promise.cancel!();

    await expect(promise).rejects.toThrow('Query was cancelled');
    expect(driver.stopped).toEqual(['q1']);
    expect(driver.started).toEqual(['SELECT 1 LIMIT 0']);
  });

  it('stops the in-flight UNLOAD', async () => {
    const driver = new TestAthenaDriver();
    driver.succeeded.add('q1');
    const promise = driver.unload('tbl', { query: { sql: 'SELECT 1', params: [] } } as any);
    await waitFor(() => driver.started.length === 2);

    await promise.cancel!();

    await expect(promise).rejects.toThrow('Query was cancelled');
    expect(driver.stopped).toEqual(['q2']);
    expect(driver.started[1]).toContain("UNLOAD (SELECT 1)\n        TO 's3://bucket/tbl'");
  });
});
