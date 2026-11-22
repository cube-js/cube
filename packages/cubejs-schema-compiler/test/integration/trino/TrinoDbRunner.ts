import { GenericContainer, Wait } from 'testcontainers';
import type { StartedTestContainer } from 'testcontainers';

import { TrinoQuery } from '../../../src/adapter/TrinoQuery';
import type { BaseQueryOptions } from '../../../src/adapter/BaseQuery';
import type { Compilers } from '../../../src/compiler/PrepareCompiler';
import { BaseDbRunner } from '../utils/BaseDbRunner';

process.env.TZ = 'GMT';

type TrinoResponse = {
  nextUri?: string;
  columns?: { name: string }[];
  data?: unknown[][];
  error?: { message: string };
};

export class TrinoDbRunner extends BaseDbRunner {
  protected container: StartedTestContainer | null = null;

  protected trinoVersion: string = process.env.TEST_TRINO_VERSION || '483';

  public override async tearDown(): Promise<void> {
    if (this.container) {
      await this.container.stop();
      this.container = null;
    }
  }

  protected async baseUrl(): Promise<string> {
    if (process.env.TEST_TRINO_HOST) {
      return `http://${process.env.TEST_TRINO_HOST}:8080`;
    }
    if (!this.container) {
      this.container = await new GenericContainer(`trinodb/trino:${this.trinoVersion}`)
        .withExposedPorts(8080)
        .withWaitStrategy(Wait.forLogMessage('======== SERVER STARTED ========'))
        .withStartupTimeout(120 * 1000)
        .start();
    }
    return `http://localhost:${this.container.getMappedPort(8080)}`;
  }

  // The REST protocol takes no bind parameters, so they are inlined as literals.
  // Test SQL must not contain a `?` other than a placeholder.
  protected static withParams(sql: string, params: unknown[]): string {
    let i = 0;
    return sql.replace(/\?/g, () => {
      const p = params[i++];
      return typeof p === 'number' ? `${p}` : `'${String(p).replace(/'/g, "''")}'`;
    });
  }

  public async execute(sql: string): Promise<Record<string, unknown>[]> {
    const headers = { 'X-Trino-User': 'test', 'X-Trino-Catalog': 'memory', 'X-Trino-Schema': 'default' };
    const fetchJson = async (url: string, init: RequestInit): Promise<TrinoResponse> => {
      const res = await fetch(url, init);
      if (!res.ok) {
        throw new Error(`Trino responded ${res.status} for '${sql}': ${await res.text()}`);
      }
      return await res.json() as TrinoResponse;
    };
    let response = await fetchJson(`${await this.baseUrl()}/v1/statement`, { method: 'POST', headers, body: sql });
    let columns: string[] = [];
    const rows: unknown[][] = [];

    for (;;) {
      if (response.error) {
        throw new Error(`Execution failed for '${sql}': ${response.error.message}`);
      }
      if (response.columns) {
        columns = response.columns.map(c => c.name);
      }
      rows.push(...(response.data || []));
      if (!response.nextUri) {
        break;
      }
      response = await fetchJson(response.nextUri, { headers });
    }
    return rows.map(row => Object.fromEntries(columns.map((c, i) => [c, row[i]])));
  }

  public override async testQueries(queries: [string, unknown[]][]): Promise<Record<string, unknown>[]> {
    let lastResult: Record<string, unknown>[] = [];

    for (const [sql, params] of queries) {
      lastResult = await this.execute(TrinoDbRunner.withParams(sql, params));
    }
    return lastResult;
  }

  public async explain([sql, params]: [string, unknown[]]): Promise<string> {
    const rows = await this.execute(`EXPLAIN ${TrinoDbRunner.withParams(sql, params)}`);
    return rows.map(r => r['Query Plan']).join('\n');
  }

  public override newTestQuery(compilers: Compilers, query: BaseQueryOptions): TrinoQuery {
    return new TrinoQuery(compilers, query);
  }

  public override port(): number {
    return 8080;
  }
}

export const dbRunner = new TrinoDbRunner();

afterAll(async () => {
  await dbRunner.tearDown();
});
