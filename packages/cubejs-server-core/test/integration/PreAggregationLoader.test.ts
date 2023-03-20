/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `PreAggregationLoader#refresh` workflow test.
 */

/* eslint-disable @typescript-eslint/no-unused-vars */
/* eslint-disable prefer-rest-params */

import { Readable } from 'stream';
import {
  BaseDriver,
  DownloadQueryResultsOptions,
  DownloadQueryResultsResult,
  UnloadOptions,
  StreamOptions,
  StreamTableDataWithTypes,
  ExternalDriverCompatibilities,
  DownloadTableMemoryData,
  DownloadTableCSVData,
  TableQueryResult,
  QueryOptions,
  TableStructure,
  DownloadTableData,
  IndexesSQL,
  ExternalCreateTableOptions,
} from '@cubejs-backend/query-orchestrator';
import { SchemaFileRepository } from '@cubejs-backend/shared';
import { CubejsServerCore, DatabaseType } from '../../src';
import { RefreshScheduler } from '../../src/core/RefreshScheduler';
import { CompilerApi } from '../../src/core/CompilerApi';
import { OrchestratorApi, OrchestratorApiOptions } from '../../src/core/OrchestratorApi';

class Stream extends Readable {
  public _read(size: number) {
    this.push('');
  }
}

class MockDriver extends BaseDriver {
  public calls = [];

  public tables = [];

  private _unloadSupported = true;

  private _readOnly = true;

  private _csvImport = true;

  private _streamImport = true;

  private _unloadWithoutTempTable = true;

  private _streamingSource = true;

  public constructor(
    options: {
      unloadSupported: boolean,
      readOnly: boolean,
      csvImport: boolean,
      streamImport: boolean,
      unloadWithoutTempTable: boolean,
      streamingSource: boolean,
    },
  ) {
    super();
    this._unloadSupported = options.unloadSupported;
    this._readOnly = options.readOnly;
    this._csvImport = options.csvImport;
    this._streamImport = options.streamImport;
    this._unloadWithoutTempTable = options.unloadWithoutTempTable;
    this._streamingSource = options.streamingSource;
  }

  public readOnly() {
    return this._readOnly;
  }

  public capabilities() {
    return {
      csvImport: this._csvImport,
      streamImport: this._streamImport,
      unloadWithoutTempTable: this._unloadWithoutTempTable,
      streamingSource: this._streamingSource,
    };
  }

  public async testConnection() {
    return Promise.resolve();
  }

  public async query(query) {
    this.calls.push({ query: [arguments, [query]] });
    return Promise.resolve([query]);
  }

  public async loadPreAggregationIntoTable(
    preAggregationTableName: string,
    loadSql: string,
    params,
    options,
  ) {
    this.calls.push({ loadPreAggregationIntoTable: [arguments, []] });
    this.tables.push({ table_name: preAggregationTableName.split('.')[1] });
    return Promise.resolve([]);
  }

  public async downloadQueryResults(
    query: string,
    values: unknown[],
    options: DownloadQueryResultsOptions
  ): Promise<DownloadQueryResultsResult> {
    this.calls.push({ downloadQueryResults: [arguments, {
      rows: [],
      types: [],
    }] });
    return Promise.resolve({
      rows: [],
      types: [],
    });
  }

  public async createSchemaIfNotExists(
    schemaName: string
  ): Promise<void> {
    this.calls.push({ createSchemaIfNotExists: [arguments, undefined] });
  }

  public async isUnloadSupported() {
    this.calls.push({ isUnloadSupported: [arguments, this._unloadSupported] });
    return this._unloadSupported;
  }

  public async unload(tableName: string, options: UnloadOptions) {
    this.calls.push({ unload: [arguments, {
      exportBucketCsvEscapeSymbol: '\\',
      csvFile: 'file.csv',
      types: [],
      csvNoHeader: true,
    }] });
    return {
      exportBucketCsvEscapeSymbol: '\\',
      csvFile: 'file.csv',
      types: [],
      csvNoHeader: true,
    };
  }

  public async stream(
    query: string,
    values: unknown[],
    { highWaterMark }: StreamOptions
  ): Promise<StreamTableDataWithTypes> {
    this.calls.push({ stream: [arguments, {
      rowStream: new Stream(),
      types: [],
    }] });
    return {
      rowStream: new Stream(),
      types: [],
    };
  }

  public async downloadTable(
    table: string,
    options: ExternalDriverCompatibilities
  ): Promise<DownloadTableMemoryData | DownloadTableCSVData> {
    this.calls.push({ downloadTable: [arguments, {
      rows: [],
      types: [],
    }] });
    return Promise.resolve({
      rows: [],
      types: [],
    });
  }

  public async tableColumnTypes(table: string) {
    this.calls.push({ tableColumnTypes: [arguments, []] });
    return [];
  }

  public async getTablesQuery(
    schemaName: string,
  ): Promise<TableQueryResult[]> {
    this.calls.push({ getTablesQuery: [arguments, JSON.stringify(this.tables, undefined, 2)] });
    return this.tables;
  }

  public async queryColumnTypes(
    sql: string,
    params?: unknown[]
  ): Promise<{ name: any; type: string; }[]> {
    this.calls.push({ queryColumnTypes: [arguments, []] });
    return [];
  }

  public async dropTable(
    tableName: string,
    options?: QueryOptions,
  ): Promise<unknown> {
    this.calls.push({ dropTable: [arguments, []] });
    return [];
  }

  public async uploadTableWithIndexes(
    table: string,
    columns: TableStructure,
    tableData: DownloadTableData,
    indexesSql: IndexesSQL,
    uniqueKeyColumns: string[] | null,
    queryTracingObj: any,
    externalOptions: ExternalCreateTableOptions
  ) {
    this.calls.push({ uploadTableWithIndexes: [arguments, undefined] });
    this.tables.push({ table_name: table.split('.')[1] });
  }
}

const schemaContent = `
  cube('TestCube', {
    sql: 'select field from test',
    measures: {
      count: {
        type: 'count',
      },
    },
    dimensions: {
      dim: {
        sql: 'field',
        type: 'string',
      },
    },
    preAggregations: {
      externalReadonly: {
        external: true,
        readOnly: true,
        measures: [count],
        dimensions: [dim],
        // refreshKey: {
        //   every: '1 hour',
        // },
        scheduledRefresh: false,
      },
      externalWritable: {
        external: true,
        readOnly: false,
        measures: [count],
        dimensions: [dim],
        // refreshKey: {
        //  every: '1 hour',
        // },
        scheduledRefresh: false,
      },
      internalReadonly: {
        external: false,
        readOnly: true,
        measures: [count],
        dimensions: [dim],
        // refreshKey: {
        //   every: '1 hour',
        // },
        scheduledRefresh: false,
      },
      internalWritable: {
        external: false,
        readOnly: false,
        measures: [count],
        dimensions: [dim],
        // refreshKey: {
        //   every: '1 hour',
        // },
        scheduledRefresh: false,
      },
    },
  });
`;

const testRepository: SchemaFileRepository = {
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([
    {
      fileName: 'main.js',
      content: schemaContent,
    },
  ]),
};

function getTestInfra(
  options: {
    readOnly: boolean,
    unloadWithoutTempTable: boolean,
    streamingSource: boolean,
    unloadSupported: boolean,
    // external
    csvImport: boolean,
    streamImport: boolean,
  },
) {
  const client = new MockDriver(options);
  const external = new MockDriver(options);
  const core = new CubejsServerCore({
    dbType: 'postgres',
    apiSecret: 'secret',
    logger: (msg, params) => undefined,
  });
  const compiler = new CompilerApi(
    testRepository,
    async () => 'postgres',
    {
      compileContext: {},
      logger: (msg, params) => undefined,
    }
  );
  const orchestrator = new OrchestratorApi(
    () => client,
    (msg, params) => undefined,
    {
      contextToDbType: async () => 'postgres',
      contextToExternalDbType: () => 'cubestore',
      externalDriverFactory: () => external,
      continueWaitTimeout: 0.1,
      queryCacheOptions: {
        queueOptions: () => ({
          concurrency: 2,
        }),
      },
      preAggregationsOptions: {
        queueOptions: () => ({
          executionTimeout: 2,
          concurrency: 2,
        }),
      },
      redisPrefix: 'TEST_PREFIX',
    }
  );
  jest
    .spyOn(core, 'getCompilerApi')
    .mockImplementation(() => compiler);
  jest
    .spyOn(core, 'getOrchestratorApi')
    .mockImplementation(() => <any>orchestrator);
  const scheduler = new RefreshScheduler(core);

  return { scheduler, client, external };
}

function getSequence(
  cliCalls: unknown[],
  extCalls: unknown[],
) {
  const cliSeq = cliCalls.map((v) => Object.keys(v)[0]);
  const extSeq = extCalls.map((v) => Object.keys(v)[0]);
  return [cliSeq, extSeq];
}

describe('Test PreAggregationLoader.refresh()', () => {
  jest.setTimeout(60000);

  const vals = [false, false, false, false, false, false];
  for (let i = 0; i < 63; i++) {
    describe(
      `with the client (readonly=${vals[0]}, unloadWithoutTempTable=${vals[1]}, ` +
      `streamingSource=${vals[2]}, unloadSupported=${vals[3]}) ` +
      `and the external (csvImport=${vals[4]}, streamImport=${vals[5]}) and `,
      // eslint-disable-next-line no-loop-func
      () => {
        const context = {
          authInfo: { tenantId: 'tenant1' },
          securityContext: { tenantId: 'tenant1' },
          requestId: 'XXX',
        };
  
        test('external readonly pre-aggregation', async () => {
          const { scheduler, client, external } = getTestInfra({
            // data client
            readOnly: vals[0],
            unloadWithoutTempTable: vals[1],
            streamingSource: vals[2],
            unloadSupported: vals[3],
            // external storage
            csvImport: vals[4],
            streamImport: vals[5],
          });
          try {
            await scheduler.buildPreAggregations(
              context,
              {
                timezones: ['UTC'],
                preAggregations: [{ id: 'TestCube.externalReadonly' }],
                forceBuildPreAggregations: false,
                throwErrors: true,
              }
            );
          } catch (e) {
            console.error(e);
          }
          expect(getSequence(client.calls, external.calls)).toMatchSnapshot();
        });
  
        test('external writable pre-aggregation', async () => {
          const { scheduler, client, external } = getTestInfra({
            // data client
            readOnly: vals[0],
            unloadWithoutTempTable: vals[1],
            streamingSource: vals[2],
            unloadSupported: vals[3],
            // external storage
            csvImport: vals[4],
            streamImport: vals[5],
          });
          try {
            await scheduler.buildPreAggregations(
              context,
              {
                timezones: ['UTC'],
                preAggregations: [{ id: 'TestCube.externalWritable' }],
                forceBuildPreAggregations: false,
                throwErrors: true,
              }
            );
          } catch (e) {
            console.error(e);
          }
          expect(getSequence(client.calls, external.calls)).toMatchSnapshot();
        });
  
        test('internal readonly pre-aggregation', async () => {
          const { scheduler, client, external } = getTestInfra({
            // data client
            readOnly: vals[0],
            unloadWithoutTempTable: vals[1],
            streamingSource: vals[2],
            unloadSupported: vals[3],
            // external storage
            csvImport: vals[4],
            streamImport: vals[5],
          });
          try {
            await scheduler.buildPreAggregations(
              context,
              {
                timezones: ['UTC'],
                preAggregations: [{ id: 'TestCube.internalReadonly' }],
                forceBuildPreAggregations: false,
                throwErrors: true,
              }
            );
          } catch (e) {
            console.error(e);
          }
          expect(getSequence(client.calls, external.calls)).toMatchSnapshot();
        });
  
        test('internal writable pre-aggregation', async () => {
          const { scheduler, client, external } = getTestInfra({
            // data client
            readOnly: vals[0],
            unloadWithoutTempTable: vals[1],
            streamingSource: vals[2],
            unloadSupported: vals[3],
            // external storage
            csvImport: vals[4],
            streamImport: vals[5],
          });
          try {
            await scheduler.buildPreAggregations(
              context,
              {
                timezones: ['UTC'],
                preAggregations: [{ id: 'TestCube.internalWritable' }],
                forceBuildPreAggregations: false,
                throwErrors: true,
              }
            );
          } catch (e) {
            console.error(e);
          }
          expect(getSequence(client.calls, external.calls)).toMatchSnapshot();
        });
      },
    );
    for (let j = 5; j >= 0; j--) {
      vals[j] = !vals[j];
      if (vals[j]) {
        break;
      }
    }
  }
});
