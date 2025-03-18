import fetch from 'node-fetch';
import { StartedTestContainer } from 'testcontainers';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import cubejs, { CubeApi, Query } from '@cubejs-client/core';
import { PostgresDBRunner } from '@cubejs-backend/testing-shared';
import { BirdBox, getBirdbox } from '../src';
import {
  DEFAULT_API_TOKEN,
  DEFAULT_CONFIG,
  JEST_AFTER_ALL_DEFAULT_TIMEOUT,
  JEST_BEFORE_ALL_DEFAULT_TIMEOUT,
} from './smoke-tests';

describe('postgres pa', () => {
  jest.setTimeout(60 * 5 * 1000);
  let db: StartedTestContainer;
  let birdbox: BirdBox;
  let client: CubeApi;

  beforeAll(async () => {
    db = await PostgresDBRunner.startContainer({});
    birdbox = await getBirdbox(
      'postgres',
      {
        ...DEFAULT_CONFIG,
        CUBEJS_DB_HOST: db.getHost(),
        CUBEJS_DB_PORT: `${db.getMappedPort(5432)}`,
        CUBEJS_DB_NAME: 'test',
        CUBEJS_DB_USER: 'test',
        CUBEJS_DB_PASS: 'test',
        CUBEJS_ROLLUP_ONLY: 'true',
        CUBEJS_REFRESH_WORKER: 'false',
      },
      {
        schemaDir: 'smoke/schema',
        cubejsConfig: 'smoke/cube.js',
      },
    );
    client = cubejs(async () => DEFAULT_API_TOKEN, {
      apiUrl: birdbox.configuration.apiUrl,
    });
  }, JEST_BEFORE_ALL_DEFAULT_TIMEOUT);

  afterAll(async () => {
    await birdbox.stop();
    await db.stop();
  }, JEST_AFTER_ALL_DEFAULT_TIMEOUT);

  test('basic pa', async () => {
    const query: Query = {
      measures: ['OrdersPA.count'],
      dimensions: ['OrdersPA.status'],
      order: {
        'OrdersPA.status': 'asc',
      },
    };
    const result = await client.load(query, {});
    expect(result.rawData()).toEqual([
      {
        'OrdersPA.count': '2',
        'OrdersPA.status': 'new',
      },
      {
        'OrdersPA.count': '2',
        'OrdersPA.status': 'processed',
      },
      {
        'OrdersPA.count': '1',
        'OrdersPA.status': 'shipped',
      },
    ]);
  });

  test('preview', async () => {
    const id = 'OrdersPA.ordersByStatus';

    const partitions = await (await fetch(`${birdbox.configuration.systemUrl}/pre-aggregations/partitions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: {
          preAggregations: [
            {
              id
            }
          ]
        }
      }),
    })).json();
    const partition = partitions.preAggregationPartitions[0].partitions[0];
    const { timezone } = partition;
    const versionEntry = partition.versionEntries[0];
    expect(versionEntry.build_range_end).not.toBeDefined();

    const preview = await (await fetch(`${birdbox.configuration.systemUrl}/pre-aggregations/preview`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: {
          preAggregationId: id,
          timezone,
          versionEntry,
        }
      }),
    })).json();
    expect(preview.preview).toBeDefined();
  });

  test('different column types', async () => {
    const query: Query = {
      dimensions: [
        'unusualDataTypes.array',
        'unusualDataTypes.bit_column',
        'unusualDataTypes.boolean_column',
        'unusualDataTypes.cidr_column',
        'unusualDataTypes.id',
        'unusualDataTypes.inet_column',
        'unusualDataTypes.json',
        'unusualDataTypes.jsonb',
        'unusualDataTypes.mac_address',
        'unusualDataTypes.point_column',
        'unusualDataTypes.status',
        'unusualDataTypes.text_column',
        'unusualDataTypes.xml_column'
      ],
      ungrouped: true,
      order: {
        'unusualDataTypes.id': 'asc'
      }
    };
    const result = await client.load(query, {});
    expect(result.rawData()).toEqual([
      {
        'unusualDataTypes.mac_address': '11:22:33:44:55:66',
        'unusualDataTypes.inet_column': '192.168.0.1',
        'unusualDataTypes.bit_column': '11111111',
        'unusualDataTypes.status': 'new',
        'unusualDataTypes.array': [1, 2, 3],
        'unusualDataTypes.text_column': 'Hello, world!',
        'unusualDataTypes.id': 1,
        'unusualDataTypes.point_column': { x: 1, y: 1 },
        'unusualDataTypes.boolean_column': true,
        'unusualDataTypes.jsonb': { key: 'value1', number: 42 },
        'unusualDataTypes.xml_column': '<root><child>data</child></root>',
        'unusualDataTypes.cidr_column': '192.168.0.0/24',
        'unusualDataTypes.json': { key: 'value1', number: 42 }
      },
      {
        'unusualDataTypes.cidr_column': '192.168.0.0/24',
        'unusualDataTypes.array': [4, 5, 6],
        'unusualDataTypes.status': 'new',
        'unusualDataTypes.mac_address': '00:11:22:33:44:55',
        'unusualDataTypes.text_column': 'Goodbye, world!',
        'unusualDataTypes.json': { key: 'value2', number: 84 },
        'unusualDataTypes.id': 2,
        'unusualDataTypes.bit_column': '00000001',
        'unusualDataTypes.xml_column': '<root><child>more data</child></root>',
        'unusualDataTypes.point_column': { x: 2, y: 2 },
        'unusualDataTypes.jsonb': { key: 'value2', number: 84 },
        'unusualDataTypes.inet_column': '192.168.0.2',
        'unusualDataTypes.boolean_column': false
      },
      {
        'unusualDataTypes.text_column': 'PostgreSQL is awesome!',
        'unusualDataTypes.boolean_column': true,
        'unusualDataTypes.json': { key: 'value3', number: 168 },
        'unusualDataTypes.array': [7, 8, 9],
        'unusualDataTypes.point_column': { x: 3, y: 3 },
        'unusualDataTypes.id': 3,
        'unusualDataTypes.mac_address': '22:33:44:55:66:77',
        'unusualDataTypes.bit_column': '11110000',
        'unusualDataTypes.status': 'processed',
        'unusualDataTypes.cidr_column': '192.168.0.0/24',
        'unusualDataTypes.xml_column': '<root><child>even more data</child></root>',
        'unusualDataTypes.jsonb': { key: 'value3', number: 168 },
        'unusualDataTypes.inet_column': '192.168.0.3'
      }
    ]);
  });
});
