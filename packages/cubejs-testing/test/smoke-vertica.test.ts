import fetch from 'node-fetch';
import { StartedTestContainer } from 'testcontainers';
// eslint-disable-next-line import/no-extraneous-dependencies
import { VerticaDBRunner } from '@cubejs-backend/testing-shared';
import cubejs, { CubeApi, Query } from '@cubejs-client/core';
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import { BirdBox, getBirdbox } from '../src';
import { DEFAULT_CONFIG } from './smoke-tests';

describe('vertica pa', () => {
  jest.setTimeout(60 * 5 * 1000);
  let db: StartedTestContainer;
  let birdbox: BirdBox;
  let client: CubeApi;

  beforeAll(async () => {
    db = await VerticaDBRunner.startContainer();
    birdbox = await getBirdbox(
      'vertica',
      {
        ...DEFAULT_CONFIG,
        CUBEJS_DB_HOST: db.getHost(),
        CUBEJS_DB_PORT: `${db.getMappedPort(5433)}`,
        CUBEJS_DB_NAME: 'test',
        CUBEJS_DB_USER: 'dbadmin',
        CUBEJS_DB_PASS: '',
        CUBEJS_ROLLUP_ONLY: 'true',
        CUBEJS_REFRESH_WORKER: 'false',
      },
      {
        schemaDir: 'smoke/schema',
        cubejsConfig: 'smoke/cube.js',
      },
    );
    client = cubejs(async () => 'test', {
      apiUrl: birdbox.configuration.apiUrl,
    });
  });

  afterAll(async () => {
    await birdbox.stop();
    await db.stop();
  });

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
});
