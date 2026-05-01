import cubejs, { CubeApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, describe, expect, jest, test } from '@jest/globals';
import { BirdBox, getBirdbox } from '../src';
import {
  DEFAULT_API_TOKEN,
  DEFAULT_CONFIG,
  JEST_AFTER_ALL_DEFAULT_TIMEOUT,
  JEST_BEFORE_ALL_DEFAULT_TIMEOUT,
} from './smoke-tests';

describe('view groups', () => {
  jest.setTimeout(60 * 5 * 1000);
  let birdbox: BirdBox;
  let client: CubeApi;

  beforeAll(async () => {
    birdbox = await getBirdbox(
      'duckdb',
      {
        CUBEJS_DB_TYPE: 'duckdb',
        ...DEFAULT_CONFIG,
      },
      {
        schemaDir: 'view-groups/schema',
      }
    );
    client = cubejs(async () => DEFAULT_API_TOKEN, {
      apiUrl: birdbox.configuration.apiUrl,
    });
  }, JEST_BEFORE_ALL_DEFAULT_TIMEOUT);

  afterAll(async () => {
    await birdbox.stop();
  }, JEST_AFTER_ALL_DEFAULT_TIMEOUT);

  test('meta response includes viewGroups', async () => {
    const meta = await client.meta();

    expect(meta.meta.viewGroups).toBeDefined();
    expect(meta.meta.viewGroups!.length).toBeGreaterThan(0);
  });

  test('standalone view_group definition is returned', async () => {
    const meta = await client.meta();
    const viewGroups = meta.meta.viewGroups!;

    const salesGroup = viewGroups.find((g) => g.name === 'sales');
    expect(salesGroup).toBeDefined();
    expect(salesGroup!.title).toBe('Sales');
    expect(salesGroup!.description).toBe('Sales related views');
    expect(salesGroup!.views).toContain('RevenueView');
  });

  test('view_group collects views from view-level viewGroup property', async () => {
    const meta = await client.meta();
    const viewGroups = meta.meta.viewGroups!;

    const salesGroup = viewGroups.find((g) => g.name === 'sales');
    expect(salesGroup!.views).toContain('CustomersView');
  });

  test('view_group collects views from plural viewGroups property', async () => {
    const meta = await client.meta();
    const viewGroups = meta.meta.viewGroups!;

    const salesGroup = viewGroups.find((g) => g.name === 'sales');
    expect(salesGroup!.views).toContain('CatalogView');

    const inventoryGroup = viewGroups.find((g) => g.name === 'inventory');
    expect(inventoryGroup).toBeDefined();
    expect(inventoryGroup!.title).toBe('Inventory');
    expect(inventoryGroup!.views).toContain('CatalogView');
  });

  test('view cube config includes viewGroup reference', async () => {
    const meta = await client.meta();

    const revenueView = meta.cubes.find((c) => c.name === 'RevenueView');
    expect(revenueView).toBeDefined();
    expect(revenueView!.viewGroup).toBe('sales');

    const customersView = meta.cubes.find((c) => c.name === 'CustomersView');
    expect(customersView).toBeDefined();
    expect(customersView!.viewGroup).toBe('sales');
  });

  test('cubes without view groups do not have viewGroup', async () => {
    const meta = await client.meta();

    const ordersCube = meta.cubes.find((c) => c.name === 'Orders');
    expect(ordersCube).toBeDefined();
    expect(ordersCube!.viewGroup).toBeUndefined();
    expect(ordersCube!.viewGroups).toBeUndefined();
  });

  test('views can be queried normally', async () => {
    const response = await client.load({
      measures: ['RevenueView.count'],
    });
    expect(response.rawData()[0]['RevenueView.count']).toBe('2');
  });

  test('view in a view group can be queried', async () => {
    const response = await client.load({
      measures: ['CustomersView.count'],
    });
    expect(response.rawData()[0]['CustomersView.count']).toBe('2');
  });

  test('view in multiple view groups can be queried', async () => {
    const response = await client.load({
      measures: ['CatalogView.count'],
    });
    expect(response.rawData()[0]['CatalogView.count']).toBe('2');
  });
});
