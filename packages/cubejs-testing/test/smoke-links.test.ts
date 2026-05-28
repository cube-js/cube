import fetch from 'node-fetch';
import { StartedTestContainer } from 'testcontainers';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import cubejs, { CubeApi } from '@cubejs-client/core';
import { PostgresDBRunner } from '@cubejs-backend/testing-shared';
import { BirdBox, getBirdbox } from '../src';
import {
  DEFAULT_API_TOKEN,
  DEFAULT_CONFIG,
  JEST_AFTER_ALL_DEFAULT_TIMEOUT,
  JEST_BEFORE_ALL_DEFAULT_TIMEOUT,
} from './smoke-tests';

describe('links through views', () => {
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
      },
      {
        schemaDir: 'links/model',
        cubejsConfig: 'links/cube.js',
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

  test('meta exposes link synthetic dimensions on view with explicit includes', async () => {
    const meta = await fetch(
      `${birdbox.configuration.apiUrl}/meta`,
      { headers: { Authorization: DEFAULT_API_TOKEN } }
    );
    const metaJson = await meta.json() as any;

    const view = metaJson.cubes.find((c: any) => c.name === 'users_with_links');
    expect(view).toBeDefined();

    const dimNames = view.dimensions.map((d: any) => d.name);
    expect(dimNames).toContain('users_with_links.full_name');
    expect(dimNames).toContain('users_with_links.full_name___link_google_search_url');
    expect(dimNames).toContain('users_with_links.full_name___link_profile_url');
  });

  test('meta exposes links metadata on parent dimension', async () => {
    const meta = await fetch(
      `${birdbox.configuration.apiUrl}/meta`,
      { headers: { Authorization: DEFAULT_API_TOKEN } }
    );
    const metaJson = await meta.json() as any;

    const view = metaJson.cubes.find((c: any) => c.name === 'users_with_links');
    const fullNameDim = view.dimensions.find((d: any) => d.name === 'users_with_links.full_name');

    expect(fullNameDim.links).toBeDefined();
    expect(fullNameDim.links).toHaveLength(2);
    expect(fullNameDim.links[0].name).toBe('google_search');
    expect(fullNameDim.links[0].label).toBe('Search on Google');
    expect(fullNameDim.links[0].icon).toBe('brand-google');
    expect(fullNameDim.links[1].name).toBe('profile');
    expect(fullNameDim.links[1].dashboard).toBe('user_profile_123');
  });

  test('synthetic link dimensions are marked as synthetic in meta', async () => {
    const meta = await fetch(
      `${birdbox.configuration.apiUrl}/meta`,
      { headers: { Authorization: DEFAULT_API_TOKEN } }
    );
    const metaJson = await meta.json() as any;

    const view = metaJson.cubes.find((c: any) => c.name === 'users_with_links');
    const syntheticDim = view.dimensions.find(
      (d: any) => d.name === 'users_with_links.full_name___link_google_search_url'
    );

    expect(syntheticDim).toBeDefined();
    expect(syntheticDim.synthetic).toBe(true);
    expect(syntheticDim.type).toBe('string');
  });

  test('wildcard view includes all link synthetic dimensions', async () => {
    const meta = await fetch(
      `${birdbox.configuration.apiUrl}/meta`,
      { headers: { Authorization: DEFAULT_API_TOKEN } }
    );
    const metaJson = await meta.json() as any;

    const view = metaJson.cubes.find((c: any) => c.name === 'users_all');
    expect(view).toBeDefined();

    const dimNames = view.dimensions.map((d: any) => d.name);
    expect(dimNames).toContain('users_all.full_name___link_google_search_url');
    expect(dimNames).toContain('users_all.full_name___link_profile_url');
  });

  test('can query dashboard link synthetic dimension through view', async () => {
    const response = await client.load({
      dimensions: [
        'users_with_links.full_name',
        'users_with_links.full_name___link_profile_url',
      ],
      limit: 1,
    });
    const data = response.rawData();
    expect(data.length).toBeGreaterThanOrEqual(1);
    const url = data[0]['users_with_links.full_name___link_profile_url'];
    expect(url).toContain('/dashboard/user_profile_123');
  });
});
