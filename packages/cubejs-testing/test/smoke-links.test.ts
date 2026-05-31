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
    expect(fullNameDim.links).toHaveLength(5);
    expect(fullNameDim.links[0].name).toBe('google_search');
    expect(fullNameDim.links[0].label).toBe('Search on Google');
    expect(fullNameDim.links[0].icon).toBe('brand-google');
    expect(fullNameDim.links[1].name).toBe('profile');
    expect(fullNameDim.links[1].dashboard).toBe('user_profile_123');
    expect(fullNameDim.links[2].name).toBe('city_dashboard');
    expect(fullNameDim.links[2].dashboard).toBe('city_dash');
    expect(fullNameDim.links[2].params).toEqual(['city', 'user_id']);
    expect(fullNameDim.links[3].name).toBe('crm_link');
    expect(fullNameDim.links[3].params).toEqual(['full_name', 'city', 'duplicate_city']);
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

  test('dashboard link with params renders dimension values in URL', async () => {
    const response = await client.load({
      dimensions: [
        'users_with_links.full_name',
        'users_with_links.city',
        'users_with_links.full_name___link_city_dashboard_url',
      ],
      order: {
        'users_with_links.full_name': 'asc',
      },
      limit: 2,
    });
    const data = response.rawData();
    expect(data.length).toBe(2);

    // Jane Smith, city=London
    const janeUrl = data[0]['users_with_links.full_name___link_city_dashboard_url'];
    expect(janeUrl).toContain('/dashboard/city_dash');
    expect(janeUrl).toContain('city=');
    expect(janeUrl).toContain('London');
    expect(janeUrl).toContain('user_id=');

    // John Doe, city=New York (space encoded)
    const johnUrl = data[1]['users_with_links.full_name___link_city_dashboard_url'];
    expect(johnUrl).toContain('/dashboard/city_dash');
    expect(johnUrl).toContain('city=');
    expect(johnUrl).toContain('New%20York');
    expect(johnUrl).toContain('user_id=');
  });

  test('REST SQL API can query link synthetic dimensions', async () => {
    const response = await fetch(
      `${birdbox.configuration.apiUrl}/cubesql`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: DEFAULT_API_TOKEN,
        },
        body: JSON.stringify({
          query: 'SELECT full_name, full_name___link_city_dashboard_url FROM users_all ORDER BY full_name ASC LIMIT 2',
        }),
      }
    );
    const text = await response.text();
    // cubesql returns newline-delimited JSON chunks
    const lines = text.trim().split('\n').filter(Boolean);
    const json = JSON.parse(lines[lines.length - 1]) as any;
    const rows = json.data || json.results || json;
    expect(rows.length).toBeGreaterThanOrEqual(1);
    const firstRow = Array.isArray(rows[0]) ? rows[0] : Object.values(rows[0]);
    const urlValue = String(firstRow[1] || firstRow[0]);
    expect(urlValue).toContain('/dashboard/city_dash');
  });

  test('param values are URL-encoded in query results', async () => {
    // Query source cube directly to verify params with URL encoding
    const response = await client.load({
      dimensions: [
        'users.full_name',
        'users.full_name___link_city_dashboard_url',
      ],
      order: {
        'users.full_name': 'asc',
      },
      limit: 2,
    });
    const data = response.rawData();
    expect(data.length).toBe(2);

    // Jane Smith, city=London (no encoding needed)
    const janeUrl = data[0]['users.full_name___link_city_dashboard_url'];
    expect(janeUrl).toContain('/dashboard/city_dash');
    expect(janeUrl).toContain('city=');
    expect(janeUrl).toContain('London');
    expect(janeUrl).toContain('user_id=');

    // John Doe, city=New York → space should be encoded as %20
    const johnUrl = data[1]['users.full_name___link_city_dashboard_url'];
    expect(johnUrl).toContain('/dashboard/city_dash');
    expect(johnUrl).toContain('city=');
    expect(johnUrl).toContain('New%20York');
    expect(johnUrl).toContain('user_id=');
  });

  test('url link with params combines base URL and encoded query string', async () => {
    const response = await client.load({
      dimensions: [
        'users.full_name',
        'users.full_name___link_crm_link_url',
      ],
      order: {
        'users.full_name': 'asc',
      },
      limit: 2,
    });
    const data = response.rawData();
    expect(data.length).toBe(2);

    // Jane Smith, city=London, duplicate_city=London (same ref deduped in args)
    const janeUrl = data[0]['users.full_name___link_crm_link_url'];
    expect(janeUrl).toContain('/dashboard/crm_contacts');
    expect(janeUrl).toContain('full_name=');
    expect(janeUrl).toContain('city=');
    expect(janeUrl).toContain('duplicate_city=');
    expect(janeUrl).toContain('London');

    // John Doe, city=New York (space encoded in both city params)
    const johnUrl = data[1]['users.full_name___link_crm_link_url'];
    expect(johnUrl).toContain('/dashboard/crm_contacts');
    expect(johnUrl).toContain('full_name=');
    expect(johnUrl).toContain('city=');
    expect(johnUrl).toContain('duplicate_city=');
    expect(johnUrl).toContain('New%20York');
  });

  test('cross-cube reference in params resolves joined dimension values', async () => {
    const response = await client.load({
      dimensions: [
        'orders.status',
        'orders.status___link_user_link_url',
      ],
      order: {
        'orders.status': 'asc',
      },
      limit: 2,
    });
    const data = response.rawData();
    expect(data.length).toBe(2);

    // 'completed' order linked to user 1 (John Doe, New York)
    const completedUrl = data[0]['orders.status___link_user_link_url'];
    expect(completedUrl).toContain('/dashboard/user_detail');
    expect(completedUrl).toContain('user_name=');
    expect(completedUrl).toContain('user_city=');
    expect(completedUrl).toContain('New%20York');

    // 'pending' order linked to user 2 (Jane Smith, London)
    const pendingUrl = data[1]['orders.status___link_user_link_url'];
    expect(pendingUrl).toContain('/dashboard/user_detail');
    expect(pendingUrl).toContain('user_name=');
    expect(pendingUrl).toContain('user_city=');
    expect(pendingUrl).toContain('London');
  });

  test('dashboard referencing cube member produces dynamic dashboard path', async () => {
    const response = await client.load({
      dimensions: [
        'users.full_name',
        'users.full_name___link_dynamic_dashboard_url',
      ],
      order: {
        'users.full_name': 'asc',
      },
      limit: 2,
    });
    const data = response.rawData();
    expect(data.length).toBe(2);

    // Jane Smith lives in London → /dashboard/London
    const janeUrl = data[0]['users.full_name___link_dynamic_dashboard_url'];
    expect(janeUrl).toContain('/dashboard/');
    expect(janeUrl).toContain('London');

    // John Doe lives in New York → /dashboard/New York
    const johnUrl = data[1]['users.full_name___link_dynamic_dashboard_url'];
    expect(johnUrl).toContain('/dashboard/');
    expect(johnUrl).toContain('New York');
  });
});
