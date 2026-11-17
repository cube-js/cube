// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, jest, expect } from '@jest/globals';
import { sign } from 'jsonwebtoken';
import fetch from 'node-fetch';

import { BirdBox, getBirdbox } from '../src';
import {
  DEFAULT_CONFIG,
  JEST_AFTER_ALL_DEFAULT_TIMEOUT,
  JEST_BEFORE_ALL_DEFAULT_TIMEOUT,
} from './smoke-tests';

describe('GraphQL Schema Caching and RBAC', () => {
  jest.setTimeout(60 * 5 * 1000);
  let birdbox: BirdBox;

  beforeAll(async () => {
    birdbox = await getBirdbox(
      'duckdb',
      {
        ...DEFAULT_CONFIG,
        CUBEJS_DEV_MODE: 'false',
        NODE_ENV: 'production',
        CUBEJS_DB_TYPE: 'duckdb',
      },
      {
        schemaDir: 'rbac-graphql/model',
        cubejsConfig: 'rbac-graphql/cube.js',
      }
    );
  }, JEST_BEFORE_ALL_DEFAULT_TIMEOUT);

  afterAll(async () => {
    await birdbox.stop();
  }, JEST_AFTER_ALL_DEFAULT_TIMEOUT);

  async function graphqlRequest(role: string, query: string): Promise<any> {
    const token = sign({
      auth: { roles: [role] },
    }, DEFAULT_CONFIG.CUBEJS_API_SECRET, { expiresIn: '1h' });

    const baseUrl = birdbox.configuration.apiUrl.replace('/cubejs-api/v1', '');
    const res = await fetch(`${baseUrl}/cubejs-api/graphql`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify({ query }),
    });
    return res.json();
  }

  async function memberFields(group: string): Promise<string[] | undefined> {
    const result = await graphqlRequest(group, '{ __type(name: "OrdersMembers") { fields { name } } }');
    return result.data?.__type?.fields?.map((f: any) => f.name);
  }

  test('introspection lists only the members a group can see', async () => {
    const fieldsA = await memberFields('tenant-a');
    expect(fieldsA).toContain('internalCode');
    expect(fieldsA).not.toContain('tier');

    const fieldsB = await memberFields('tenant-b');
    expect(fieldsB).toContain('tier');
    expect(fieldsB).not.toContain('internalCode');

    // Asked again after tenant-b, so a schema cached for one group can't be served to another
    expect(await memberFields('tenant-a')).toEqual(fieldsA);
  });

  test('introspection exposes nothing to a group that sees no members', async () => {
    const result = await graphqlRequest('default', '{ __schema { types { name } } }');
    expect(result.errors).toBeUndefined();

    const typeNames = result.data.__schema.types.map((t: any) => t.name);
    expect(typeNames).toContain('Query');
    expect(typeNames).not.toContain('Result');
    expect(typeNames).not.toContain('OrdersMembers');
    expect(JSON.stringify(result)).not.toContain('internalCode');

    const placeholder = await graphqlRequest('default', '{ no_queries_available }');
    expect(placeholder.errors).toBeUndefined();
    expect(placeholder.data.no_queries_available).toContain('No cubes or views have members visible to you');
  });

  test('tenant-a can query internalCode but not tier', async () => {
    const allowedResult = await graphqlRequest('tenant-a', `{
      cube(where: { orders: {} }) {
        orders { internalCode }
      }
    }`);
    expect(allowedResult.errors).toBeUndefined();
    expect(allowedResult.data.cube).toHaveLength(1);
    expect(allowedResult.data.cube[0].orders.internalCode).toBe('secret123');

    const restrictedResult = await graphqlRequest('tenant-a', `{
      cube(where: { orders: {} }) {
        orders { tier }
      }
    }`);
    expect(restrictedResult.errors).toBeDefined();
    expect(restrictedResult.errors[0].message).toContain('Cannot query field "tier"');
  });

  test('tenant-b can query tier but not internalCode', async () => {
    const allowedResult = await graphqlRequest('tenant-b', `{
      cube(where: { orders: {} }) {
        orders { tier }
      }
    }`);
    expect(allowedResult.errors).toBeUndefined();
    expect(allowedResult.data.cube).toHaveLength(1);
    expect(allowedResult.data.cube[0].orders.tier).toBe('premium');

    const restrictedResult = await graphqlRequest('tenant-b', `{
      cube(where: { orders: {} }) {
        orders { internalCode }
      }
    }`);
    expect(restrictedResult.errors).toBeDefined();
    expect(restrictedResult.errors[0].message).toContain('Cannot query field "internalCode"');
  });

  test('default group cannot query any fields - complete denial returns errors', async () => {
    for (const member of ['internalCode', 'tier', 'count']) {
      const result = await graphqlRequest('default', `{
        cube(where: { orders: {} }) {
          orders { ${member} }
        }
      }`);
      expect(result.errors[0].message).toContain('Cannot query field "cube"');
      expect(result.data).toBeUndefined();
    }
  });
});
