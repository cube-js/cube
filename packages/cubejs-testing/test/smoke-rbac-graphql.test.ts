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

  test('all roles see the same unfiltered schema', async () => {
    const introspectionQuery = '{ __type(name: "OrdersMembers") { fields { name } } }';

    const resultA = await graphqlRequest('tenant-a', introspectionQuery);
    const resultB = await graphqlRequest('tenant-b', introspectionQuery);
    const resultDefault = await graphqlRequest('default', introspectionQuery);

    const fieldsA = resultA.data?.__type?.fields?.map((f: any) => f.name) || [];
    const fieldsB = resultB.data?.__type?.fields?.map((f: any) => f.name) || [];
    const fieldsDefault = resultDefault.data?.__type?.fields?.map((f: any) => f.name) || [];

    expect(fieldsA).toEqual(fieldsB);
    expect(fieldsA).toEqual(fieldsDefault);
    expect(fieldsA).toContain('internalCode');
    expect(fieldsA).toContain('tier');
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
    expect(restrictedResult.errors[0].message).toContain('You requested hidden member');
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
    expect(restrictedResult.errors[0].message).toContain('You requested hidden member');
  });

  test('default role cannot query any fields - complete denial returns errors', async () => {
    const result1 = await graphqlRequest('default', `{
      cube(where: { orders: {} }) {
        orders { internalCode }
      }
    }`);
    expect(result1.errors).toBeDefined();
    expect(result1.errors[0].message).toContain('You requested hidden member');

    const result2 = await graphqlRequest('default', `{
      cube(where: { orders: {} }) {
        orders { tier }
      }
    }`);
    expect(result2.errors).toBeDefined();
    expect(result2.errors[0].message).toContain('You requested hidden member');

    const result3 = await graphqlRequest('default', `{
      cube(where: { orders: {} }) {
        orders { count }
      }
    }`);
    expect(result3.errors).toBeDefined();
    expect(result3.errors[0].message).toContain('You requested hidden member');
  });
});
