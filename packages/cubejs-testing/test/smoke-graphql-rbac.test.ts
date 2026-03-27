/**
 * Integration test for GraphQL schema caching per security context.
 *
 * This test verifies that when different users (with different RBAC roles)
 * make GraphQL requests, they each get a schema appropriate to their
 * access level - not a cached schema from another user.
 *
 * The fix uses `visibilityMaskHash` as the cache key for GraphQL schemas.
 */

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

describe('GraphQL Schema Caching per Security Context', () => {
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
        schemaDir: 'graphql-rbac/model',
        cubejsConfig: 'graphql-rbac/cube.js',
      }
    );
  }, JEST_BEFORE_ALL_DEFAULT_TIMEOUT);

  afterAll(async () => {
    await birdbox.stop();
  }, JEST_AFTER_ALL_DEFAULT_TIMEOUT);

  async function graphqlIntrospection(role: string): Promise<any> {
    const token = sign({
      auth: { roles: [role] },
    }, DEFAULT_CONFIG.CUBEJS_API_SECRET, { expiresIn: '1h' });

    // apiUrl includes /cubejs-api/v1, but GraphQL is at /cubejs-api/graphql
    const baseUrl = birdbox.configuration.apiUrl.replace('/cubejs-api/v1', '');
    const res = await fetch(`${baseUrl}/cubejs-api/graphql`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify({
        query: '{ __type(name: "OrdersMembers") { fields { name } } }',
      }),
    });
    return res.json();
  }

  test('tenant-a sees internalCode but not tier', async () => {
    const result = await graphqlIntrospection('tenant-a');
    const fields = result.data?.__type?.fields?.map((f: any) => f.name) || [];

    expect(fields).toContain('internalCode');
    expect(fields).not.toContain('tier');
  });

  test('tenant-b sees tier but not internalCode', async () => {
    const result = await graphqlIntrospection('tenant-b');
    const fields = result.data?.__type?.fields?.map((f: any) => f.name) || [];

    expect(fields).toContain('tier');
    expect(fields).not.toContain('internalCode');
  });

  test('alternating requests maintain correct schemas', async () => {
    // Request A -> B -> A -> B to verify caching works correctly
    const resultA1 = await graphqlIntrospection('tenant-a');
    const resultB1 = await graphqlIntrospection('tenant-b');
    const resultA2 = await graphqlIntrospection('tenant-a');
    const resultB2 = await graphqlIntrospection('tenant-b');

    const fieldsA1 = resultA1.data?.__type?.fields?.map((f: any) => f.name) || [];
    const fieldsB1 = resultB1.data?.__type?.fields?.map((f: any) => f.name) || [];
    const fieldsA2 = resultA2.data?.__type?.fields?.map((f: any) => f.name) || [];
    const fieldsB2 = resultB2.data?.__type?.fields?.map((f: any) => f.name) || [];

    // A should always see internalCode, never tier
    expect(fieldsA1).toContain('internalCode');
    expect(fieldsA1).not.toContain('tier');
    expect(fieldsA2).toEqual(fieldsA1);

    // B should always see tier, never internalCode
    expect(fieldsB1).toContain('tier');
    expect(fieldsB1).not.toContain('internalCode');
    expect(fieldsB2).toEqual(fieldsB1);
  });

  test('default role sees neither internalCode nor tier', async () => {
    const result = await graphqlIntrospection('default');
    const fields = result.data?.__type?.fields?.map((f: any) => f.name) || [];

    expect(fields).not.toContain('internalCode');
    expect(fields).not.toContain('tier');
    // Should still see basic fields
    expect(fields).toContain('count');
    expect(fields).toContain('amount');
  });
});
