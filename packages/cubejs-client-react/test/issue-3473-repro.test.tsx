/**
 * Repro for https://github.com/cube-js/cube/issues/3473
 * "Duplicate requests aren't caught with useCubeQuery when making POST requests"
 *
 * HttpTransport mutates its `headers` object (adds `Content-Type: application/json`)
 * when it switches to POST. Because useCubeQuery deep-compares its effect deps
 * (including the cubeApi instance, which shares that headers object), a re-render
 * with an equal query and an equal (re-created) cubeApi triggers a duplicate
 * request for POST, while GET is deduplicated.
 */
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import type { Root } from 'react-dom/client';
import cube from '@cubejs-client/core';
import type { CubeApi } from '@cubejs-client/core';

import { useCubeQuery } from '../src/hooks/cube-query';
import CubeProvider from '../src/CubeProvider';
import type { ReadonlyQueryInput } from '../src/types';

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
  .IS_REACT_ACT_ENVIRONMENT = true;

const API_URL = 'http://localhost:4000/cubejs-api/v1';

const shortQuery: ReadonlyQueryInput = { measures: ['Orders.count'] };

// A query whose GET URL exceeds 2000 chars, so HttpTransport switches to POST
const longQuery: ReadonlyQueryInput = {
  measures: ['Orders.count'],
  filters: [{
    member: 'Orders.status',
    operator: 'equals',
    values: Array.from({ length: 200 }, (_, i) => `status_value_${i}`),
  }],
};

function Consumer({ query }: { query: ReadonlyQueryInput }) {
  useCubeQuery(query);
  return null;
}

describe('issue-3473: useCubeQuery duplicate POST requests', () => {
  let container: HTMLDivElement;
  let root: Root;
  let fetchMock: jest.Mock;
  const originalFetch = globalThis.fetch;

  beforeEach(() => {
    container = document.createElement('div');
    root = createRoot(container);
    fetchMock = jest.fn(async () => ({
      status: 400,
      ok: false,
      text: async () => JSON.stringify({ error: 'test error' }),
      json: async () => ({ error: 'test error' }),
    }));
    (globalThis as any).fetch = fetchMock;
  });

  afterEach(() => {
    act(() => root.unmount());
    (globalThis as any).fetch = originalFetch;
  });

  async function renderOnce(query: ReadonlyQueryInput, cubeApi: CubeApi) {
    await act(async () => {
      root.render(
        <CubeProvider cubeApi={cubeApi}>
          <Consumer query={query} />
        </CubeProvider>
      );
    });
    await act(async () => {
      await new Promise((r) => setTimeout(r, 20));
    });
  }

  async function renderTwice(query: ReadonlyQueryInput, makeApi: () => CubeApi) {
    await renderOnce(query, makeApi());
    await renderOnce(query, makeApi());
  }

  const newApi = () => cube('token', { apiUrl: API_URL });

  it('control: GET query + re-created cubeApi on re-render -> single request', async () => {
    await renderTwice(shortQuery, newApi);
    expect(fetchMock.mock.calls.map((c) => c[1].method)).toEqual(['GET']);
  });

  it('control: POST query + stable cubeApi on re-render -> single request', async () => {
    const api = newApi();
    await renderTwice(longQuery, () => api);
    expect(fetchMock.mock.calls.map((c) => c[1].method)).toEqual(['POST']);
  });

  it('POST query + re-created cubeApi on re-render should not issue a duplicate request', async () => {
    await renderTwice(longQuery, newApi);
    expect(fetchMock.mock.calls.map((c) => c[1].method)).toEqual(['POST']);
  });
});
