// https://github.com/cube-js/cube/issues/3473
// useCubeQuery skips a reload when a re-render passes an equal query and an
// equal (deep-compared) cubeApi. Once the request URL is longer than 2000
// characters the transport switches to POST and writes 'Content-Type' into the
// headers object shared with the cubeApi, so the previous cubeApi is no longer
// deep-equal to a freshly built one and every re-render loads again.
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import type { Root } from 'react-dom/client';
import cube from '@cubejs-client/core';

import { useCubeQuery } from '../src/hooks/cube-query';
import type { ReadonlyQueryInput } from '../src/types';

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
  .IS_REACT_ACT_ENVIRONMENT = true;

const loadResponse = {
  queryType: 'regularQuery',
  results: [
    {
      query: { measures: ['Orders.count'] },
      data: [{ 'Orders.count': '1' }],
      annotation: { measures: {}, dimensions: {}, segments: {}, timeDimensions: {} },
    },
  ],
};

const shortQuery: ReadonlyQueryInput = { measures: ['Orders.count'] };
const longQuery: ReadonlyQueryInput = {
  measures: ['Orders.count'],
  filters: [{
    member: 'Orders.id',
    operator: 'equals' as const,
    values: Array.from({ length: 100 }, (_, i) => `a40b2052-4137-11eb-b378-0242ac13${String(i).padStart(4, '0')}`),
  }],
};

describe('issue #3473: duplicate requests with POST', () => {
  let container: HTMLDivElement;
  let root: Root;
  let fetchMock: jest.Mock;
  const originalFetch = globalThis.fetch;

  // A cubeApi built during render, as in the issue: a new but equal instance
  // on every render
  function TestComponent({ query }: { query: ReadonlyQueryInput }) {
    const cubeApi = cube('token', { apiUrl: 'http://localhost:4000/cubejs-api/v1' });
    useCubeQuery(query, { cubeApi });

    return null;
  }

  async function render(query: ReadonlyQueryInput) {
    await act(async () => {
      root.render(<TestComponent query={query} />);
    });
  }

  beforeEach(() => {
    // On master the POST case reloads on every render, and every load
    // re-renders, so stop answering after a few calls to keep the loop finite
    fetchMock = jest.fn(() => (fetchMock.mock.calls.length > 5
      ? new Promise(() => undefined)
      : Promise.resolve({
        status: 200,
        text: async () => JSON.stringify(loadResponse),
      })));
    globalThis.fetch = fetchMock as unknown as typeof fetch;
    container = document.createElement('div');
    root = createRoot(container);
  });

  afterEach(() => {
    act(() => root.unmount());
    globalThis.fetch = originalFetch;
  });

  it('does not reload an unchanged GET query on re-render (control)', async () => {
    await render(shortQuery);
    await render(shortQuery);

    expect(fetchMock.mock.calls.map(([, init]) => init.method)).toEqual(['GET']);
  });

  it('does not reload an unchanged POST query on re-render', async () => {
    await render(longQuery);
    await render(longQuery);

    expect(fetchMock.mock.calls.map(([, init]) => init.method)).toEqual(['POST']);
  });
});
