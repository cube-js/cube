import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import type { Root } from 'react-dom/client';
import cube from '@cubejs-client/core';

import { useCubeQuery } from '../src/hooks/cube-query';
import type { ReadonlyQueryInput } from '../src/types';

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
  .IS_REACT_ACT_ENVIRONMENT = true;

// https://github.com/cube-js/cube/issues/3473
// Guards against HttpTransport writing Content-Type into the shared `headers` object
// on POST: a re-render with an otherwise identical Cube API client then no longer
// deep-equals the memoized deps, and useCubeQuery issues a duplicate request.
describe('issue #3473: useCubeQuery duplicate POST requests', () => {
  let root: Root;
  const fetchMock = jest.fn(() => new Promise<Response>(() => undefined));

  beforeEach(() => {
    fetchMock.mockClear();
    (globalThis as any).fetch = fetchMock;
    root = createRoot(document.createElement('div'));
  });

  afterEach(() => {
    act(() => root.unmount());
  });

  function TestComponent({ query, method }: { query: ReadonlyQueryInput; method: 'GET' | 'POST' }) {
    // A client constructed with identical options on every render
    const cubeApi = cube('token', { apiUrl: 'http://localhost/cubejs-api/v1', method });
    useCubeQuery(query, { cubeApi });
    return null;
  }

  async function renderTwice(method: 'GET' | 'POST') {
    const query = { measures: ['Orders.count'] };
    await act(async () => {
      root.render(<TestComponent query={query} method={method} />);
    });
    await act(async () => {
      root.render(<TestComponent query={{ ...query }} method={method} />);
    });
  }

  it('does not refetch an unchanged query on re-render (GET)', async () => {
    await renderTwice('GET');
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it('does not refetch an unchanged query on re-render (POST)', async () => {
    await renderTwice('POST');
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});
