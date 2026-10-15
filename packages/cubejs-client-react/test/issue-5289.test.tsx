import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import type { Root } from 'react-dom/client';

import { useCubeQuery } from '../src/hooks/cube-query';
import type { ReadonlyQueryInput, UseCubeQueryInternalResult } from '../src/types';

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
  .IS_REACT_ACT_ENVIRONMENT = true;

// https://github.com/cube-js/cube/issues/5289
describe('issue #5289: useCubeQuery stale result with resetResultSetOnChange', () => {
  let root: Root;
  const renders: Array<{ query: ReadonlyQueryInput; result: UseCubeQueryInternalResult }> = [];

  function TestComponent({
    query,
    cubeApi,
    resetResultSetOnChange = true,
  }: { query: ReadonlyQueryInput; cubeApi: any; resetResultSetOnChange?: boolean }) {
    const result = useCubeQuery(query, { cubeApi, resetResultSetOnChange });
    renders.push({ query, result });
    return null;
  }

  beforeEach(() => {
    renders.length = 0;
    root = createRoot(document.createElement('div'));
  });

  afterEach(() => {
    act(() => root.unmount());
  });

  it('never returns the previous query result for a changed query', async () => {
    const firstResult = { request: 'first' };
    const cubeApi = {
      load: jest.fn()
        .mockResolvedValueOnce(firstResult)
        .mockReturnValueOnce(new Promise(() => undefined)),
    };
    const firstQuery = { measures: ['Orders.count'] };
    const secondQuery = {
      measures: ['Orders.count'],
      filters: [{ member: 'Orders.status', operator: 'equals' as const, values: ['completed'] }],
    };

    await act(async () => {
      root.render(<TestComponent query={firstQuery} cubeApi={cubeApi} />);
    });
    expect(renders[renders.length - 1].result.resultSet).toBe(firstResult);

    const firstRenderAfterChange = renders.length;
    await act(async () => {
      root.render(<TestComponent query={secondQuery} cubeApi={cubeApi} />);
    });

    const afterChange = renders.slice(firstRenderAfterChange);
    expect(afterChange.length).toBeGreaterThan(0);
    for (const { query, result } of afterChange) {
      expect(query).toBe(secondQuery);
      expect(result.resultSet).toBeNull();
      expect(result.isLoading).toBe(true);
    }
  });

  it('never returns the previous query error for a changed query', async () => {
    const cubeApi = {
      load: jest.fn()
        .mockRejectedValueOnce(new Error('first failed'))
        .mockReturnValueOnce(new Promise(() => undefined)),
    };
    const firstQuery = { measures: ['Orders.count'] };
    const secondQuery = { measures: ['Orders.total'] };

    await act(async () => {
      root.render(<TestComponent query={firstQuery} cubeApi={cubeApi} />);
    });
    expect(renders[renders.length - 1].result.error?.message).toBe('first failed');

    const firstRenderAfterChange = renders.length;
    await act(async () => {
      root.render(<TestComponent query={secondQuery} cubeApi={cubeApi} />);
    });

    for (const { result } of renders.slice(firstRenderAfterChange)) {
      expect(result.error).toBeNull();
    }
  });

  it('keeps the previous result for a changed query without resetResultSetOnChange', async () => {
    const firstResult = { request: 'first' };
    const cubeApi = {
      load: jest.fn()
        .mockResolvedValueOnce(firstResult)
        .mockReturnValueOnce(new Promise(() => undefined)),
    };
    const firstQuery = { measures: ['Orders.count'] };
    const secondQuery = { measures: ['Orders.total'] };

    await act(async () => {
      root.render(<TestComponent query={firstQuery} cubeApi={cubeApi} resetResultSetOnChange={false} />);
    });

    const firstRenderAfterChange = renders.length;
    await act(async () => {
      root.render(<TestComponent query={secondQuery} cubeApi={cubeApi} resetResultSetOnChange={false} />);
    });

    const afterChange = renders.slice(firstRenderAfterChange);
    expect(afterChange.length).toBeGreaterThan(0);
    for (const { result } of afterChange) {
      expect(result.resultSet).toBe(firstResult);
      expect(result.isLoading).toBe(true);
      expect(result.error).toBeNull();
    }
  });
});
