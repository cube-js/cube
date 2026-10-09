// https://github.com/cube-js/cube/issues/5289
// useCubeQuery returns the previous query's resultSet (with isLoading=false) on the
// first render after the query changes, even with resetResultSetOnChange: true.
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import type { Root } from 'react-dom/client';

import { useCubeQuery } from '../src/hooks/cube-query';
import type { ReadonlyQueryInput, UseCubeQueryOptions } from '../src/types';

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
  .IS_REACT_ACT_ENVIRONMENT = true;

type RenderSnapshot = {
  query: ReadonlyQueryInput;
  resultSet: unknown;
  isLoading: boolean;
};

describe('useCubeQuery (issue #5289)', () => {
  let container: HTMLDivElement;
  let root: Root;
  let renders: RenderSnapshot[];

  function TestComponent({ query, options }: { query: ReadonlyQueryInput; options: UseCubeQueryOptions }) {
    const { resultSet, isLoading } = useCubeQuery(query, options);
    renders.push({ query, resultSet, isLoading });
    return null;
  }

  beforeEach(() => {
    renders = [];
    container = document.createElement('div');
    root = createRoot(container);
  });

  afterEach(() => {
    act(() => root.unmount());
  });

  it('never returns the previous query result for a new query when resetResultSetOnChange is set', async () => {
    const firstResult = { request: 'first' };
    const secondResult = { request: 'second' };
    let resolveSecond!: (v: object) => void;
    const cubeApi = {
      load: jest.fn()
        .mockResolvedValueOnce(firstResult)
        .mockReturnValueOnce(new Promise((resolve) => { resolveSecond = resolve; })),
    };
    const options: UseCubeQueryOptions = { cubeApi: cubeApi as never, resetResultSetOnChange: true };

    const firstQuery = { measures: ['Orders.count'] };
    const secondQuery = {
      measures: ['Orders.count'],
      filters: [{ member: 'Orders.status', operator: 'equals' as const, values: ['shipped'] }],
    };

    await act(async () => {
      root.render(<TestComponent query={firstQuery} options={options} />);
    });
    expect(renders[renders.length - 1].resultSet).toBe(firstResult);

    renders = [];
    await act(async () => {
      root.render(<TestComponent query={secondQuery} options={options} />);
    });

    // Every render with the new query must not expose the result of the old one.
    const stale = renders.filter((r) => r.query === secondQuery && r.resultSet === firstResult);
    expect(stale).toEqual([]);
    // While the new query is in flight, the hook must report loading.
    expect(renders.every((r) => r.isLoading || r.resultSet === secondResult)).toBe(true);

    await act(async () => {
      resolveSecond(secondResult);
    });
    expect(renders[renders.length - 1].resultSet).toBe(secondResult);
  });
});
