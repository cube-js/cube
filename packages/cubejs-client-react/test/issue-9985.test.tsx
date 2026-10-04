// Reproduction for https://github.com/cube-js/cube/issues/9985
// `useCubeQuery` `isLoading` is always true for an empty query.
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import type { Root } from 'react-dom/client';

import { useCubeQuery } from '../src/hooks/cube-query';
import type {
  ReadonlyQueryInput,
  UseCubeQueryInternalResult,
  UseCubeQueryOptions,
} from '../src/types';

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
  .IS_REACT_ACT_ENVIRONMENT = true;

describe('useCubeQuery with an empty query (#9985)', () => {
  let container: HTMLDivElement;
  let root: Root;
  let hookResult: UseCubeQueryInternalResult;

  function TestComponent({
    query,
    options,
  }: {
    query: ReadonlyQueryInput;
    options: UseCubeQueryOptions;
  }) {
    hookResult = useCubeQuery(query, options);

    return null;
  }

  async function render(query: ReadonlyQueryInput, options: UseCubeQueryOptions) {
    await act(async () => {
      root.render(<TestComponent query={query} options={options} />);
    });
  }

  beforeEach(() => {
    container = document.createElement('div');
    root = createRoot(container);
  });

  afterEach(() => {
    act(() => root.unmount());
  });

  it('does not stay loading forever for { measures: [], dimensions: [] }', async () => {
    const cubeApi = { load: jest.fn(), subscribe: jest.fn() };

    await render({ measures: [], dimensions: [] }, { cubeApi: cubeApi as never });

    expect(cubeApi.load).not.toHaveBeenCalled();
    expect(hookResult.resultSet).toBeNull();
    expect(hookResult.error).toBeNull();
    expect(hookResult.isLoading).toBe(false);
  });

  it('does not stay loading forever for {}', async () => {
    const cubeApi = { load: jest.fn(), subscribe: jest.fn() };

    await render({}, { cubeApi: cubeApi as never });

    expect(cubeApi.load).not.toHaveBeenCalled();
    expect(hookResult.isLoading).toBe(false);
  });

  it('does not stay loading forever with subscribe: true', async () => {
    const cubeApi = { load: jest.fn(), subscribe: jest.fn() };

    await render(
      { measures: [], dimensions: [], timeDimensions: [] },
      { cubeApi: cubeApi as never, subscribe: true }
    );

    expect(cubeApi.subscribe).not.toHaveBeenCalled();
    expect(hookResult.isLoading).toBe(false);
  });
});
