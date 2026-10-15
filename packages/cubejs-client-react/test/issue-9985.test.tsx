import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import type { Root } from 'react-dom/client';

import { useCubeQuery } from '../src/hooks/cube-query';
import type { ReadonlyQueryInput, UseCubeQueryInternalResult } from '../src/types';

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
  .IS_REACT_ACT_ENVIRONMENT = true;

// https://github.com/cube-js/cube/issues/9985
describe('issue #9985: useCubeQuery isLoading for an empty query', () => {
  let root: Root;
  const renders: UseCubeQueryInternalResult[] = [];

  function TestComponent({ query, cubeApi }: { query: ReadonlyQueryInput; cubeApi: any }) {
    const result = useCubeQuery(query, { cubeApi });
    renders.push(result);
    return null;
  }

  beforeEach(() => {
    renders.length = 0;
    root = createRoot(document.createElement('div'));
  });

  afterEach(() => {
    act(() => root.unmount());
  });

  it('is not loading when the query is empty', async () => {
    const cubeApi = { load: jest.fn() };
    const emptyQuery = { measures: [], dimensions: [], timeDimensions: [], filters: [] };

    await act(async () => {
      root.render(<TestComponent query={emptyQuery} cubeApi={cubeApi} />);
    });

    expect(cubeApi.load).not.toHaveBeenCalled();
    expect(renders[renders.length - 1].isLoading).toBe(false);
  });
});
