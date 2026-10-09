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

type Deferred<T> = {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (error: Error) => void;
};

function deferred<T>(): Deferred<T> {
  let resolve!: Deferred<T>['resolve'];
  let reject!: Deferred<T>['reject'];
  const promise = new Promise<T>((promiseResolve, promiseReject) => {
    resolve = promiseResolve;
    reject = promiseReject;
  });

  return { promise, resolve, reject };
}

describe('useCubeQuery', () => {
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

  function render(query: ReadonlyQueryInput, options: UseCubeQueryOptions) {
    act(() => {
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

  it('keeps the latest result when a superseded load resolves with null last', async () => {
    const firstRequest = deferred<null>();
    const secondRequest = deferred<object>();
    const latestResult = { request: 'latest' };
    const cubeApi = {
      load: jest.fn()
        .mockReturnValueOnce(firstRequest.promise)
        .mockReturnValueOnce(secondRequest.promise),
    };

    render({ measures: ['Orders.count'] }, { cubeApi: cubeApi as never });
    render({ measures: ['Users.count'] }, { cubeApi: cubeApi as never });

    await act(async () => {
      secondRequest.resolve(latestResult);
      await secondRequest.promise;
    });

    expect(hookResult.resultSet).toBe(latestResult);

    await act(async () => {
      firstRequest.resolve(null);
      await firstRequest.promise;
    });

    expect(hookResult.resultSet).toBe(latestResult);
    expect(hookResult.isLoading).toBe(false);
  });

  it('ignores stale load errors, progress, and loading updates', async () => {
    const firstRequest = deferred<object>();
    const secondRequest = deferred<object>();
    const latestProgress = { stage: 'Executing query' };
    const staleProgress = { stage: 'Stale query' };
    const cubeApi = {
      load: jest.fn()
        .mockReturnValueOnce(firstRequest.promise)
        .mockReturnValueOnce(secondRequest.promise),
    };

    render({ measures: ['Orders.count'] }, { cubeApi: cubeApi as never });
    act(() => {
      cubeApi.load.mock.calls[0][1].progressCallback({
        progressResponse: staleProgress,
      });
    });

    expect(hookResult.progress).toBe(staleProgress);

    render({ measures: ['Users.count'] }, { cubeApi: cubeApi as never });

    expect(hookResult.progress).toBeNull();

    act(() => {
      cubeApi.load.mock.calls[1][1].progressCallback({
        progressResponse: latestProgress,
      });
      cubeApi.load.mock.calls[0][1].progressCallback({
        progressResponse: staleProgress,
      });
    });

    expect(hookResult.progress).toBe(latestProgress);

    await act(async () => {
      firstRequest.reject(new Error('stale failure'));
      await firstRequest.promise.catch(() => undefined);
    });

    expect(hookResult.error).toBeNull();
    expect(hookResult.progress).toBe(latestProgress);
    expect(hookResult.isLoading).toBe(true);

    await act(async () => {
      secondRequest.resolve({ request: 'latest' });
      await secondRequest.promise;
    });
  });

  it('settles loading when an in-flight request is invalidated without a replacement', async () => {
    const request = deferred<object>();
    const cubeApi = { load: jest.fn().mockReturnValue(request.promise) };

    render({ measures: ['Orders.count'] }, { cubeApi: cubeApi as never });

    act(() => {
      cubeApi.load.mock.calls[0][1].progressCallback({
        progressResponse: { stage: 'Executing query' },
      });
    });

    expect(hookResult.isLoading).toBe(true);
    expect(hookResult.progress).not.toBeNull();

    render(
      { measures: ['Orders.count'] },
      { cubeApi: cubeApi as never, skip: true }
    );

    expect(hookResult.isLoading).toBe(false);
    expect(hookResult.progress).toBeNull();

    await act(async () => {
      request.resolve({ request: 'stale' });
      await request.promise;
    });

    expect(hookResult.resultSet).toBeNull();
    expect(hookResult.isLoading).toBe(false);
  });

  it('preserves the initial loading state for an empty query', () => {
    const cubeApi = { load: jest.fn() };

    render({}, { cubeApi: cubeApi as never });

    expect(cubeApi.load).not.toHaveBeenCalled();
    expect(hookResult.isLoading).toBe(true);
  });

  it('ignores callbacks from a superseded subscription', () => {
    const callbacks: Array<(error: Error | null, result?: object | null) => void> = [];
    const progressCallbacks: Array<(progress: object) => void> = [];
    const cubeApi = {
      subscribe: jest.fn((query, options, callback) => {
        progressCallbacks.push(options.progressCallback);
        callbacks.push(callback);

        return { unsubscribe: jest.fn() };
      }),
    };
    const latestResult = { request: 'latest' };

    render(
      { measures: ['Orders.count'] },
      { cubeApi: cubeApi as never, subscribe: true }
    );
    render(
      { measures: ['Users.count'] },
      { cubeApi: cubeApi as never, subscribe: true }
    );

    act(() => {
      progressCallbacks[0]({ progressResponse: { stage: 'Stale query' } });
      callbacks[0](null, { request: 'stale' });
    });

    expect(hookResult.resultSet).toBeNull();
    expect(hookResult.progress).toBeNull();
    expect(hookResult.isLoading).toBe(true);

    act(() => callbacks[1](null, latestResult));

    expect(hookResult.resultSet).toBe(latestResult);
    expect(hookResult.isLoading).toBe(false);

    act(() => callbacks[0](new Error('stale failure')));

    expect(hookResult.resultSet).toBe(latestResult);
    expect(hookResult.error).toBeNull();
  });
});
