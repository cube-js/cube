import {
  isQueryPresent,
  type CubeApi,
  type DryRunResponse,
  type LoadMethodOptions,
  type Meta,
  type SqlQuery,
} from '@cubejs-client/core';
import { get, writable } from 'svelte/store';

import { tryGetCubeContext } from '../context';
import { CubeClientError, isAbortError, normalizeCubeError } from '../internal/errors';
import { areQueryInputsEqual } from '../internal/query-equality';
import { RequestLifecycle } from '../internal/request-lifecycle';
import { asReadable, isBrowser } from '../internal/source';
import type {
  CubeContextValue,
  CubeFetchOptions,
  CubeFetchState,
  CubeFetchStore,
  QueryFetchOptions,
  QueryFetchRefetchOptions,
  QueryInput,
  Source,
} from '../types';

type CubeFetchMethod = 'meta' | 'sql' | 'dryRun';

interface CreateCubeFetchControllerOptions {
  method: CubeFetchMethod;
  query?: Source<QueryInput>;
  options: Source<CubeFetchOptions>;
  context: CubeContextValue | null;
  autoExecute: boolean;
}

function areFetchOptionsEqual(
  left: CubeFetchOptions,
  right: CubeFetchOptions
): boolean {
  return (
    left.cubeApi === right.cubeApi &&
    left.skip === right.skip &&
    left.baseRequestId === right.baseRequestId
  );
}

function createCubeFetchController<T>({
  method,
  query: queryInput,
  options: optionsInput,
  context,
  autoExecute,
}: CreateCubeFetchControllerOptions): CubeFetchStore<
  T,
  QueryFetchRefetchOptions
> {
  const querySource = queryInput ? asReadable(queryInput) : null;
  const optionsSource = asReadable(optionsInput);
  const state = writable<CubeFetchState<T>>({
    response: null,
    error: null,
    isLoading: false,
  });
  const request = new RequestLifecycle();
  const mutexObj: Record<string, unknown> = {};

  let currentQuery = querySource ? get(querySource) : null;
  let currentOptions = get(optionsSource);
  let contextCubeApi: CubeApi | null = null;
  let sourceUnsubscribers: Array<() => void> = [];
  let subscriberCount = 0;
  let started = false;
  let destroyed = false;
  let evaluationQueued = false;

  const commit = (update: (value: CubeFetchState<T>) => CubeFetchState<T>) => {
    if (!destroyed) {
      state.update(update);
    }
  };

  const queueEvaluation = () => {
    if (!autoExecute || !started || destroyed || evaluationQueued) {
      return;
    }

    evaluationQueued = true;
    queueMicrotask(() => {
      evaluationQueued = false;
      if (started && !destroyed) {
        void execute();
      }
    });
  };

  const execute = async (
    override: QueryFetchRefetchOptions = {},
    allowWhileStopped = false
  ): Promise<void> => {
    if (destroyed || (!started && !allowWhileStopped)) {
      return;
    }

    const query = override.query ?? currentQuery;
    const options = currentOptions;
    const cubeApi =
      options.cubeApi ?? (context ? get(context.cubeApi) : contextCubeApi);
    const skip = Boolean(options.skip && !override.ignoreSkip);

    if (skip || (method !== 'meta' && (!query || !isQueryPresent(query)))) {
      void request.cancel();
      commit((value) => ({
        ...value,
        error: null,
        isLoading: false,
      }));
      return;
    }

    if (!cubeApi) {
      void request.cancel();
      commit((value) => ({
        ...value,
        error: new CubeClientError('Cube API client is not provided'),
        isLoading: false,
      }));
      return;
    }

    const active = request.begin();
    const baseRequestId = override.baseRequestId ?? options.baseRequestId;
    const loadOptions: LoadMethodOptions = {
      mutexObj,
      mutexKey: method,
      signal: active.abortController.signal,
      ...(baseRequestId ? { baseRequestId } : {}),
    };

    commit((value) => ({
      ...value,
      response: null,
      error: null,
      isLoading: true,
    }));

    try {
      let response: T | null;

      if (method === 'meta') {
        response = (await cubeApi.meta(loadOptions)) as T | null;
      } else if (method === 'sql') {
        response = (await cubeApi.sql(query!, loadOptions)) as T | null;
      } else {
        response = (await cubeApi.dryRun(query!, loadOptions)) as T | null;
      }

      if (!response || !request.isCurrent(active.generation)) {
        return;
      }

      commit(() => ({
        response,
        error: null,
        isLoading: false,
      }));
    } catch (error) {
      if (!request.isCurrent(active.generation) || isAbortError(error)) {
        return;
      }

      commit(() => ({
        response: null,
        error: normalizeCubeError(error),
        isLoading: false,
      }));
    }
  };

  const start = () => {
    if (started || destroyed) {
      return;
    }

    started = true;
    sourceUnsubscribers = [
      optionsSource.subscribe((value) => {
        if (!areFetchOptionsEqual(currentOptions, value)) {
          currentOptions = value;
          queueEvaluation();
        }
      }),
    ];

    if (querySource) {
      sourceUnsubscribers.push(
        querySource.subscribe((value) => {
          if (!areQueryInputsEqual(currentQuery, value)) {
            currentQuery = value;
            queueEvaluation();
          }
        })
      );
    }

    if (context) {
      sourceUnsubscribers.push(
        context.cubeApi.subscribe((value) => {
          if (contextCubeApi !== value) {
            contextCubeApi = value;
            queueEvaluation();
          }
        })
      );
    }

    queueEvaluation();
  };

  const stop = async () => {
    started = false;
    evaluationQueued = false;
    sourceUnsubscribers.splice(0).forEach((unsubscribe) => unsubscribe());
    await request.cancel();

    if (!started) {
      commit((value) => ({ ...value, isLoading: false }));
    }
  };

  return {
    subscribe(run, invalidate) {
      subscriberCount += 1;
      const unsubscribe = state.subscribe(run, invalidate);

      if (subscriberCount === 1 && isBrowser()) {
        start();
      }

      return () => {
        unsubscribe();
        subscriberCount = Math.max(0, subscriberCount - 1);
        if (subscriberCount === 0) {
          void stop();
        }
      };
    },
    refetch: (override = {}) =>
      execute(
        { ...override, ignoreSkip: override.ignoreSkip ?? true },
        true
      ),
    start,
    stop,
    async destroy() {
      if (destroyed) {
        return;
      }

      destroyed = true;
      started = false;
      evaluationQueued = false;
      sourceUnsubscribers.splice(0).forEach((unsubscribe) => unsubscribe());
      await request.destroy();
    },
  };
}

export function createCubeMeta(
  options: Source<CubeFetchOptions> = {}
): CubeFetchStore<Meta, QueryFetchRefetchOptions> {
  return createCubeFetchController<Meta>({
    method: 'meta',
    options,
    context: tryGetCubeContext(),
    autoExecute: true,
  });
}

export function createCubeSql(
  query: Source<QueryInput>,
  options: Source<CubeFetchOptions> = {}
): CubeFetchStore<SqlQuery | SqlQuery[], QueryFetchRefetchOptions> {
  return createCubeFetchController<SqlQuery | SqlQuery[]>({
    method: 'sql',
    query,
    options,
    context: tryGetCubeContext(),
    autoExecute: true,
  });
}

export function createDryRun(
  query: Source<QueryInput>,
  options: Source<CubeFetchOptions> = {}
): CubeFetchStore<DryRunResponse, QueryFetchRefetchOptions> {
  return createCubeFetchController<DryRunResponse>({
    method: 'dryRun',
    query,
    options,
    context: tryGetCubeContext(),
    autoExecute: true,
  });
}

export function createLazyDryRun(
  query: Source<QueryInput> = {},
  options: Source<CubeFetchOptions> = {}
): CubeFetchStore<DryRunResponse, QueryFetchRefetchOptions> & {
  run(options?: QueryFetchRefetchOptions): Promise<void>;
} {
  const store = createCubeFetchController<DryRunResponse>({
    method: 'dryRun',
    query,
    options,
    context: tryGetCubeContext(),
    autoExecute: false,
  });

  return {
    ...store,
    run: store.refetch,
  };
}

export type { QueryFetchOptions };
