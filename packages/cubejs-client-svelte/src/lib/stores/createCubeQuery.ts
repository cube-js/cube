import {
  isQueryPresent,
  type CubeApi,
  type LoadMethodOptions,
  type ProgressResult,
  type QueryRecordType,
  type ResultSet,
} from '@cubejs-client/core';
import { get, writable } from 'svelte/store';

import { tryGetCubeContext } from '../context';
import { isAbortError, normalizeCubeError, CubeClientError } from '../internal/errors';
import {
  areCubeQueryOptionsEqual,
  areQueryInputsEqual,
  deepEqual,
} from '../internal/query-equality';
import { RequestLifecycle } from '../internal/request-lifecycle';
import { asReadable, isBrowser } from '../internal/source';
import type {
  CubeContextValue,
  CubeProviderOptions,
  CubeQueryOptions,
  CubeQueryRefetchOptions,
  CubeQueryState,
  CubeQueryStore,
  QueryInput,
  Source,
} from '../types';

interface CreateCubeQueryControllerOptions<TQuery extends QueryInput> {
  query: Source<TQuery>;
  options: Source<CubeQueryOptions>;
  context: CubeContextValue | null;
}

function toProgress(progress: ProgressResult) {
  return {
    stage: progress.stage(),
    timeElapsed: progress.timeElapsed(),
  };
}

export function createCubeQueryController<TQuery extends QueryInput>({
  query: queryInput,
  options: optionsInput,
  context,
}: CreateCubeQueryControllerOptions<TQuery>): CubeQueryStore<TQuery> {
  const querySource = asReadable(queryInput);
  const optionsSource = asReadable(optionsInput);
  const state = writable<CubeQueryState<TQuery>>({
    query: get(querySource),
    previousQuery: null,
    resultSet: null,
    error: null,
    isLoading: false,
    progress: null,
  });
  const request = new RequestLifecycle();
  const mutexObj: Record<string, unknown> = {};

  let currentQuery = get(querySource);
  let currentOptions = get(optionsSource);
  let contextCubeApi: CubeApi | null = null;
  let providerOptions: Readonly<CubeProviderOptions> = {};
  let sourceUnsubscribers: Array<() => void> = [];
  let subscriberCount = 0;
  let started = false;
  let destroyed = false;
  let evaluationQueued = false;

  const commit = (
    update: (value: CubeQueryState<TQuery>) => CubeQueryState<TQuery>
  ) => {
    if (!destroyed) {
      state.update(update);
    }
  };

  const queueEvaluation = () => {
    if (!started || destroyed || evaluationQueued) {
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

  const handleFailure = (
    generation: number,
    error: unknown,
    resetResultSet: boolean
  ) => {
    if (!request.isCurrent(generation) || isAbortError(error)) {
      return;
    }

    commit((value) => ({
      ...value,
      resultSet: resetResultSet ? null : value.resultSet,
      error: normalizeCubeError(error),
      isLoading: false,
      progress: null,
    }));
  };

  const execute = async (
    override: CubeQueryRefetchOptions<TQuery> = {},
    allowWhileStopped = false
  ): Promise<void> => {
    if (destroyed || (!started && !allowWhileStopped)) {
      return;
    }

    const query = override.query ?? currentQuery;
    const options = currentOptions;
    const contextualCubeApi = context ? get(context.cubeApi) : contextCubeApi;
    const contextualOptions = context ? get(context.options) : providerOptions;
    const cubeApi = options.cubeApi ?? contextualCubeApi;
    const skip = Boolean(options.skip && !override.ignoreSkip);

    commit((value) => ({ ...value, query }));

    if (skip || !isQueryPresent(query)) {
      void request.cancel();
      commit((value) => ({
        ...value,
        error: null,
        isLoading: false,
        progress: null,
      }));
      return;
    }

    if (!cubeApi) {
      void request.cancel();
      commit((value) => ({
        ...value,
        error: new CubeClientError('Cube API client is not provided'),
        isLoading: false,
        progress: null,
      }));
      return;
    }

    const active = request.begin();
    const resetResultSet = options.resetResultSetOnChange ?? true;
    const baseRequestId = override.baseRequestId ?? options.baseRequestId;

    commit((value) => ({
      ...value,
      previousQuery: query,
      resultSet: resetResultSet ? null : value.resultSet,
      error: null,
      isLoading: true,
      progress: null,
    }));

    const loadOptions: LoadMethodOptions = {
      mutexObj,
      mutexKey: 'query',
      signal: active.abortController.signal,
      castNumerics:
        options.castNumerics ?? contextualOptions.castNumerics ?? false,
      ...(options.cache ? { cache: options.cache } : {}),
      ...(baseRequestId ? { baseRequestId } : {}),
      progressCallback: (progress) => {
        if (request.isCurrent(active.generation)) {
          commit((value) => ({
            ...value,
            progress: toProgress(progress),
          }));
        }
      },
    };

    if (options.subscribe) {
      try {
        const subscription = cubeApi.subscribe(
          query,
          loadOptions,
          (error, result) => {
            if (!request.isCurrent(active.generation)) {
              return;
            }

            if (error) {
              handleFailure(active.generation, error, resetResultSet);
            } else if (result) {
              commit((value) => ({
                ...value,
                resultSet: result as ResultSet<QueryRecordType<TQuery>>,
                error: null,
                isLoading: false,
                progress: null,
              }));
            }
          }
        );
        request.setSubscription(active.generation, subscription);
      } catch (error) {
        handleFailure(active.generation, error, resetResultSet);
      }
      return;
    }

    try {
      const result = (await cubeApi.load(
        query,
        loadOptions
      )) as ResultSet<QueryRecordType<TQuery>> | null;

      if (!result || !request.isCurrent(active.generation)) {
        return;
      }

      commit((value) => ({
        ...value,
        resultSet: result,
        error: null,
        isLoading: false,
        progress: null,
      }));
    } catch (error) {
      handleFailure(active.generation, error, resetResultSet);
    }
  };

  const start = () => {
    if (started || destroyed) {
      return;
    }

    started = true;
    sourceUnsubscribers = [
      querySource.subscribe((value) => {
        if (!areQueryInputsEqual(currentQuery, value)) {
          currentQuery = value;
          queueEvaluation();
        }
      }),
      optionsSource.subscribe((value) => {
        if (!areCubeQueryOptionsEqual(currentOptions, value)) {
          currentOptions = value;
          queueEvaluation();
        }
      }),
    ];

    if (context) {
      sourceUnsubscribers.push(
        context.cubeApi.subscribe((value) => {
          if (contextCubeApi !== value) {
            contextCubeApi = value;
            queueEvaluation();
          }
        }),
        context.options.subscribe((value) => {
          if (!deepEqual(providerOptions, value)) {
            providerOptions = value;
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
      commit((value) => ({
        ...value,
        isLoading: false,
        progress: null,
      }));
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
        {
          ...override,
          ignoreSkip: override.ignoreSkip ?? true,
        },
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

export function createCubeQuery<TQuery extends QueryInput>(
  query: Source<TQuery>,
  options: Source<CubeQueryOptions> = {}
): CubeQueryStore<TQuery> {
  return createCubeQueryController({
    query,
    options,
    context: tryGetCubeContext(),
  });
}
