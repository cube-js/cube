import {
  isQueryPresent,
  type CubeApi,
  type LoadMethodOptions,
  type ProgressResult,
  type ResultSet,
  type SqlQuery,
  type UnsubscribeObj,
} from '@cubejs-client/core';
import { get, writable } from 'svelte/store';

import { tryGetCubeContext } from '../context';
import { CubeClientError, isAbortError, normalizeCubeError } from '../internal/errors';
import {
  areRendererInputsEqual,
  areRendererOptionsEqual,
  deepEqual,
} from '../internal/query-equality';
import { RequestLifecycle } from '../internal/request-lifecycle';
import { asReadable, isBrowser } from '../internal/source';
import type {
  CubeContextValue,
  CubeProviderOptions,
  QueryInput,
  QueryRendererController,
  QueryRendererControllerState,
  QueryRendererInput,
  QueryRendererOptions,
  Source,
} from '../types';

interface CreateQueryRendererControllerOptions {
  input: Source<QueryRendererInput>;
  options: Source<QueryRendererOptions>;
  context: CubeContextValue | null;
}
function toProgress(progress: ProgressResult) {
  return {
    stage: progress.stage(),
    timeElapsed: progress.timeElapsed(),
  };
}

function namedQueriesArePresent(input: QueryRendererInput): boolean {
  if (!('queries' in input) || !input.queries) {
    return false;
  }

  const queries = Object.values(input.queries);
  return queries.length > 0 && queries.every((query) => isQueryPresent(query));
}

export function createQueryRendererController({
  input: inputValue,
  options: optionsValue,
  context,
}: CreateQueryRendererControllerOptions): QueryRendererController {
  const inputSource = asReadable(inputValue);
  const optionsSource = asReadable(optionsValue);
  const state = writable<QueryRendererControllerState>({
    resultSet: null,
    error: null,
    isLoading: false,
    progress: null,
    sqlQuery: null,
  });
  const request = new RequestLifecycle();
  const mutexObj: Record<string, unknown> = {};

  let currentInput = get(inputSource);
  let currentOptions = get(optionsSource);
  let contextCubeApi: CubeApi | null = null;
  let providerOptions: Readonly<CubeProviderOptions> = {};
  let sourceUnsubscribers: Array<() => void> = [];
  let subscriberCount = 0;
  let started = false;
  let destroyed = false;
  let evaluationQueued = false;

  const commit = (
    update: (
      value: QueryRendererControllerState
    ) => QueryRendererControllerState
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

  const execute = async (allowWhileStopped = false): Promise<void> => {
    if (destroyed || (!started && !allowWhileStopped)) {
      return;
    }

    const input = currentInput;
    const options = currentOptions;
    const contextualOptions = context ? get(context.options) : providerOptions;
    const cubeApi =
      options.cubeApi ??
      (context ? get(context.cubeApi) : contextCubeApi);
    const isNamed = 'queries' in input && input.queries !== undefined;
    const present = isNamed
      ? namedQueriesArePresent(input)
      : isQueryPresent(input.query);

    if (options.skip || !present) {
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

    if (isNamed && options.loadSql) {
      void request.cancel();
      commit((value) => ({
        ...value,
        error: new CubeClientError(
          'loadSql is not supported with named queries.'
        ),
        isLoading: false,
        progress: null,
      }));
      return;
    }

    if (isNamed && options.subscribe) {
      void request.cancel();
      commit((value) => ({
        ...value,
        error: new CubeClientError(
          'subscribe is not supported with named queries.'
        ),
        isLoading: false,
        progress: null,
      }));
      return;
    }

    const active = request.begin();
    const resetResultSet = options.resetResultSetOnChange ?? true;
    const commonOptions: LoadMethodOptions = {
      mutexObj,
      signal: active.abortController.signal,
      ...(options.baseRequestId ? { baseRequestId: options.baseRequestId } : {}),
    };
    const loadOptions: LoadMethodOptions = {
      ...commonOptions,
      mutexKey: 'query',
      castNumerics:
        options.castNumerics ?? contextualOptions.castNumerics ?? false,
      ...(options.cache ? { cache: options.cache } : {}),
      progressCallback: (progress) => {
        if (request.isCurrent(active.generation)) {
          commit((value) => ({
            ...value,
            progress: toProgress(progress),
          }));
        }
      },
    };

    commit((value) => ({
      ...value,
      resultSet:
        resetResultSet || options.loadSql === 'only'
          ? null
          : value.resultSet,
      sqlQuery: null,
      error: null,
      isLoading: true,
      progress: null,
    }));

    try {
      if (isNamed) {
        const entries = Object.entries(input.queries!);
        const values = await Promise.all(
          entries.map(async ([name, query]) => {
            const result = (await cubeApi.load(query, {
              ...loadOptions,
              mutexKey: `query:${name}`,
            })) as ResultSet | null;
            return [name, result] as const;
          })
        );

        if (
          !request.isCurrent(active.generation) ||
          values.some(([, result]) => !result)
        ) {
          return;
        }

        commit((value) => ({
          ...value,
          resultSet: Object.fromEntries(values) as Record<string, ResultSet>,
          error: null,
          isLoading: false,
          progress: null,
        }));
        return;
      }

      const query = input.query as QueryInput;

      if (options.loadSql === 'only') {
        const sqlQuery = (await cubeApi.sql(query, {
          ...commonOptions,
          mutexKey: 'sql',
        })) as SqlQuery | SqlQuery[] | null;

        if (!sqlQuery || !request.isCurrent(active.generation)) {
          return;
        }

        commit((value) => ({
          ...value,
          sqlQuery,
          error: null,
          isLoading: false,
          progress: null,
        }));
        return;
      }

      if (options.subscribe) {
        let sqlPromise: Promise<SqlQuery | SqlQuery[] | null> | null = null;
        if (options.loadSql) {
          sqlPromise = cubeApi.sql(query, {
            ...commonOptions,
            mutexKey: 'sql',
          }) as Promise<SqlQuery | SqlQuery[] | null>;
        }

        const subscription: UnsubscribeObj = cubeApi.subscribe(
          query,
          loadOptions,
          (error, result) => {
            if (!request.isCurrent(active.generation)) {
              return;
            }

            if (error) {
              commit((value) => ({
                ...value,
                resultSet: resetResultSet ? null : value.resultSet,
                error: normalizeCubeError(error),
                isLoading: false,
                progress: null,
              }));
            } else if (result) {
              commit((value) => ({
                ...value,
                resultSet: result,
                error: null,
                isLoading: false,
                progress: null,
              }));
            }
          }
        );
        request.setSubscription(active.generation, subscription);

        if (sqlPromise) {
          void sqlPromise
            .then((sqlQuery) => {
              if (sqlQuery && request.isCurrent(active.generation)) {
                commit((value) => ({ ...value, sqlQuery }));
              }
            })
            .catch((error) => {
              if (
                request.isCurrent(active.generation) &&
                !isAbortError(error)
              ) {
                commit((value) => ({
                  ...value,
                  error: normalizeCubeError(error),
                  isLoading: false,
                  progress: null,
                }));
                void request.cancel();
              }
            });
        }
        return;
      }

      if (options.loadSql) {
        const [sqlQuery, resultSet] = (await Promise.all([
          cubeApi.sql(query, {
            ...commonOptions,
            mutexKey: 'sql',
          }),
          cubeApi.load(query, loadOptions),
        ])) as [SqlQuery | SqlQuery[] | null, ResultSet | null];

        if (
          !sqlQuery ||
          !resultSet ||
          !request.isCurrent(active.generation)
        ) {
          return;
        }

        commit(() => ({
          resultSet,
          sqlQuery,
          error: null,
          isLoading: false,
          progress: null,
        }));
        return;
      }

      const resultSet = (await cubeApi.load(
        query,
        loadOptions
      )) as ResultSet | null;

      if (!resultSet || !request.isCurrent(active.generation)) {
        return;
      }

      commit((value) => ({
        ...value,
        resultSet,
        error: null,
        isLoading: false,
        progress: null,
      }));
    } catch (error) {
      if (!request.isCurrent(active.generation) || isAbortError(error)) {
        return;
      }

      commit((value) => ({
        ...value,
        resultSet: resetResultSet ? null : value.resultSet,
        error: normalizeCubeError(error),
        isLoading: false,
        progress: null,
      }));
    }
  };

  const start = () => {
    if (started || destroyed) {
      return;
    }

    started = true;
    sourceUnsubscribers = [
      inputSource.subscribe((value) => {
        if (!areRendererInputsEqual(currentInput, value)) {
          currentInput = value;
          queueEvaluation();
        }
      }),
      optionsSource.subscribe((value) => {
        if (!areRendererOptionsEqual(currentOptions, value)) {
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
    refetch: () => execute(true),
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

export function createQueryRenderer(
  input: Source<QueryRendererInput>,
  options: Source<QueryRendererOptions> = {}
): QueryRendererController {
  return createQueryRendererController({
    input,
    options,
    context: tryGetCubeContext(),
  });
}
