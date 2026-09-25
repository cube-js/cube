import {
  defaultHeuristics,
  defaultOrder,
  isQueryPresent,
  moveItemInArray,
  movePivotItem,
  ResultSet,
  type ChartType,
  type BinaryOperator,
  type CubeApi,
  type DryRunResponse,
  type Filter,
  type LoadMethodOptions,
  type Meta,
  type PivotConfig,
  type PivotQuery,
  type Query,
  type QueryOrder,
  type TCubeDimension,
  type TCubeMeasure,
  type TCubeSegment,
  type TimeDimension,
  type UnaryOperator,
} from '@cubejs-client/core';
import { get, writable } from 'svelte/store';

import { tryGetCubeContext } from '../context';
import { CubeClientError, isAbortError, normalizeCubeError } from '../internal/errors';
import { isUnaryOperator, sanitizeFilters, updateFilterAtPath } from '../internal/filters';
import {
  deriveMembers,
  emptyDerivedMembers,
  serializeOrderMembers,
  type DerivedMembers,
} from '../internal/members';
import {
  areQueryInputsEqual,
  areVizStatesEqual,
  deepEqual,
} from '../internal/query-equality';
import {
  areSemanticQueriesEqual,
  cleanQuery,
  cloneValue,
  sanitizeValidatedQuery,
} from '../internal/query';
import { RequestLifecycle } from '../internal/request-lifecycle';
import { asReadable, isBrowser } from '../internal/source';
import type {
  CubeContextValue,
  CubeQueryOptions,
  FilterMemberInput,
  FilterUpdate,
  MemberUpdater,
  QueryBuilderActions,
  QueryBuilderOptions,
  QueryBuilderProposedState,
  QueryBuilderState,
  QueryBuilderStore,
  SelectedDimension,
  SelectedMeasure,
  SelectedSegment,
  SelectedTimeDimension,
  Source,
  TimeDimensionUpdate,
  VizState,
} from '../types';
import { createCubeQueryController } from './createCubeQuery';

interface CreateQueryBuilderControllerOptions {
  options: Source<QueryBuilderOptions>;
  context: CubeContextValue | null;
}

interface TransitionUpdate {
  query?: Query;
  chartType?: ChartType;
  pivotConfig?: PivotConfig | null;
}

function emptyAvailableMembers() {
  return {
    measures: [],
    dimensions: [],
    segments: [],
    timeDimensions: [],
  };
}

function derivePatch(derived: DerivedMembers) {
  return {
    measures: derived.measures,
    dimensions: derived.dimensions,
    segments: derived.segments,
    timeDimensions: derived.timeDimensions,
    filters: derived.filters,
    filterTree: derived.filterTree ?? [],
    availableMeasures: derived.availableMeasures,
    availableDimensions: derived.availableDimensions,
    availableTimeDimensions: derived.availableTimeDimensions,
    availableSegments: derived.availableSegments,
    availableMembers: derived.availableMembers,
    availableFilterMembers: derived.availableFilterMembers,
    missingMembers: derived.missingMembers,
    orderMembers: derived.orderMembers,
  };
}

function cloneVizState(state: VizState): VizState {
  return {
    query: cloneValue(state.query),
    chartType: state.chartType,
    pivotConfig: cloneValue(state.pivotConfig),
  };
}

function optionValuesEqual(
  left: QueryBuilderOptions,
  right: QueryBuilderOptions
): boolean {
  return (
    left.cubeApi === right.cubeApi &&
    left.disableHeuristics === right.disableHeuristics &&
    left.stateChangeHeuristics === right.stateChangeHeuristics &&
    left.onVizStateChanged === right.onVizStateChanged &&
    left.executeQuery === right.executeQuery &&
    left.subscribe === right.subscribe &&
    left.resetResultSetOnChange === right.resetResultSetOnChange &&
    left.castNumerics === right.castNumerics &&
    left.cache === right.cache &&
    left.baseRequestId === right.baseRequestId &&
    left.schemaVersion === right.schemaVersion &&
    left.onSchemaChange === right.onSchemaChange
  );
}

function memberName(value: FilterMemberInput): string {
  return typeof value === 'string' ? value : value.name;
}

function toFilter(input: FilterUpdate): Filter | null {
  if ('and' in input || 'or' in input) {
    return sanitizeFilters([cloneValue(input as Filter)])[0] ?? null;
  }

  const raw = input as {
    member?: FilterMemberInput;
    dimension?: FilterMemberInput;
    operator?: BinaryOperator | UnaryOperator;
    values?: string[];
  };
  const rawMember = raw.member ?? raw.dimension;
  if (!rawMember || !raw.operator) {
    return null;
  }

  const name = memberName(rawMember);
  if (isUnaryOperator(raw.operator)) {
    return {
      member: name,
      operator: raw.operator as UnaryOperator,
    };
  }

  return {
    member: name,
    operator: raw.operator,
    values: [...(raw.values ?? [])],
  } as Filter;
}

function normalizePivotConfig(
  query: Query,
  pivotConfig?: PivotConfig | null
): PivotConfig {
  // Core's implementation only reads ordinary Query fields, although its
  // public signature requires the server-only queryType discriminator.
  return ResultSet.getNormalizedPivotConfig(
    query as PivotQuery,
    pivotConfig ?? undefined
  );
}

function toTimeDimension(input: TimeDimensionUpdate): TimeDimension {
  const dimension =
    typeof input.dimension === 'string'
      ? input.dimension
      : input.dimension.name;

  if ('compareDateRange' in input) {
    return {
      dimension,
      ...(input.granularity ? { granularity: input.granularity } : {}),
      compareDateRange: cloneValue(input.compareDateRange),
    };
  }

  return {
    dimension,
    ...(input.granularity ? { granularity: input.granularity } : {}),
    ...(input.dateRange ? { dateRange: cloneValue(input.dateRange) } : {}),
  };
}

export function createQueryBuilderController({
  options: optionsInput,
  context,
}: CreateQueryBuilderControllerOptions): QueryBuilderStore {
  const optionsSource = asReadable(optionsInput);
  let currentOptions = get(optionsSource);
  const initialQuery = cleanQuery(
    cloneValue(
      (currentOptions.initialVizState?.query ??
        currentOptions.defaultQuery ??
        {}) as Query
    )
  );
  const initialChartType =
    currentOptions.initialVizState?.chartType ??
    currentOptions.defaultChartType ??
    'line';
  const initialPivotConfig = normalizePivotConfig(
    initialQuery,
    currentOptions.initialVizState?.pivotConfig
  );
  const initialDerived = emptyDerivedMembers();

  let currentState: QueryBuilderState = {
    query: initialQuery,
    validatedQuery: null,
    isQueryPresent: isQueryPresent(initialQuery),
    isValidated: false,
    chartType: initialChartType,
    pivotConfig: initialPivotConfig,
    meta: null,
    isFetchingMeta: false,
    metaError: null,
    richMetaError: null,
    metaErrorStack: null,
    dryRunError: null,
    loadError: null,
    error: null,
    dryRunResponse: null,
    missingMembers: [],
    measures: [],
    dimensions: [],
    segments: [],
    timeDimensions: [],
    filters: [],
    filterTree: initialQuery.filters ?? [],
    orderMembers: [],
    availableMeasures: [],
    availableDimensions: [],
    availableTimeDimensions: [],
    availableSegments: [],
    availableMembers: emptyAvailableMembers(),
    availableFilterMembers: [],
    resultSet: null,
    isLoading: false,
    loadingState: { isLoading: false },
    progress: null,
  };
  const state = writable<QueryBuilderState>(currentState);
  const metadataRequest = new RequestLifecycle();
  const dryRunRequest = new RequestLifecycle();
  const schemaRequest = new RequestLifecycle();
  const metadataMutex: Record<string, unknown> = {};
  const dryRunMutex: Record<string, unknown> = {};

  const executionQuery = writable<Query>(initialQuery);
  const executionOptions = writable<CubeQueryOptions>({ skip: true });
  const execution = createCubeQueryController({
    query: executionQuery,
    options: executionOptions,
    context,
  });

  let contextCubeApi: CubeApi | null = null;
  let sourceUnsubscribers: Array<() => void> = [];
  let executionUnsubscriber: (() => void) | null = null;
  let subscriberCount = 0;
  let started = false;
  let destroyed = false;
  let metadataFetchQueued = false;
  let dryRunQueued = false;
  let lastDryRunQuery: Query | null = null;
  let orderMemberKeys = initialDerived.orderMemberKeys;
  let sessionGranularity =
    initialQuery.timeDimensions?.[0]?.granularity ?? 'day';
  let shouldApplyHeuristicOrder = false;
  let lastEmittedVizState: VizState = cloneVizState({
    query: initialQuery,
    chartType: initialChartType,
    pivotConfig: initialPivotConfig,
  });

  const commit = (
    update:
      | Partial<QueryBuilderState>
      | ((value: QueryBuilderState) => QueryBuilderState)
  ) => {
    if (destroyed) {
      return;
    }

    const next =
      typeof update === 'function'
        ? update(currentState)
        : { ...currentState, ...update };
    currentState = {
      ...next,
      error: next.loadError ?? next.dryRunError ?? next.metaError,
      loadingState: { isLoading: next.isLoading },
    };
    state.set(currentState);
  };

  const getCubeApi = (): CubeApi | null =>
    currentOptions.cubeApi ??
    (context ? get(context.cubeApi) : contextCubeApi);

  const emitVizState = () => {
    const next: VizState = {
      query: currentState.query,
      chartType: currentState.chartType,
      pivotConfig: currentState.pivotConfig,
    };

    if (!areVizStatesEqual(lastEmittedVizState, next)) {
      lastEmittedVizState = cloneVizState(next);
      currentOptions.onVizStateChanged?.(cloneVizState(next));
    }
  };

  const syncExecution = () => {
    executionQuery.set(currentState.query);
    executionOptions.set({
      cubeApi: currentOptions.cubeApi,
      skip:
        !(currentOptions.executeQuery ?? true) ||
        !currentState.meta ||
        !currentState.isQueryPresent ||
        currentState.missingMembers.length > 0,
      subscribe: currentOptions.subscribe ?? false,
      resetResultSetOnChange:
        currentOptions.resetResultSetOnChange ?? false,
      castNumerics: currentOptions.castNumerics,
      cache: currentOptions.cache,
      baseRequestId: currentOptions.baseRequestId,
    });
  };

  const scheduleDryRun = (force = false) => {
    if (force) {
      lastDryRunQuery = null;
    }
    if (!started || destroyed || dryRunQueued) {
      return;
    }

    dryRunQueued = true;
    queueMicrotask(() => {
      dryRunQueued = false;
      if (started && !destroyed) {
        void runDryRun();
      }
    });
  };

  const runDryRun = async (): Promise<void> => {
    const query = currentState.query;
    const meta = currentState.meta;
    const cubeApi = getCubeApi();

    if (
      !meta ||
      !currentState.isQueryPresent ||
      currentState.missingMembers.length > 0
    ) {
      void dryRunRequest.cancel();
      lastDryRunQuery = null;
      commit({
        validatedQuery: null,
        isValidated: false,
        dryRunError: null,
      });
      return;
    }

    if (lastDryRunQuery && areSemanticQueriesEqual(lastDryRunQuery, query)) {
      return;
    }

    if (!cubeApi) {
      commit({
        validatedQuery: null,
        isValidated: false,
        dryRunError: new CubeClientError('Cube API client is not provided'),
      });
      return;
    }

    const requestedQuery = cloneValue(query);
    const active = dryRunRequest.begin();
    lastDryRunQuery = cloneValue(query);
    commit({
      validatedQuery: null,
      isValidated: false,
      dryRunError: null,
    });

    const loadOptions: LoadMethodOptions = {
      mutexObj: dryRunMutex,
      mutexKey: 'query-builder-dry-run',
      signal: active.abortController.signal,
      ...(currentOptions.baseRequestId
        ? { baseRequestId: currentOptions.baseRequestId }
        : {}),
    };

    try {
      const response = (await cubeApi.dryRun(
        requestedQuery,
        loadOptions
      )) as DryRunResponse | null;

      if (
        !response ||
        !dryRunRequest.isCurrent(active.generation) ||
        !areSemanticQueriesEqual(requestedQuery, currentState.query)
      ) {
        return;
      }

      let nextQuery = currentState.query;
      if (shouldApplyHeuristicOrder) {
        const order = response.queryOrder.flatMap((value) =>
          Object.entries(value)
        ) as Array<[string, QueryOrder]>;
        nextQuery = cleanQuery({ ...nextQuery, order });
      }

      const pivotConfig = ResultSet.getNormalizedPivotConfig(
        response.pivotQuery,
        currentState.pivotConfig
      );
      const derived = deriveMembers(
        meta,
        nextQuery,
        orderMemberKeys
      );
      orderMemberKeys = derived.orderMemberKeys;
      shouldApplyHeuristicOrder = false;

      commit({
        query: nextQuery,
        validatedQuery: sanitizeValidatedQuery(nextQuery),
        isQueryPresent: isQueryPresent(nextQuery),
        isValidated: true,
        pivotConfig,
        dryRunResponse: response,
        dryRunError: null,
        ...derivePatch(derived),
      });
      emitVizState();
      syncExecution();
    } catch (error) {
      if (!dryRunRequest.isCurrent(active.generation) || isAbortError(error)) {
        return;
      }

      lastDryRunQuery = null;
      commit({
        validatedQuery: null,
        isValidated: false,
        dryRunError: normalizeCubeError(error),
      });
    }
  };

  const transition = (
    update: TransitionUpdate,
    applyHeuristics = true
  ) => {
    const previous = currentState;
    let query = cleanQuery(update.query ?? previous.query);
    let chartType = update.chartType ?? previous.chartType;
    let pivotCandidate =
      update.pivotConfig !== undefined
        ? update.pivotConfig
        : previous.pivotConfig;
    shouldApplyHeuristicOrder = false;

    if (
      applyHeuristics &&
      previous.meta &&
      !currentOptions.disableHeuristics
    ) {
      const contextValue = {
        meta: previous.meta,
        sessionGranularity,
      };
      const proposed: QueryBuilderProposedState = {
        query,
        chartType: update.chartType,
        pivotConfig: update.pivotConfig,
      };
      const custom = currentOptions.stateChangeHeuristics?.(
        {
          query: previous.query,
          chartType: previous.chartType,
          pivotConfig: previous.pivotConfig,
        },
        proposed,
        contextValue
      );
      const heuristic =
        custom ??
        defaultHeuristics(
          { query, chartType: update.chartType },
          previous.query,
          contextValue
        );

      query = cleanQuery(heuristic.query ?? query);
      chartType = heuristic.chartType ?? chartType;
      if (heuristic.pivotConfig !== undefined) {
        pivotCandidate = heuristic.pivotConfig;
      }
      if (heuristic.sessionGranularity !== undefined) {
        sessionGranularity = heuristic.sessionGranularity ?? 'day';
      }
      shouldApplyHeuristicOrder = Boolean(
        heuristic.shouldApplyHeuristicOrder
      );
    }

    if (shouldApplyHeuristicOrder) {
      query = cleanQuery({ ...query, order: defaultOrder(query) });
    }

    const queryChanged = !areQueryInputsEqual(previous.query, query);
    const semanticQueryChanged = !areSemanticQueriesEqual(
      previous.query,
      query
    );
    const pivotConfig = normalizePivotConfig(
      query,
      pivotCandidate ?? undefined
    );
    const derived = previous.meta
      ? deriveMembers(previous.meta, query, orderMemberKeys)
      : {
          ...emptyDerivedMembers(),
          filterTree: query.filters ?? [],
          orderMemberKeys,
        };
    orderMemberKeys = derived.orderMemberKeys;

    commit({
      query,
      chartType,
      pivotConfig,
      isQueryPresent: isQueryPresent(query),
      ...(semanticQueryChanged
        ? {
            validatedQuery: null,
            isValidated: false,
            dryRunError: null,
          }
        : queryChanged && previous.isValidated
          ? { validatedQuery: sanitizeValidatedQuery(query) }
        : {}),
      ...derivePatch(derived),
    });
    emitVizState();
    syncExecution();

    if (semanticQueryChanged) {
      scheduleDryRun();
    }
  };

  const scheduleMetadataFetch = () => {
    if (!started || destroyed || metadataFetchQueued) {
      return;
    }

    metadataFetchQueued = true;
    queueMicrotask(() => {
      metadataFetchQueued = false;
      if (started && !destroyed) {
        void fetchMeta();
      }
    });
  };

  const fetchMeta = async (): Promise<void> => {
    const cubeApi = getCubeApi();
    if (!cubeApi) {
      const error = new CubeClientError('Cube API client is not provided');
      commit({
        meta: null,
        isFetchingMeta: false,
        metaError: error,
        richMetaError: error,
        metaErrorStack: null,
      });
      syncExecution();
      return;
    }

    const active = metadataRequest.begin();
    commit({
      isFetchingMeta: true,
      metaError: null,
      richMetaError: null,
      metaErrorStack: null,
    });

    try {
      const meta = (await cubeApi.meta({
        mutexObj: metadataMutex,
        mutexKey: 'query-builder-meta',
        signal: active.abortController.signal,
        ...(currentOptions.baseRequestId
          ? { baseRequestId: currentOptions.baseRequestId }
          : {}),
      })) as Meta | null;

      if (!meta || !metadataRequest.isCurrent(active.generation)) {
        return;
      }

      const derived = deriveMembers(meta, currentState.query, orderMemberKeys);
      orderMemberKeys = derived.orderMemberKeys;
      lastDryRunQuery = null;
      commit({
        meta,
        isFetchingMeta: false,
        metaError: null,
        richMetaError: null,
        metaErrorStack: null,
        ...derivePatch(derived),
      });
      syncExecution();
      scheduleDryRun(true);
    } catch (error) {
      if (!metadataRequest.isCurrent(active.generation) || isAbortError(error)) {
        return;
      }

      const richError = error as {
        message?: string;
        response?: { stack?: string };
      };
      commit({
        meta: null,
        isFetchingMeta: false,
        metaError: normalizeCubeError(error),
        richMetaError: error,
        metaErrorStack:
          richError.response?.stack?.replace(richError.message ?? '', '') ??
          null,
      });
      syncExecution();
    }
  };

  const checkSchemaVersion = async (): Promise<void> => {
    const cubeApi = getCubeApi();
    const schemaVersion = currentOptions.schemaVersion;
    if (!cubeApi || schemaVersion == null || !currentState.meta) {
      return;
    }

    const active = schemaRequest.begin();
    try {
      const meta = (await cubeApi.meta({
        mutexKey: 'query-builder-schema-check',
        signal: active.abortController.signal,
      })) as Meta | null;

      if (
        meta &&
        schemaRequest.isCurrent(active.generation) &&
        currentState.meta &&
        !deepEqual(meta.meta, currentState.meta.meta)
      ) {
        currentOptions.onSchemaChange?.({
          schemaVersion,
          refresh: fetchMeta,
        });
      }
    } catch (error) {
      if (schemaRequest.isCurrent(active.generation) && !isAbortError(error)) {
        commit({ metaError: normalizeCubeError(error) });
      }
    }
  };

  const createNamedUpdater = <
    TMember extends TCubeMeasure | TCubeDimension | TCubeSegment,
    TSelected extends { index: number },
  >(
    field: 'measures' | 'dimensions' | 'segments'
  ): MemberUpdater<TMember, TSelected> => ({
    add(member) {
      transition({
        query: {
          ...currentState.query,
          [field]: [...(currentState.query[field] ?? []), member.name],
        },
      });
    },
    remove(member) {
      const values = currentState.query[field] ?? [];
      if (member.index < 0 || member.index >= values.length) {
        return;
      }
      transition({
        query: {
          ...currentState.query,
          [field]: values.filter((_, index) => index !== member.index),
        },
      });
    },
    update(member, updateWith) {
      const values = [...(currentState.query[field] ?? [])];
      if (member.index < 0 || member.index >= values.length) {
        return;
      }
      values.splice(member.index, 1, updateWith.name);
      transition({ query: { ...currentState.query, [field]: values } });
    },
    set(members) {
      transition({
        query: {
          ...currentState.query,
          [field]: members.map((member) => member.name),
        },
      });
    },
  });

  const timeDimensionUpdater: MemberUpdater<
    TimeDimensionUpdate,
    SelectedTimeDimension
  > = {
    add(value) {
      transition({
        query: {
          ...currentState.query,
          timeDimensions: [
            ...(currentState.query.timeDimensions ?? []),
            toTimeDimension(value),
          ],
        },
      });
    },
    remove(value) {
      const values = currentState.query.timeDimensions ?? [];
      if (value.index < 0 || value.index >= values.length) {
        return;
      }
      transition({
        query: {
          ...currentState.query,
          timeDimensions: values.filter((_, index) => index !== value.index),
        },
      });
    },
    update(value, updateWith) {
      const values = [...(currentState.query.timeDimensions ?? [])];
      if (value.index < 0 || value.index >= values.length) {
        return;
      }
      values.splice(value.index, 1, toTimeDimension(updateWith));
      transition({
        query: { ...currentState.query, timeDimensions: values },
      });
    },
    set(values) {
      transition({
        query: {
          ...currentState.query,
          timeDimensions: values.map(toTimeDimension),
        },
      });
    },
  };

  const filterUpdater: MemberUpdater<FilterUpdate, (typeof currentState.filters)[number]> = {
    add(value) {
      const filter = toFilter(value);
      if (!filter) {
        return;
      }
      transition({
        query: {
          ...currentState.query,
          filters: [...(currentState.query.filters ?? []), filter],
        },
      });
    },
    remove(value) {
      transition({
        query: {
          ...currentState.query,
          filters: updateFilterAtPath(
            currentState.query.filters ?? [],
            value.path,
            null
          ),
        },
      });
    },
    update(value, updateWith) {
      const filter = toFilter(updateWith);
      if (!filter) {
        return;
      }
      transition({
        query: {
          ...currentState.query,
          filters: updateFilterAtPath(
            currentState.query.filters ?? [],
            value.path,
            filter
          ),
        },
      });
    },
    set(values) {
      transition({
        query: {
          ...currentState.query,
          filters: sanitizeFilters(
            values.map(toFilter).filter((value): value is Filter => Boolean(value))
          ),
        },
      });
    },
  };

  const actions: QueryBuilderActions = {
    updateQuery(patch) {
      transition({ query: { ...currentState.query, ...cloneValue(patch) } });
    },
    replaceQuery(query) {
      transition({ query: cloneValue(query) });
    },
    updateMeasures: createNamedUpdater<TCubeMeasure, SelectedMeasure>('measures'),
    updateDimensions: createNamedUpdater<TCubeDimension, SelectedDimension>(
      'dimensions'
    ),
    updateSegments: createNamedUpdater<TCubeSegment, SelectedSegment>('segments'),
    updateTimeDimensions: timeDimensionUpdater,
    updateFilters: filterUpdater,
    updateChartType(chartType) {
      transition({ chartType });
    },
    updateOrder: {
      set(memberId, order = 'asc') {
        const members = currentState.orderMembers.map((member) =>
          member.id === memberId ? { ...member, order } : member
        );
        transition({
          query: {
            ...currentState.query,
            order: serializeOrderMembers(members),
          },
        });
      },
      update(order) {
        transition({ query: { ...currentState.query, order } });
      },
      reorder(sourceIndex, destinationIndex) {
        if (
          sourceIndex < 0 ||
          destinationIndex < 0 ||
          sourceIndex >= currentState.orderMembers.length ||
          destinationIndex >= currentState.orderMembers.length
        ) {
          return;
        }
        const members = moveItemInArray(
          currentState.orderMembers,
          sourceIndex,
          destinationIndex
        );
        orderMemberKeys = members.map((member) => member.id);
        transition({
          query: {
            ...currentState.query,
            order: serializeOrderMembers(members),
          },
        });
      },
    },
    updatePivotConfig: {
      moveItem({
        sourceIndex,
        destinationIndex,
        sourceAxis,
        destinationAxis,
      }) {
        transition(
          {
            pivotConfig: movePivotItem(
              currentState.pivotConfig,
              sourceIndex,
              destinationIndex,
              sourceAxis,
              destinationAxis
            ),
          },
          false
        );
      },
      update(patch) {
        transition(
          {
            pivotConfig: { ...currentState.pivotConfig, ...cloneValue(patch) },
          },
          false
        );
      },
      replace(config) {
        transition({ pivotConfig: cloneValue(config) }, false);
      },
    },
    setLimit(limit) {
      const query = { ...currentState.query };
      if (limit == null) {
        delete query.limit;
      } else {
        query.limit = limit;
      }
      transition({ query });
    },
    setOffset(offset) {
      const query = { ...currentState.query };
      if (offset == null) {
        delete query.offset;
      } else {
        query.offset = offset;
      }
      transition({ query });
    },
    refreshMeta: fetchMeta,
    refetch: () => {
      if (
        !currentState.meta ||
        !currentState.isQueryPresent ||
        currentState.missingMembers.length > 0
      ) {
        return Promise.resolve();
      }

      return execution.refetch({ ignoreSkip: true });
    },
  };

  const start = () => {
    if (started || destroyed) {
      return;
    }

    started = true;
    sourceUnsubscribers = [
      optionsSource.subscribe((value) => {
        if (optionValuesEqual(currentOptions, value)) {
          return;
        }

        const previous = currentOptions;
        currentOptions = value;
        if (previous.cubeApi !== value.cubeApi) {
          void metadataRequest.cancel();
          void dryRunRequest.cancel();
          void schemaRequest.cancel();
          lastDryRunQuery = null;
          commit({
            meta: null,
            validatedQuery: null,
            isValidated: false,
            dryRunResponse: null,
            dryRunError: null,
          });
          syncExecution();
          scheduleMetadataFetch();
        } else {
          syncExecution();
        }
        if (previous.schemaVersion !== value.schemaVersion) {
          void checkSchemaVersion();
        }
      }),
    ];

    if (context) {
      sourceUnsubscribers.push(
        context.cubeApi.subscribe((value) => {
          if (contextCubeApi !== value) {
            contextCubeApi = value;
            if (!currentOptions.cubeApi) {
              void metadataRequest.cancel();
              void dryRunRequest.cancel();
              void schemaRequest.cancel();
              lastDryRunQuery = null;
              commit({
                meta: null,
                validatedQuery: null,
                isValidated: false,
                dryRunResponse: null,
                dryRunError: null,
              });
              syncExecution();
              scheduleMetadataFetch();
            }
          }
        })
      );
    }

    executionUnsubscriber = execution.subscribe((value) => {
      commit({
        resultSet: value.resultSet,
        loadError: value.error,
        isLoading: value.isLoading,
        progress: value.progress,
      });
    });
    execution.start();
    syncExecution();
    scheduleMetadataFetch();
  };

  const stop = async () => {
    started = false;
    metadataFetchQueued = false;
    dryRunQueued = false;
    sourceUnsubscribers.splice(0).forEach((unsubscribe) => unsubscribe());
    executionUnsubscriber?.();
    executionUnsubscriber = null;
    await Promise.all([
      metadataRequest.cancel(),
      dryRunRequest.cancel(),
      schemaRequest.cancel(),
      execution.stop(),
    ]);

    if (!started) {
      commit({
        isFetchingMeta: false,
        isLoading: false,
        progress: null,
      });
    }
  };

  return {
    actions,
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
    start,
    stop,
    async destroy() {
      if (destroyed) {
        return;
      }

      destroyed = true;
      started = false;
      sourceUnsubscribers.splice(0).forEach((unsubscribe) => unsubscribe());
      executionUnsubscriber?.();
      executionUnsubscriber = null;
      await Promise.all([
        metadataRequest.destroy(),
        dryRunRequest.destroy(),
        schemaRequest.destroy(),
        execution.destroy(),
      ]);
    },
  };
}

export function createQueryBuilder(
  options: Source<QueryBuilderOptions> = {}
): QueryBuilderStore {
  return createQueryBuilderController({
    options,
    context: tryGetCubeContext(),
  });
}
