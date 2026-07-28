import React from 'react';
import { clone, equals, indexBy, pick, prop, uniq, uniqBy } from 'ramda';
import {
  defaultHeuristics,
  defaultOrder,
  flattenFilters,
  getQueryMembers,
  isQueryPresent,
  moveItemInArray,
  movePivotItem,
  validateQuery,
  ResultSet,
  removeEmptyQueryFields
} from '@cubejs-client/core';
import type {
  CubeApi,
  CubeMember,
  DryRunResponse,
  Meta,
  NotFoundMember,
  PivotConfig,
  PivotQuery,
  Query,
  QueryOrder,
  RequestError,
  TCubeDimension,
  TCubeMeasure,
  TCubeSegment,
  TDefaultHeuristicsState,
  TOrderMember,
} from '@cubejs-client/core';

import QueryRenderer from './QueryRenderer';
import CubeContext from './CubeContext';
import { removeEmpty } from './utils';
import type {
  AvailableCube,
  AvailableMembers,
  ChartType,
  CubeContextProps,
  FilterUpdateFields,
  FilterUpdateInput,
  GranularityOption,
  IndexedDimension,
  IndexedMeasure,
  IndexedSegment,
  MemberUpdater,
  MemberUpdaterFactory,
  MetaErrorSource,
  MutexObj,
  PivotConfigUpdaterArgs,
  QueryBuilderInternalState,
  QueryBuilderProps,
  QueryBuilderRenderProps,
  QueryBuilderState,
  QueryBuilderStateUpdate,
  QueryMemberType,
  QueryRendererRenderProps,
  ResolvedFilter,
  ResolveMemberArgs,
  ResolvedTimeDimension,
  TimeDimensionComparisonUpdateFields,
  TimeDimensionRangedUpdateFields,
  TimeDimensionUpdateInput,
  TimeDimensionWithExtraFields,
} from './types';

const granularities: GranularityOption[] = [
  { name: undefined, title: 'w/o grouping' },
  { name: 'second', title: 'Second' },
  { name: 'minute', title: 'Minute' },
  { name: 'hour', title: 'Hour' },
  { name: 'day', title: 'Day' },
  { name: 'week', title: 'Week' },
  { name: 'month', title: 'Month' },
  { name: 'quarter', title: 'Quarter' },
  { name: 'year', title: 'Year' },
];

export default class QueryBuilder extends React.Component<QueryBuilderProps, QueryBuilderInternalState> {
  static contextType = CubeContext;

  static defaultProps = {
    cubeApi: null,
    stateChangeHeuristics: null,
    disableHeuristics: false,
    render: null,
    wrapWithQueryRenderer: true,
    defaultChartType: 'line',
    defaultQuery: {},
    initialVizState: null,
    onVizStateChanged: null,

    // deprecated
    query: null,
    setQuery: null,
    vizState: null,
    setVizState: null,
  };

  // This is an anti-pattern, only kept for backward compatibility
  // https://reactjs.org/blog/2018/06/07/you-probably-dont-need-derived-state.html#anti-pattern-unconditionally-copying-props-to-state
  static getDerivedStateFromProps(props: QueryBuilderProps, state: QueryBuilderInternalState) {
    if (props.query || props.vizState) {
      const nextState = {
        ...state,
        ...(props.vizState || {}),
      };

      if (Array.isArray(props.query)) {
        throw new Error('Array of queries is not supported.');
      }

      return {
        ...nextState,
        query: {
          ...nextState.query,
          ...(props.query || {}),
        },
      };
    }
    return null;
  }

  static resolveMember(
    type: QueryMemberType,
    { meta, query }: ResolveMemberArgs
  ): unknown[] {
    if (!meta) {
      return [];
    }

    if (Array.isArray(query)) {
      return query.reduce<unknown[]>(
        (memo, currentQuery) => memo.concat(
          QueryBuilder.resolveMember(type, {
            meta,
            query: currentQuery,
          })
        ),
        []
      );
    }

    if (type === 'timeDimensions') {
      return (query.timeDimensions || []).map((m, index) => ({
        ...m,
        dimension: {
          ...(meta.resolveMember(m.dimension, 'dimensions') as TCubeDimension),
          granularities,
        },
        index,
      }));
    }

    return (query[type] || []).map((m, index) => ({
      index,
      ...(meta.resolveMember(m, type) as CubeMember),
    }));
  }

  constructor(props: QueryBuilderProps) {
    super(props);

    this.state = {
      query: (props.defaultQuery || props.query) as Query,
      chartType: props.defaultChartType,
      validatedQuery: props.query, // deprecated, validatedQuery should not be set until after dry-run for safety
      missingMembers: [],
      // todo: rename to `isMetaReady`
      isFetchingMeta: true,
      dryRunResponse: null,
      ...props.vizState, // deprecated
      ...props.initialVizState,
    };

    this.mutexObj = {};
    this.orderMembersOrderKeys = [];
  }

  async componentDidMount() {
    this.prevContext = this.context;
    await this.fetchMeta();
  }

  async componentDidUpdate(prevProps: QueryBuilderProps) {
    const { schemaVersion, onSchemaChange } = this.props;
    const { meta } = this.state;

    if (this.prevContext?.cubeApi !== this.context?.cubeApi) {
      this.prevContext = this.context;
      await this.fetchMeta();
    }

    if (prevProps.schemaVersion !== schemaVersion) {
      try {
        const newMeta = await this.cubeApi().meta();
        if (!equals(newMeta, meta) && typeof onSchemaChange === 'function') {
          onSchemaChange({
            schemaVersion: schemaVersion as number,
            refresh: async () => {
              await this.fetchMeta();
            },
          });
        }
      } catch (error) {
        // eslint-disable-next-line
        this.setState({ metaError: error as Error });
      }
    }
  }

  // `this.context` is typed as `any` by React and holds `CubeContextProps`.
  // It is not re-declared here: a class field would be emitted at runtime and
  // shadow the context React assigns.
  private mutexObj: MutexObj;

  private orderMembersOrderKeys: string[];

  private prevContext?: CubeContextProps;

  fetchMeta = async () => {
    if (!this.cubeApi()) {
      return;
    }

    let meta: Meta | undefined;
    let metaError: MetaErrorSource | null = null;
    let richMetaError: Error | null = null;
    let metaErrorStack: string | null = null;

    try {
      this.setState({ isFetchingMeta: true });
      meta = await this.cubeApi().meta();
    } catch (error) {
      const requestError = error as RequestError;

      metaError = requestError.response?.plainError || requestError;
      richMetaError = requestError;
      metaErrorStack = requestError.response?.stack?.replace(requestError.message || '', '') || '';
    }

    this.setState(
      {
        meta,
        metaError: metaError ? new Error(metaError.message || metaError.toString()) : null,
        richMetaError,
        metaErrorStack,
        isFetchingMeta: false,
      },
      () => {
        // Run update query to force viz state update
        // This will catch any new missing members, and also validate the query against the new meta
        this.updateQuery({});
      }
    );
  };

  cubeApi(): CubeApi {
    const { cubeApi } = this.props;
    // eslint-disable-next-line react/destructuring-assignment
    return cubeApi || (this.context && this.context.cubeApi);
  }

  getMissingMembers(query: Query, meta?: Meta) {
    if (!meta) {
      return [];
    }

    return getQueryMembers(query)
      .map((member) => {
        const resolvedMember = meta.resolveMember(member, ['measures', 'dimensions', 'segments']) as NotFoundMember;
        if (resolvedMember.error) {
          return member;
        }
        return false;
      })
      .filter(Boolean) as string[];
  }

  isQueryPresent() {
    const { query } = this.state;
    return QueryRenderer.isQueryPresent(query);
  }

  prepareRenderProps(queryRendererProps?: QueryRendererRenderProps): QueryBuilderRenderProps {
    const getName = (member: unknown) => (member as CubeMember).name;

    const toTimeDimension = (member: TimeDimensionUpdateInput) => {
      const rangeSelection = member.compareDateRange
        ? { compareDateRange: member.compareDateRange }
        : { dateRange: member.dateRange };

      return removeEmpty({
        dimension: member.dimension.name,
        granularity: member.granularity,
        ...rangeSelection,
      });
    };

    const toFilter = (member: FilterUpdateInput) => ({
      member: member.member?.name || member.dimension?.name,
      operator: member.operator,
      ...(['set', 'notSet'].includes(member.operator) ? {} : { values: member.values }),
    });

    const updateMethods: MemberUpdaterFactory = (memberType, toQuery = getName) => ({
      add: (member) => {
        const { query } = this.state;
        this.updateQuery({
          [memberType]: ((query[memberType] || []) as unknown[]).concat(toQuery(member)),
        } as Query);
      },
      remove: (member) => {
        const { query } = this.state;

        return this.updateQuery({
          [memberType]: ((query[memberType] || []) as unknown[]).filter((_, index) => index !== member.index),
        } as Query);
      },
      update: (member, updateWith) => {
        const { query } = this.state;
        const members = ((query[memberType] || []) as unknown[]).concat([]);
        members.splice(member.index, 1, toQuery(updateWith));
        return this.updateQuery({
          [memberType]: members,
        } as Query);
      },
    });

    const {
      meta,
      metaError,
      richMetaError,
      query,
      queryError,
      chartType,
      pivotConfig,
      validatedQuery,
      missingMembers,
      isFetchingMeta,
      dryRunResponse,
      metaErrorStack
    } = this.state;

    const flatFilters = uniqBy((filter) => `${prop('member', filter)}${prop('operator', filter)}`,
      flattenFilters((meta && query.filters) || []).map((filter) => ({
        ...filter,
        member: filter.member || filter.dimension,
      })));

    const filters: ResolvedFilter[] = flatFilters.map((m, i) => ({
      ...m,
      dimension: meta!.resolveMember(
        m.member || m.dimension!,
        ['dimensions', 'measures']
      ) as TCubeDimension | TCubeMeasure,
      operators: meta!.filterOperatorsForMember(m.member || m.dimension!, ['dimensions', 'measures']),
      index: i,
    }));

    const measures = QueryBuilder.resolveMember('measures', this.state) as IndexedMeasure[];
    const dimensions = QueryBuilder.resolveMember('dimensions', this.state) as IndexedDimension[];
    const timeDimensions = QueryBuilder.resolveMember('timeDimensions', this.state) as ResolvedTimeDimension[];
    const segments: IndexedSegment[] = ((meta && query.segments) || []).map((m, i) => ({
      index: i,
      ...(meta!.resolveMember(m, 'segments') as TCubeSegment),
    }));

    let availableMeasures: TCubeMeasure[] = [];
    let availableDimensions: TCubeDimension[] = [];
    let availableSegments: TCubeSegment[] = [];
    let availableFilterMembers: Array<AvailableCube<TCubeMeasure> | AvailableCube<TCubeDimension>> = [];

    const availableMembers: AvailableMembers = meta?.membersGroupedByCube() || {
      measures: [],
      dimensions: [],
      segments: [],
      timeDimensions: [],
    };

    if (meta) {
      availableMeasures = meta.membersForQuery(query, 'measures') as TCubeMeasure[];
      availableDimensions = meta.membersForQuery(query, 'dimensions') as TCubeDimension[];
      availableSegments = meta.membersForQuery(query, 'segments') as TCubeSegment[];

      const indexedMeasures = indexBy(prop('cubeName'), availableMembers.measures);
      const indexedDimensions = indexBy(prop('cubeName'), availableMembers.dimensions);
      const cubeNames = uniq([...Object.keys(indexedMeasures), ...Object.keys(indexedDimensions)]).sort();

      // Measures and dimensions of a cube are merged into one member list, which
      // the declared type describes as either one or the other
      availableFilterMembers = cubeNames.map((name): AvailableCube<TCubeMeasure | TCubeDimension> => {
        const cube = indexedMeasures[name] || indexedDimensions[name];

        return {
          ...cube,
          members: [
            ...indexedMeasures[name]?.members,
            ...indexedDimensions[name]?.members
          ].sort((a, b) => (a.shortTitle > b.shortTitle ? 1 : -1)),
        };
      }) as Array<AvailableCube<TCubeMeasure> | AvailableCube<TCubeDimension>>;
    }

    const activeOrder = Array.isArray(query.order) ? Object.fromEntries(query.order) : query.order;
    const members = [
      ...measures,
      ...dimensions,
      ...timeDimensions.map(({ dimension }) => dimension)
    ];

    let orderMembers = uniqBy(prop('id'), [
      // uniqBy prefers first, so these will only be added if not already in the query
      ...members.map(({ name, title }) => ({ id: name, title, order: activeOrder?.[name] || 'none' })),
    ]);

    if (this.orderMembersOrderKeys.length !== orderMembers.length) {
      this.orderMembersOrderKeys = orderMembers.map(({ id }) => id);
    }

    if (this.orderMembersOrderKeys.length) {
      // Preserve order until the members change or manually re-ordered
      // This is needed so that when an order member becomes active, it doesn't jump to the top of the list
      orderMembers = (this.orderMembersOrderKeys || [])
        .map((id) => orderMembers.find((member) => member.id === id))
        .filter(Boolean) as TOrderMember[];
    }

    return {
      meta,
      metaError,
      richMetaError,
      metaErrorStack,
      query,
      error: queryError, // Match same name as QueryRenderer prop
      validatedQuery: validatedQuery as Query,
      isQueryPresent: this.isQueryPresent(),
      chartType,
      measures,
      dimensions,
      // The declared granularity options require a `name`, which the
      // "w/o grouping" option leaves out
      timeDimensions: timeDimensions as (TimeDimensionWithExtraFields & { index: number })[],
      segments,
      filters,
      orderMembers,
      availableMeasures,
      availableDimensions,
      availableTimeDimensions: availableDimensions.filter((m) => m.type === 'time'),
      availableSegments,
      availableMembers,
      availableFilterMembers,
      updateQuery: (queryUpdate) => this.updateQuery(queryUpdate),
      updateMeasures: updateMethods<TCubeMeasure>('measures'),
      updateDimensions: updateMethods<TCubeDimension>('dimensions'),
      updateSegments: updateMethods<TCubeSegment>('segments'),
      updateTimeDimensions: updateMethods<TimeDimensionUpdateInput>('timeDimensions', toTimeDimension) as
        MemberUpdater<TimeDimensionRangedUpdateFields | TimeDimensionComparisonUpdateFields>,
      updateFilters: updateMethods<FilterUpdateInput>('filters', toFilter) as MemberUpdater<FilterUpdateFields>,
      updateChartType: (newChartType: ChartType) => this.updateVizState({ chartType: newChartType }),
      updateOrder: {
        set: (memberId: string, newOrder: QueryOrder | 'none' = 'asc') => {
          this.updateQuery({
            order: orderMembers
              .map((orderMember) => ({
                ...orderMember,
                order: orderMember.id === memberId ? newOrder : orderMember.order,
              }))
              .reduce<[string, QueryOrder][]>(
                (acc, { id, order }) => (order !== 'none' ? [...acc, [id, order]] : acc),
                []
              ),
          });
        },
        update: (order) => {
          this.updateQuery({
            order,
          });
        },
        reorder: (sourceIndex: number, destinationIndex: number) => {
          if (sourceIndex == null || destinationIndex == null) {
            return;
          }

          const nextArray = moveItemInArray(orderMembers, sourceIndex, destinationIndex);
          this.orderMembersOrderKeys = nextArray.map(({ id }) => id);

          this.updateQuery({
            order: nextArray.reduce<[string, QueryOrder][]>(
              (acc, { id, order }) => (order !== 'none' ? [...acc, [id, order]] : acc),
              []
            ),
          });
        },
      },
      pivotConfig,
      updatePivotConfig: {
        moveItem: ({ sourceIndex, destinationIndex, sourceAxis, destinationAxis }: PivotConfigUpdaterArgs) => {
          this.updateVizState({
            pivotConfig: movePivotItem(
              pivotConfig as PivotConfig, sourceIndex, destinationIndex, sourceAxis, destinationAxis
            ),
          });
        },
        update: (config) => {
          const { limit } = config;

          this.updateVizState({
            pivotConfig: {
              ...pivotConfig,
              ...config,
            },
            ...(limit ? { query: { ...query, limit } } : null),
          });
        },
      },
      missingMembers,
      refresh: this.fetchMeta,
      isFetchingMeta,
      dryRunResponse: dryRunResponse as DryRunResponse | undefined,
      ...queryRendererProps,
    };
  }

  updateQuery(queryUpdate: Query) {
    const { query } = this.state;

    this.updateVizState({
      query: removeEmptyQueryFields({
        ...query,
        ...queryUpdate,
      }),
    });
  }

  async updateVizState(state: QueryBuilderStateUpdate) {
    const { setQuery, setVizState } = this.props;
    const { query: stateQuery, pivotConfig: statePivotConfig, chartType, meta } = this.state;

    const finalState = this.applyStateChangeHeuristics(state);
    if (!finalState.query) {
      finalState.query = { ...stateQuery };
    }

    let vizStateSent: QueryBuilderState | null = null;
    const handleVizStateChange = (currentState: QueryBuilderStateUpdate) => {
      const { onVizStateChanged } = this.props;
      if (onVizStateChanged) {
        const newVizState = pick(['chartType', 'pivotConfig', 'query'], currentState) as QueryBuilderState;
        // Don't run callbacks more than once unless the viz state has changed since last time
        if (!vizStateSent || !equals(vizStateSent, newVizState)) {
          onVizStateChanged(newVizState);
          // use clone to make sure we don't save object references
          vizStateSent = clone(newVizState);
        }
      }
    };

    // deprecated, setters replaced by onVizStateChanged
    const runSetters = (currentState: QueryBuilderStateUpdate) => {
      if (setVizState) {
        setVizState(pick(['chartType', 'pivotConfig', 'query'], currentState) as QueryBuilderState);
      }
      if (currentState.query && setQuery) {
        setQuery(currentState.query);
      }
    };

    if (finalState.shouldApplyHeuristicOrder) {
      finalState.query.order = defaultOrder(finalState.query);
    }

    finalState.pivotConfig = ResultSet.getNormalizedPivotConfig(
      finalState.query as PivotQuery,
      (finalState.pivotConfig !== undefined ? finalState.pivotConfig : statePivotConfig) as PivotConfig | undefined
    );

    finalState.missingMembers = this.getMissingMembers(finalState.query, meta);
    finalState.chartType = finalState.chartType || state.chartType || chartType;

    // deprecated
    runSetters({
      ...state,
      query: finalState.query,
    });

    // Update optimistically so that UI does not stutter
    this.setState({
      ...finalState,
      queryError: null,
    } as QueryBuilderInternalState);

    handleVizStateChange(finalState);

    const shouldFetchDryRun = !equals(
      pick(['measures', 'dimensions', 'timeDimensions'], stateQuery),
      pick(['measures', 'dimensions', 'timeDimensions'], finalState.query)
    );

    if (shouldFetchDryRun && isQueryPresent(finalState.query) && finalState.missingMembers.length === 0) {
      try {
        const response = await this.cubeApi().dryRun(finalState.query, {
          mutexObj: this.mutexObj,
        });

        if (finalState.shouldApplyHeuristicOrder) {
          finalState.query.order = (response.queryOrder || []).reduce((memo, current) => ({ ...memo, ...current }), {});
        }

        finalState.pivotConfig = ResultSet.getNormalizedPivotConfig(response.pivotQuery, finalState.pivotConfig);
        finalState.validatedQuery = this.validatedQuery(finalState);
        finalState.dryRunResponse = response;

        // deprecated
        if (isQueryPresent(stateQuery)) {
          runSetters({
            ...this.state,
            ...finalState,
          });
        }
      } catch (error) {
        const requestError = error as RequestError;

        this.setState({
          queryError: new Error(requestError.response?.plainError || requestError.message),
          richQueryError: new Error(requestError.message || requestError.toString())
        });
      }
    }

    this.setState(finalState as QueryBuilderInternalState, () => handleVizStateChange(this.state));
  }

  validatedQuery(state?: QueryBuilderStateUpdate) {
    const { query } = state || this.state;

    return validateQuery(query);
  }

  defaultHeuristics(newState: QueryBuilderStateUpdate): QueryBuilderStateUpdate {
    const { query, sessionGranularity, meta } = this.state;

    return defaultHeuristics(newState as TDefaultHeuristicsState, query, {
      meta: meta as Meta,
      sessionGranularity: sessionGranularity || 'day',
    }) as QueryBuilderStateUpdate;
  }

  applyStateChangeHeuristics(newState: QueryBuilderStateUpdate): QueryBuilderStateUpdate {
    const { stateChangeHeuristics, disableHeuristics } = this.props;
    if (disableHeuristics) {
      return newState;
    }
    return (stateChangeHeuristics && stateChangeHeuristics(this.state, newState)) || this.defaultHeuristics(newState);
  }

  render() {
    const { query } = this.state;
    const { cubeApi, render, wrapWithQueryRenderer } = this.props;

    if (wrapWithQueryRenderer) {
      return (
        <QueryRenderer
          query={query}
          cubeApi={cubeApi}
          resetResultSetOnChange={false}
          render={(queryRendererProps) => {
            if (render) {
              return render(this.prepareRenderProps(queryRendererProps));
            }
            return null;
          }}
        />
      );
    } else {
      if (render) {
        return render(this.prepareRenderProps());
      }
      return null;
    }
  }
}
