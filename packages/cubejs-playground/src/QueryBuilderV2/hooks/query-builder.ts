import { useEffect, useMemo, useRef, useState } from 'react';
import {
  ChartType,
  CubeApi,
  DateRange,
  DryRunResponse,
  Filter,
  LogicalAndFilter,
  LogicalOrFilter,
  Meta,
  PivotConfig,
  ProgressResult,
  Query,
  QueryOrder,
  ResultSet,
  SqlQuery,
  TCubeDimension,
  TCubeMeasure,
  TCubeSegment,
  TimeDimensionGranularity,
  TQueryOrderObject,
} from '@cubejs-client/core';

import {
  extractMembersFromFilters,
  getUsedCubesAndMembers,
  getQueryHash,
  movePivotItem,
  prepareQuery,
  useIsFirstRender,
  validateQuery,
} from '../utils';
import { CubeStats, MissingMember, WithUndefinedValues, MemberViewType, Cube } from '../types';

import { useEvent } from './event';

interface UseQueryBuilderProps {
  displayPrivateItems?: boolean;
  defaultQuery?: Query;
  defaultChartType?: ChartType;
  defaultPivotConfig?: PivotConfig;
  schemaVersion?: number;
  cubeApi?: CubeApi;
  memberViewType?: MemberViewType;
  tracking?: {
    event: (name: string, props?: Record<string, any>) => void;
  };
  onQueryChange?: (query: {
    query: Query;
    chartType?: ChartType;
    pivotConfig?: PivotConfig;
  }) => void;
  /**
   * Validates and prepares the query once it's get updated
   */
  queryValidator?: (query: Query) => Query;
}

type CubeMembers = {
  dimensions: Record<string, TCubeDimension>;
  measures: Record<string, TCubeMeasure>;
  segments: Record<string, TCubeSegment>;
};

type MemberUpdater = {
  add(name: string): boolean;
  remove(name: string): boolean;
  toggle(name: string): void;
  clear(): void;
  list: string[];
};

type MemberUpdaterMap = {
  dimensions: MemberUpdater;
  measures: MemberUpdater;
  segments: MemberUpdater;
};

const SIMPLE_MEMBERS: (keyof MemberUpdaterMap)[] = ['dimensions', 'measures', 'segments'];

const removeFiltersByMember = (filters: Query['filters'], memberName: string): Filter[] => {
  if (!Array.isArray(filters) || !filters.length) {
    return [];
  }

  return filters
    .map((filter) => {
      // If it's a logical filter, process its subfilters
      if ('and' in filter || 'or' in filter) {
        const key = 'and' in filter ? 'and' : 'or';
        const sanitizedSubFilters = removeFiltersByMember(
          'and' in filter ? filter.and : filter.or,
          memberName
        );

        // If all subfilters are removed, return null
        if (sanitizedSubFilters.length === 0) {
          return null;
        }

        // Return the logical filter with sanitized subfilters
        return { [key]: sanitizedSubFilters } as LogicalAndFilter | LogicalOrFilter;
      }

      // If it's a unary or binary filter, check its member
      if ('member' in filter && filter.member === memberName) {
        return null; // Remove the filter
      }

      return filter; // Keep the filter
    })
    .filter(Boolean) as Filter[]; // Remove null values
};

export function useQueryBuilder(props: UseQueryBuilderProps) {
  const mutexRef = useRef({});
  const firstRun = useIsFirstRender();

  let {
    cubeApi,
    schemaVersion,
    defaultChartType,
    defaultQuery,
    defaultPivotConfig,
    tracking,
    queryValidator,
    displayPrivateItems = true,
    memberViewType = 'name',
    onQueryChange,
  } = props;

  function queryValidation(query: Query) {
    let validatedQuery = validateQuery(query);

    prepareQuery(validatedQuery);

    if (queryValidator) {
      validatedQuery = queryValidator?.(query) ?? query;
    }

    return validatedQuery;
  }

  // Validate default query
  if (firstRun) {
    try {
      defaultQuery = queryValidation(defaultQuery || {});
    } catch (e: any) {
      console.error('Invalid default query', e);
      defaultQuery = {};
    }
  }

  // UI state
  const [selectedCubeName, selectCubeName] = useState<string | null>(null);

  const [query, setQueryInstance] = useState<Query>(defaultQuery || {});
  const [executedQuery, setExecutedQuery] = useState<Query | null>(null);

  // Invalidation markers
  const [isDataModelChanged, setIsDataModelChanged] = useState(false);
  const [isApiTokenChanged, setIsApiTokenChanged] = useState(false);

  // Calculate hash to invalidate query
  const queryHash = getQueryHash(query);

  const loadingRef = useRef(0);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [resultSet, setResultSet] = useState<ResultSet | null>(null);
  const [sqlQuery, setSqlQuery] = useState<SqlQuery | null>(null);
  const [progress, setProgress] = useState<ProgressResult | null>(null);

  const metaLoadingRef = useRef(0);
  const [isMetaLoading, setIsMetaLoading] = useState(false);
  const [meta, setMeta] = useState<Meta | null>(null);
  const [metaError, setMetaError] = useState(null);
  const [richMetaError, setRichMetaError] = useState(null);

  const verificationRef = useRef(0);
  const [isVerifying, setIsVerifying] = useState(false);
  const [verificationError, setVerificationError] = useState<string | null>(null);
  const [dryRunResponse, setDryRunResponse] = useState<DryRunResponse | null>(null);

  const [chartType, setChartType] = useState<ChartType>(defaultChartType || ('line' as ChartType));

  const [cubes, setCubes] = useState<Cube[]>([]);
  const [members, setMembers] = useState<CubeMembers>({
    dimensions: {},
    measures: {},
    segments: {},
  });
  const selectedCube = cubes.find((cube) => cube.name === selectedCubeName) ?? null;

  const [dateRangesStore, setDateRangesStore] = useState<string[]>(
    (query?.timeDimensions || [])
      .filter((timeDimension) => {
        return !!timeDimension.dateRange;
      })
      .map((timeDimension) => timeDimension.dimension)
  );

  const [pivotConfig, setPivotConfig] = useState<PivotConfig>(() => {
    return ResultSet.getNormalizedPivotConfig(
      { ...query, queryType: 'regularQuery' },
      defaultPivotConfig
    );
  });

  const progressCallback = (progressResult: ProgressResult) => {
    setProgress(progressResult);
  };

  const { usedCubes, usedMembers, usedMembersInFilters, usedMembersInGrouping, usedGranularities } =
    useMemo(() => getUsedCubesAndMembers(query, dateRangesStore), [query, dateRangesStore.join()]);

  const [missingCubes, missingMembers] = useMemo<[string[], MissingMember[]]>(() => {
    return [
      usedCubes.filter((cube) => !cubes.some((c) => c.name === cube)),
      [
        ...(query.dimensions
          ?.filter((dimension) => !members.dimensions[dimension])
          .map((name) => ({ name, category: 'dimensions' }) as MissingMember) || []),
        ...(query.measures
          ?.filter((measure) => !members.measures[measure])
          .map((name) => ({ name, category: 'measures' }) as MissingMember) || []),
        ...(query.segments
          ?.filter((segment) => !members.segments[segment])
          .map((name) => ({ name, category: 'segments' }) as MissingMember) || []),
        ...(query.timeDimensions
          ?.filter((timeDimension) => !members.dimensions[timeDimension.dimension])
          .map(
            ({ dimension, granularity }) =>
              ({
                name: dimension,
                category: 'timeDimensions',
                granularity,
              }) as MissingMember
          ) || []),
        ...(usedMembersInFilters
          .filter((dimension) => !members.dimensions[dimension] && !members.measures[dimension])
          .map((name) => ({ name, category: 'dimensions' }) as MissingMember) || []),
      ],
    ];
  }, [usedCubes, usedMembers, meta, members]);

  // place joined cubes first
  cubes.sort((c1, c2) => {
    const c1joined = isCubeUsed(c1.name);
    const c2joined = isCubeUsed(c2.name);

    return c1joined > c2joined ? -1 : c1joined < c2joined ? 1 : 0;
  });

  async function runQuery() {
    const currentRequest = ++loadingRef.current;

    if (!cubeApi) {
      return Promise.reject();
    }

    const queryCopy = JSON.parse(JSON.stringify(query)) as Query;

    setIsLoading(true);
    setProgress(null);
    setError(null);

    return Promise.all([
      cubeApi.load(query, {
        mutexObj: mutexRef.current,
        mutexKey: 'query',
        progressCallback,
      }),
      cubeApi.sql(query),
    ])
      .then(([resultSet, sqlQuery]) => {
        if (currentRequest !== loadingRef.current) {
          return;
        }

        setIsApiTokenChanged(false);
        setIsDataModelChanged(false);
        setIsLoading(false);
        setExecutedQuery(queryCopy);
        setResultSet(resultSet);
        setSqlQuery(sqlQuery);
        setProgress(null);

        tracking?.event('load_request_success:frontend', {
          isNewPlayground: true,
        });
      })
      .catch((error) => {
        if (currentRequest !== loadingRef.current) {
          return;
        }

        setIsLoading(false);
        setProgress(null);
        setError(error);
      });
  }

  function stopQuery() {
    loadingRef.current++;
    setIsLoading(false);
  }

  function clearQuery() {
    loadingRef.current++;
    setIsLoading(false);
    setQuery({});
    setExecutedQuery(null);
    setResultSet(null);
    setDateRangesStore([]);
    setError(null);
    selectCubeName(null);
    setError(null);
    setVerificationError(null);
  }

  function loadMeta() {
    const currentRequest = ++metaLoadingRef.current;

    if (!cubeApi) {
      return;
    }

    setIsMetaLoading(true);

    return cubeApi
      .meta()
      .then((newMeta) => {
        if (currentRequest !== metaLoadingRef.current) {
          return;
        }

        const visibilityFilter = (item: { public?: boolean }) => {
          return !displayPrivateItems ? item.public : true;
        };

        setIsMetaLoading(false);

        const memberData: CubeMembers = {
          dimensions: {},
          measures: {},
          segments: {},
        };

        newMeta.meta.cubes.filter(visibilityFilter).forEach((cube) => {
          cube.dimensions.filter(visibilityFilter).forEach((dimension) => {
            memberData.dimensions[dimension.name] = dimension;
          });

          cube.measures.filter(visibilityFilter).forEach((measure) => {
            memberData.measures[measure.name] = measure;
          });

          cube.segments.filter(visibilityFilter).forEach((segment) => {
            memberData.segments[segment.name] = segment;
          });
        });

        setMembers(memberData);

        setCubes(
          newMeta.meta.cubes
            .filter(visibilityFilter)
            .map((cube) => {
              return {
                ...cube,
                measures: cube.measures.filter(visibilityFilter),
                dimensions: cube.dimensions.filter(visibilityFilter),
                segments: cube.segments.filter(visibilityFilter),
              };
            })
            .sort((a, b) => a.name.localeCompare(b.name)) as Cube[]
        );

        setMeta(newMeta);
      })
      .catch((error) => {
        if (currentRequest !== metaLoadingRef.current) {
          return;
        }

        setIsMetaLoading(false);
        setMetaError(error.response?.plainError?.trim() || String(error));
        setRichMetaError(error);
        // metaErrorStack = error.response?.stack?.replace(error.message || '', '') || '';
      });
  }

  function dryRun() {
    const currentRequest = ++verificationRef.current;

    if (!meta || !cubeApi || !usedCubes.length) {
      return;
    }

    setIsVerifying(true);

    cubeApi
      .dryRun(query)
      .then((dryRunResponse) => {
        if (currentRequest !== verificationRef.current) {
          return;
        }

        setVerificationError(null);
        setIsVerifying(false);
        setDryRunResponse(dryRunResponse);
        setPivotConfig(ResultSet.getNormalizedPivotConfig(dryRunResponse.pivotQuery, pivotConfig));
      })
      .catch((error) => {
        if (currentRequest !== verificationRef.current) {
          return;
        }

        setIsVerifying(false);
        setVerificationError(error.response?.plainError || error.message || String(error));
      });
  }

  function setQuery(query: Query) {
    setQueryInstance((originalQuery) => {
      try {
        const originalHash = getQueryHash(originalQuery);

        let validatedQuery = queryValidation(query);

        return originalHash !== getQueryHash(validatedQuery) ? validatedQuery : originalQuery;
      } catch (e: any) {
        console.error('An invalid query has been set', query);

        return originalQuery;
      }
    });
  }

  function updateQuery(queryPart: Query | ((query: Query) => WithUndefinedValues<Query> | void)) {
    setQueryInstance((originalQuery) => {
      const copiedQuery = JSON.parse(JSON.stringify(originalQuery)) as Query;

      try {
        const originalHash = getQueryHash(copiedQuery);

        let query: Query;

        if (typeof queryPart === 'function') {
          const newQuery = queryPart(copiedQuery);

          // if function returns nothing we don't need to update query
          if (!newQuery) {
            return originalQuery;
          }

          query = queryValidation({ ...copiedQuery, ...newQuery });
        } else {
          query = queryValidation({
            ...copiedQuery,
            ...queryPart,
          });
        }

        return originalHash !== getQueryHash(query) ? query : originalQuery;
      } catch (e: any) {
        console.error('An invalid query has been set', query);

        return originalQuery;
      }
    });
  }

  function getCubeByName(name: string) {
    return cubes.find((cube) => cube.name === name);
  }

  function isCubeUsed(name: string) {
    return usedCubes.includes(name);
  }

  function isMemberUsed(name: string) {
    return usedMembers.includes(name);
  }

  function getMemberFormat(name: string) {
    const member = members.dimensions[name] || members.measures[name];

    if (!member || !('format' in member)) {
      return null;
    }

    return member.format;
  }

  // Find all dimensions inside hierarchies that can be added alongside the given dimension.
  const getConnectedDimensionNames = useEvent((name: string) => {
    const names: string[] = [name];
    const cubeName = name.split('.')[0];
    const cube = getCubeByName(cubeName);

    if (cube) {
      // Find all hierarchies that include the given dimension
      const hierarchiesToFill = (cube.hierarchies ?? []).filter((hierarchy) => {
        return hierarchy.levels.includes(name);
      });

      // If there is only one hierarchy that can be filled, we can add all levels that are above the given dimension
      if (hierarchiesToFill.length === 1) {
        const levels = hierarchiesToFill[0].levels;

        // If no dimension in the hierarchy selected, then we can proceed
        if (!levels.some((dimensionName) => query.dimensions?.includes(dimensionName))) {
          levels
            .slice(0, levels.indexOf(name))
            .reverse()
            .forEach((otherName) => {
              if (names.includes(otherName)) {
                return names;
              }

              names.push(otherName);
            });
        }
      }
    }

    return names.reverse();
  });

  // Updaters with simple common logic for dimensions, measures and segments
  const simpleUpdaters = SIMPLE_MEMBERS.reduce((acc, type) => {
    acc[type] = {
      add(name: string) {
        const member = members[type][name];

        if (!member) {
          console.log(`Unable to add ${type.slice(0, -1)}. Member is not found`, name);

          return false;
        }

        let names: string[] = [name];

        if (type === 'dimensions') {
          names = getConnectedDimensionNames(name);
        }

        updateQuery((query) => {
          const list = query[type] || [];

          names.forEach((name) => {
            if (!list?.includes(name)) {
              list.push(name);
            }
          });

          return { [type]: list };
        });

        return true;
      },
      remove(name: string) {
        updateQuery((query) => {
          const list = query[type] || [];

          const index = list?.indexOf(name);

          if (index !== -1) {
            list.splice(index, 1);
          }

          return { [type]: list };
        });

        return true;
      },
      toggle(name: string) {
        updateQuery((query) => {
          const list = query[type] || [];

          const index = list?.indexOf(name);

          if (index === -1) {
            const member = members[type][name];

            if (!member) {
              console.log(`Unable to toggle ${type.slice(0, -1)}. Member is not found`, name);

              return;
            }

            if (type === 'dimensions') {
              list.push(...getConnectedDimensionNames(name));
            } else {
              list.push(name);
            }
          } else {
            list.splice(index, 1);
          }

          return { [type]: list };
        });

        return true;
      },
      clear() {
        updateQuery(() => {
          return {
            [type]: [],
          };
        });
      },
      get list() {
        return query[type] || [];
      },
    };

    return acc;
  }, {} as Partial<MemberUpdaterMap>) as MemberUpdaterMap;

  const grouping = {
    add(name: string, granularity: TimeDimensionGranularity) {
      const member = members.dimensions[name];

      if (!member) {
        console.log('Unable to set grouping. Member is not found', name);

        return false;
      }

      if (member.type !== 'time') {
        console.log('Unable to set grouping. Incorrect member type', {
          name,
          type: member.type,
        });

        return false;
      }

      updateQuery((query) => {
        const { timeDimensions = [] } = query;

        const component = timeDimensions.find((d) => d.dimension === name);

        if (!component) {
          timeDimensions.push({ dimension: name, granularity });
        } else {
          component.granularity = granularity;
        }

        return { timeDimensions };
      });

      return true;
    },
    remove(name: string) {
      updateQuery((query) => {
        let { timeDimensions = [] } = query;

        const component = timeDimensions.find((d) => d.dimension === name);

        if (!component) {
          return;
        }

        delete component.granularity;

        // If component has no date range either we can remove it
        if (!component.dateRange) {
          timeDimensions = timeDimensions.filter((d) => d.dimension !== name);
        }

        return { timeDimensions };
      });

      return true;
    },
    toggle(name: string, granularity: TimeDimensionGranularity) {
      updateQuery((query) => {
        let { timeDimensions = [] } = query;

        const component = timeDimensions.find((d) => d.dimension === name);

        if (!component) {
          const member = members.dimensions[name];

          if (!member) {
            console.log('Unable to toggle grouping. Member is not found', name);

            return;
          }

          if (member.type !== 'time') {
            console.log('Unable to toggle grouping. Incorrect member type', {
              name,
              type: member.type,
            });

            return;
          }

          timeDimensions.push({ dimension: name, granularity });
        } else {
          if (component.granularity === granularity) {
            delete component.granularity;

            // If component has no date range either we can remove it
            if (!component.dateRange) {
              timeDimensions = timeDimensions.filter((d) => d.dimension !== name);
            }
          } else {
            component.granularity = granularity;
          }
        }

        return { timeDimensions };
      });

      return true;
    },
    get(name: string) {
      const { timeDimensions = [] } = query;

      return timeDimensions.find((timeDimension) => timeDimension.dimension === name)?.granularity;
    },
    getAll() {
      const { timeDimensions = [] } = query;

      return timeDimensions.filter((timeDimension) => timeDimension.granularity);
    },
    reorder(names: string[]) {
      updateQuery((query) => {
        const { timeDimensions = [] } = query;

        const reordered = timeDimensions.sort((a, b) => {
          const aIndex = names.indexOf(a.dimension);
          const bIndex = names.indexOf(b.dimension);

          return aIndex - bIndex;
        });

        return { timeDimensions: reordered };
      });
    },
    clear() {
      updateQuery((query) => {
        return {
          timeDimensions: query.timeDimensions?.filter((d) => !d.granularity),
        };
      });
    },
  };

  const dateRanges = {
    set(name: string, dateRange?: DateRange) {
      const member = members.dimensions[name];

      if (!member) {
        console.log('Unable to add date range. Member is not found', name);

        return false;
      }

      if (member.type !== 'time') {
        console.log('Unable to add date range. Incorrect member type', {
          name,
          type: member.type,
        });

        return false;
      }

      setDateRangesStore((dateRanges) => {
        if (!dateRanges.includes(name)) {
          return [...dateRanges, name];
        }

        return dateRanges;
      });

      if (dateRange) {
        updateQuery((query) => {
          const { timeDimensions = [] } = query;

          const component = timeDimensions.find((d) => d.dimension === name);

          if (!component) {
            timeDimensions.push({ dimension: name, dateRange });
          } else {
            component.dateRange = dateRange;
          }

          return { timeDimensions };
        });
      }

      return true;
    },
    remove(name: string) {
      updateQuery((query) => {
        let { timeDimensions = [] } = query;

        const component = timeDimensions.find((d) => d.dimension === name);

        if (!component) {
          return;
        }

        delete component.dateRange;

        // If component has no granularity either we can remove it
        if (!component.granularity) {
          timeDimensions = timeDimensions.filter((d) => d.dimension !== name);
        }

        return { timeDimensions };
      });

      setDateRangesStore((dateRanges) => {
        return dateRanges.filter((d) => d !== name);
      });

      return true;
    },
    get list() {
      return dateRangesStore;
    },
    clear() {
      updateQuery((query) => {
        return {
          timeDimensions: query.timeDimensions?.filter((d) => !d.dateRange),
        };
      });

      setDateRangesStore([]);
    },
  };

  const order = {
    set(name: string, order: QueryOrder) {
      const member = members.dimensions[name] || members.measures[name];

      if (!member) {
        console.log('Unable to set order. Member is not found', name);

        return false;
      }

      updateQuery((query) => {
        const orderMap = (query.order || {}) as TQueryOrderObject;

        if (orderMap[name] === order) {
          return;
        }

        return {
          order: {
            ...orderMap,
            [name]: order,
          },
        };
      });

      return true;
    },
    remove(name: string) {
      updateQuery((query) => {
        const orderMap = (query.order || {}) as TQueryOrderObject;

        if (!orderMap[name]) {
          return;
        }

        delete orderMap[name];

        return {
          order: orderMap,
        };
      });

      return true;
    },
    get(name: string) {
      const orderMap = (query.order || {}) as TQueryOrderObject;

      return orderMap[name];
    },
    get map() {
      return (query.order || {}) as TQueryOrderObject;
    },
    setOrder(names: string[]) {
      updateQuery((query) => {
        names = [...names];

        const orderMap = (query.order || {}) as TQueryOrderObject;

        Object.keys(orderMap).forEach((name) => {
          // suppress TS warning
          if (typeof name !== 'string') {
            return;
          }

          if (!names.includes(name)) {
            names.push(orderMap[name]);
          }
        });

        const order = names.reduce((acc, name) => {
          if (name in orderMap) {
            acc[name] = orderMap[name];
          }

          return acc;
        }, {} as TQueryOrderObject);

        return {
          order,
        };
      });
    },
    getOrder() {
      const orderMap = (query.order || {}) as TQueryOrderObject;

      return Object.keys(orderMap) as string[];
    },
    clear() {
      updateQuery(() => {
        return {
          order: undefined,
        };
      });
    },
  };

  const filters = {
    add(filter: Filter) {
      if ('member' in filter && filter.member) {
        const name = filter.member;

        const member = members.dimensions[name] || members.measures[name];

        if (!member) {
          console.log('Unable to set order. Member is not found', name);

          return false;
        }
      }

      const hash = JSON.stringify(filter);

      updateQuery((query) => {
        const filters = query?.filters || [];

        filters.push(filter);

        return {
          filters,
        };
      });

      return true;
    },
    remove(index: number) {
      updateQuery((query) => {
        const filters = query?.filters || [];

        filters.splice(index, 1);

        return {
          filters,
        };
      });

      return true;
    },
    removeByMember(name: string) {
      updateQuery((query) => {
        const filters = query.filters || [];

        return { filters: removeFiltersByMember(filters, name) };
      });

      return true;
    },
    update(index: number, filter: Filter) {
      updateQuery((query) => {
        const filters = query?.filters || [];

        filters[index] = filter;

        return {
          filters,
        };
      });

      return true;
    },
    get list() {
      return query?.filters || [];
    },
    clear() {
      updateQuery(() => {
        return {
          filters: [],
        };
      });
    },
  };

  // UI state management
  useEffect(() => {
    if (selectedCubeName && !getCubeByName(selectedCubeName)) {
      selectCubeName(null);
    }
  }, [meta]);

  // Each time the query is changed we need to make a dry run to load pivot config and validate query
  useEffect(() => {
    if (meta) {
      dryRun();
    }
  }, [queryHash, chartType, meta]);

  useEffect(() => {
    onQueryChange?.({ query, chartType, pivotConfig });
  }, [queryHash, chartType, pivotConfig]);

  // Update invalidation markers
  useEffect(() => {
    if (executedQuery) {
      setIsApiTokenChanged(true);
    }
  }, [cubeApi]);

  useEffect(() => {
    if (executedQuery) {
      setIsDataModelChanged(true);
    }
  }, [schemaVersion, cubeApi]);

  // After time dimensions updated...
  useEffect(() => {
    let updateDateRanges = false;

    const timeDimensions = query?.timeDimensions || [];

    // ...make sure that all the related dateRanges are added
    timeDimensions
      .filter((timeDimension) => {
        return !!timeDimension.dateRange;
      })
      .map((timeDimension) => timeDimension.dimension)
      .forEach((dimensionName) => {
        if (!dateRangesStore.includes(dimensionName)) {
          updateDateRanges = true;
          dateRangesStore.push(dimensionName);
        }
      });

    // ...make sure that all non-related dateRanges are removed
    // const filteredDateRanges = dateRangesStore.filter((dimensionName) =>
    //   timeDimensions.find((timeDimension) => timeDimension.dimension === dimensionName)
    // );

    if (updateDateRanges) {
      setDateRangesStore([...dateRangesStore]);
    }
  }, [JSON.stringify(query.timeDimensions), JSON.stringify(dateRangesStore)]);

  // Each time schema is changed we need to reload meta
  useEffect(() => {
    void loadMeta();
  }, [schemaVersion, cubeApi]);

  const isQueryEmpty =
    !query.measures?.length &&
    !query.dimensions?.length &&
    !query.timeDimensions?.length &&
    !query.filters?.length &&
    !query.segments?.length;

  // @ts-ignore
  const connectionId = usedCubes[0]
    ? // @ts-ignore
    (() => {
      const cubeName = usedCubes.find((cubeName) => getCubeByName(cubeName)?.connectedComponent);

      return cubeName ? getCubeByName(cubeName)?.connectedComponent : undefined;
    })()
    : undefined;

  // @ts-ignore
  const joinableCubes = !usedCubes.length
    ? [...cubes]
    : cubes.filter((cube) =>
      // @ts-ignore
      connectionId != null ? cube.connectedComponent === connectionId : cube.name === usedCubes[0]
    );
  const joinableCubeNames = joinableCubes.map((cube) => cube.name);
  const joinableMembers = useMemo(
    () =>
      !usedCubes.length
        ? {
          dimensions: members.dimensions,
          measures: members.measures,
          segments: members.segments,
        }
        : {
          dimensions: Object.fromEntries(
            Object.entries(members.dimensions).filter(([name]) =>
              joinableCubeNames.includes(name.split('.')[0])
            )
          ),
          measures: Object.fromEntries(
            Object.entries(members.measures).filter(([name]) =>
              joinableCubeNames.includes(name.split('.')[0])
            )
          ),
          segments: Object.fromEntries(
            Object.entries(members.segments).filter(([name]) =>
              joinableCubeNames.includes(name.split('.')[0])
            )
          ),
        },
    [joinableCubeNames.join(',')]
  );

  const updatePivotConfig = {
    moveItem: ({
      sourceIndex,
      destinationIndex,
      sourceAxis,
      destinationAxis,
    }: {
      sourceIndex: number;
      destinationIndex: number;
      sourceAxis: 'x' | 'y';
      destinationAxis: 'x' | 'y';
    }) => {
      setPivotConfig(
        ResultSet.getNormalizedPivotConfig(
          { ...query, queryType: 'regularQuery' },
          {
            ...pivotConfig,
            ...movePivotItem(
              pivotConfig,
              sourceIndex,
              destinationIndex,
              sourceAxis,
              destinationAxis
            ),
          }
        )
      );
    },
    update: (config: PivotConfig) => {
      setPivotConfig(
        ResultSet.getNormalizedPivotConfig(
          { ...query, queryType: 'regularQuery' },
          {
            ...pivotConfig,
            ...config,
          }
        )
      );
    },
  };

  const queryStats = useMemo(() => {
    const measures = query?.measures || [];
    const dimensions = query?.dimensions || [];
    const segments = query?.segments || [];
    const filters = extractMembersFromFilters(query?.filters || []);
    const dateRanges =
      query?.timeDimensions
        ?.filter((timeDimension) => timeDimension.dateRange)
        .map((timeDimension) => timeDimension.dimension) || [];
    const grouping =
      query?.timeDimensions
        ?.filter((timeDimension) => timeDimension.granularity)
        .map((timeDimension) => timeDimension.dimension) || [];
    const all = [...measures, ...dimensions, ...segments, ...filters, ...dateRanges, ...grouping];
    const allCubeNames: string[] = [];

    all.forEach((member) => {
      const cubeName = member.split('.')[0];

      if (!allCubeNames.includes(cubeName)) {
        allCubeNames.push(cubeName);
      }
    });

    return allCubeNames.reduce(
      (allStats, cubeName) => {
        const cube = getCubeByName(cubeName);
        const stats: CubeStats = {
          measures: [],
          dimensions: [],
          segments: [],
          filters: [],
          folders: {},
          hierarchies: {},
          dateRanges: [],
          grouping: [],
          timeDimensions: [],
          instance: cube,
          isUsed: false,
        };

        const cubePrefix = `${cubeName}.`;

        measures?.forEach((measure) => {
          if (measure.includes(cubePrefix)) {
            stats.measures.push(measure);
            stats.isUsed = true;
          }
        });

        dimensions?.forEach((dimension) => {
          if (dimension.includes(cubePrefix)) {
            stats.dimensions.push(dimension);
            stats.isUsed = true;
          }
        });

        segments?.forEach((segment) => {
          if (segment.includes(cubePrefix)) {
            stats.segments.push(segment);
            stats.isUsed = true;
          }
        });

        filters.forEach((member) => {
          if (member.includes(cubePrefix)) {
            stats.filters.push(member);
            stats.isUsed = true;
          }
        });

        dateRanges.forEach((member) => {
          if (member.includes(cubePrefix)) {
            stats.dateRanges.push(member);
            stats.isUsed = true;
          }
        });

        grouping.forEach((member) => {
          if (member.includes(cubePrefix)) {
            stats.grouping.push(member);
            stats.isUsed = true;
          }
        });

        stats.timeDimensions = [...stats.dateRanges];

        stats.grouping.forEach((member) => {
          if (!stats.timeDimensions.includes(member)) {
            stats.timeDimensions.push(member);
          }
        });

        cube?.hierarchies?.forEach((hierarchy) => {
          const hierarchyName = hierarchy.name;

          stats.hierarchies[hierarchyName] = [];

          const dimensions = stats.hierarchies[hierarchyName];

          hierarchy.levels.forEach((dimensionName) => {
            if (stats.dimensions.includes(dimensionName)) {
              dimensions.push(dimensionName);
            }
          });
        });

        cube?.folders?.forEach((folder) => {
          const folderName = folder.name;

          stats.folders[folderName] = {
            dimensions: [],
            measures: [],
            segments: [],
            grouping: [],
          };

          const folderStats = stats.folders[folderName];

          folder.members.forEach((memberName) => {
            if (stats.dimensions.includes(memberName)) {
              if (!folderStats.dimensions.includes(memberName)) {
                folderStats.dimensions.push(memberName);
              }
            } else if (stats.measures.includes(memberName)) {
              if (!folderStats.measures.includes(memberName)) {
                folderStats.measures.push(memberName);
              }
            } else if (stats.segments.includes(memberName)) {
              if (!folderStats.segments.includes(memberName)) {
                folderStats.segments.push(memberName);
              }
            } else if (stats.hierarchies[memberName]) {
              // add all selected dimensions from the hierarchy
              stats.hierarchies[memberName].forEach((levelMemberName) => {
                if (!folderStats.dimensions.includes(levelMemberName)) {
                  folderStats.dimensions.push(levelMemberName);
                }
              });
            }

            if (grouping.includes(memberName)) {
              folderStats.grouping.push(memberName);
            }
          });
        });

        allStats[cubeName] = stats;

        return allStats;
      },
      {} as Record<string, CubeStats>
    );
  }, [queryHash, meta, cubes.length]);

  const memberList = useMemo(() => {
    return [...Object.values(members.dimensions), ...Object.values(members.measures)];
  }, [members]);

  const hasPrivateMembers = useMemo(() => {
    return usedMembers.some((memberName) => {
      const member = memberList.find((m) => m.name === memberName);

      return !member?.public;
    });
  }, [usedCubes, usedMembers]);

  return {
    // options
    memberViewType,
    // query
    query: JSON.parse(JSON.stringify(query)) as Query, // always provide a copy of query to avoid indirect mutation
    queryHash,
    executedQuery,
    runQuery,
    stopQuery,
    clearQuery,
    setQuery,
    updateQuery,
    isVerifying,
    verificationError,
    isQueryTouched: !executedQuery || queryHash !== getQueryHash(executedQuery),
    error,
    isLoading,
    progress,
    // meta & stats
    meta,
    isMetaLoading,
    metaError,
    richMetaError,
    loadMeta,
    queryStats,
    // configuration
    pivotConfig,
    updatePivotConfig,
    // responses
    resultSet,
    dryRunResponse,
    sqlQuery,
    // utils
    getCubeByName,
    getMemberFormat,
    // data
    cubes,
    members,
    joinableCubes,
    joinableMembers,
    usedCubes,
    usedMembers,
    usedGranularities,
    usedMembersInGrouping,
    usedMembersInFilters,
    missingCubes,
    missingMembers,
    // updaters
    ...simpleUpdaters,
    grouping,
    dateRanges,
    order,
    filters,
    // state
    chartType,
    hasPrivateMembers,
    setChartType,
    isCubeJoined: isCubeUsed,
    isMemberJoined: isMemberUsed,
    isCubeUsed,
    isQueryEmpty,
    isApiTokenChanged,
    isDataModelChanged,
    isResultOutdated: !!(
      executedQuery &&
      (queryHash !== getQueryHash(executedQuery) || isApiTokenChanged || isDataModelChanged)
    ),
    // api
    cubeApi,
    mutexObj: mutexRef.current,
    // ui
    selectedCube,
    selectCube: useEvent((name: string | null) => selectCubeName(name)),
    // @ts-ignore
    totalRows: resultSet?.totalRows(),
  };
}
