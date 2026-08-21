import {
  GRANULARITIES,
  getQueryMembers,
  type Meta,
  type NotFoundMember,
  type Query,
  type QueryOrder,
  type TCubeDimension,
  type TCubeMeasure,
  type TCubeSegment,
  type TGranularityMap,
  type TOrderMember,
} from '@cubejs-client/core';

import { flattenFilterTree } from './filters';
import type {
  AvailableCube,
  AvailableMembers,
  ResolvedFilter,
  SelectedDimension,
  SelectedMeasure,
  SelectedSegment,
  SelectedTimeDimension,
} from '../types';

export interface DerivedMembers {
  measures: SelectedMeasure[];
  dimensions: SelectedDimension[];
  segments: SelectedSegment[];
  timeDimensions: SelectedTimeDimension[];
  filters: ResolvedFilter[];
  filterTree: Query['filters'];
  availableMeasures: TCubeMeasure[];
  availableDimensions: TCubeDimension[];
  availableTimeDimensions: TCubeDimension[];
  availableSegments: TCubeSegment[];
  availableMembers: AvailableMembers;
  availableFilterMembers: Array<
    AvailableCube<TCubeMeasure | TCubeDimension>
  >;
  missingMembers: string[];
  orderMembers: TOrderMember[];
  orderMemberKeys: string[];
}

const EMPTY_AVAILABLE_MEMBERS: AvailableMembers = {
  measures: [],
  dimensions: [],
  segments: [],
  timeDimensions: [],
};

export function emptyDerivedMembers(): DerivedMembers {
  return {
    measures: [],
    dimensions: [],
    segments: [],
    timeDimensions: [],
    filters: [],
    filterTree: [],
    availableMeasures: [],
    availableDimensions: [],
    availableTimeDimensions: [],
    availableSegments: [],
    availableMembers: EMPTY_AVAILABLE_MEMBERS,
    availableFilterMembers: [],
    missingMembers: [],
    orderMembers: [],
    orderMemberKeys: [],
  };
}

function resolveNamedMember<T>(
  name: string,
  index: number,
  resolved: T | NotFoundMember
): (T & { index: number }) | (NotFoundMember & { name: string; index: number }) {
  if ('error' in (resolved as NotFoundMember)) {
    return {
      ...(resolved as NotFoundMember),
      name,
      index,
    };
  }

  return {
    ...(resolved as T),
    index,
  };
}

function mergeGranularities(
  custom: ReadonlyArray<{ name: string; title: string }> | undefined
): TGranularityMap[] {
  const values = new Map<string | undefined, TGranularityMap>();
  GRANULARITIES.forEach((value) => values.set(value.name, { ...value }));
  custom?.forEach((value) => values.set(value.name, { ...value }));
  return [...values.values()];
}

function orderEntries(query: Query): Array<[string, QueryOrder]> {
  if (Array.isArray(query.order)) {
    return query.order;
  }

  return Object.entries(query.order ?? {});
}

function buildOrderMembers(
  query: Query,
  measures: SelectedMeasure[],
  dimensions: SelectedDimension[],
  timeDimensions: SelectedTimeDimension[],
  previousKeys: string[]
): { orderMembers: TOrderMember[]; orderMemberKeys: string[] } {
  const candidates = new Map<string, { id: string; title: string }>();

  [...measures, ...dimensions].forEach((member) => {
    candidates.set(member.name, { id: member.name, title: member.title });
  });
  timeDimensions.forEach((value) => {
    candidates.set(value.dimension.name, {
      id: value.dimension.name,
      title: value.dimension.title,
    });
  });

  const activeOrder = new Map(orderEntries(query));
  const survivingKeys = previousKeys.filter((key) => candidates.has(key));
  const orderMemberKeys = [
    ...survivingKeys,
    ...[...candidates.keys()].filter((key) => !survivingKeys.includes(key)),
  ];

  return {
    orderMemberKeys,
    orderMembers: orderMemberKeys.map((key) => ({
      ...candidates.get(key)!,
      order: activeOrder.get(key) ?? 'none',
    })),
  };
}

export function deriveMembers(
  meta: Meta,
  query: Query,
  previousOrderMemberKeys: string[] = []
): DerivedMembers {
  const measures = (query.measures ?? []).map((name, index) =>
    resolveNamedMember(
      name,
      index,
      meta.resolveMember(name, 'measures')
    )
  ) as SelectedMeasure[];
  const dimensions = (query.dimensions ?? []).map((name, index) =>
    resolveNamedMember(
      name,
      index,
      meta.resolveMember(name, 'dimensions')
    )
  ) as SelectedDimension[];
  const segments = (query.segments ?? []).map((name, index) =>
    resolveNamedMember(name, index, meta.resolveMember(name, 'segments'))
  ) as SelectedSegment[];
  const timeDimensions = (query.timeDimensions ?? []).map((value, index) => {
    const resolved = meta.resolveMember(value.dimension, 'dimensions');
    const custom =
      'error' in resolved || !('granularities' in resolved)
        ? undefined
        : resolved.granularities;

    return {
      ...value,
      index,
      dimension: {
        ...resolved,
        name: value.dimension,
        granularities: mergeGranularities(custom),
      },
    } as SelectedTimeDimension;
  });
  const filters = flattenFilterTree(query.filters ?? []).map(
    ({ filter, path }, index) => {
      const name = filter.member ?? filter.dimension!;
      return {
        filter,
        path,
        index,
        member: meta.resolveMember(name, ['dimensions', 'measures']),
        operators: meta.filterOperatorsForMember(name, [
          'dimensions',
          'measures',
        ]),
      } as ResolvedFilter;
    }
  );
  const availableMeasures = meta.membersForQuery(
    query,
    'measures'
  ) as TCubeMeasure[];
  const availableDimensions = meta.membersForQuery(
    query,
    'dimensions'
  ) as TCubeDimension[];
  const availableSegments = meta.membersForQuery(
    query,
    'segments'
  ) as TCubeSegment[];
  const availableMembers = meta.membersGroupedByCube() as AvailableMembers;
  const measuresByCube = new Map(
    availableMembers.measures.map((cube) => [cube.cubeName, cube])
  );
  const dimensionsByCube = new Map(
    availableMembers.dimensions.map((cube) => [cube.cubeName, cube])
  );
  const cubeNames = new Set([
    ...measuresByCube.keys(),
    ...dimensionsByCube.keys(),
  ]);
  const availableFilterMembers = [...cubeNames]
    .sort()
    .map((cubeName) => {
      const measureCube = measuresByCube.get(cubeName);
      const dimensionCube = dimensionsByCube.get(cubeName);
      const cube = measureCube ?? dimensionCube!;
      return {
        ...cube,
        members: [
          ...(measureCube?.members ?? []),
          ...(dimensionCube?.members ?? []),
        ].sort((left, right) =>
          left.shortTitle < right.shortTitle
            ? -1
            : left.shortTitle > right.shortTitle
              ? 1
              : 0
        ),
      };
    });
  const missingMembers = getQueryMembers(query).filter((name) => {
    const member = meta.resolveMember(name, [
      'measures',
      'dimensions',
      'segments',
    ]);
    return 'error' in member;
  });
  const uniqueMissingMembers = [...new Set(missingMembers)];
  const order = buildOrderMembers(
    query,
    measures,
    dimensions,
    timeDimensions,
    previousOrderMemberKeys
  );

  return {
    measures,
    dimensions,
    segments,
    timeDimensions,
    filters,
    filterTree: query.filters ?? [],
    availableMeasures,
    availableDimensions,
    availableTimeDimensions: availableDimensions.filter(
      (member) => member.type === 'time'
    ),
    availableSegments,
    availableMembers,
    availableFilterMembers,
    missingMembers: uniqueMissingMembers,
    ...order,
  };
}

export function serializeOrderMembers(
  members: TOrderMember[]
): Array<[string, QueryOrder]> {
  return members.reduce<Array<[string, QueryOrder]>>((result, member) => {
    if (member.order !== 'none') {
      result.push([member.id, member.order]);
    }
    return result;
  }, []);
}
