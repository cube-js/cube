import {
  Query,
  TimeDimensionGranularity,
  TransformedQuery,
} from '@cubejs-client/core';
import { all, allPass, anyPass, contains, equals, sortBy } from 'ramda';

import { QueryMemberKey } from '../../types';

export type PreAggregationDefinition = {
  code: string;
  measures: string[];
  dimensions: string[];
  timeDimension?: string;
  granularity?: TimeDimensionGranularity;
};

export function getPreAggregationDefinition(
  transformedQuery,
  preAggregationName = 'main'
): PreAggregationDefinition {
  const members: Omit<PreAggregationDefinition, 'code'> = {
    measures: [],
    dimensions: [],
  };
  let lines: string[] = [];

  if (transformedQuery?.leafMeasures.length) {
    members.measures = [...transformedQuery.leafMeasures];
    lines.push(`measures: [${transformedQuery.leafMeasures.join(', ')}]`);
  }

  if (transformedQuery?.sortedDimensions.length) {
    members.dimensions = [...transformedQuery.sortedDimensions];
    lines.push(`dimensions: [${transformedQuery.sortedDimensions.join(', ')}]`);
  }

  if (transformedQuery?.sortedTimeDimensions.length) {
    members.timeDimension = transformedQuery.sortedTimeDimensions[0][0];
    members.granularity = transformedQuery.sortedTimeDimensions[0][1];

    lines.push(`timeDimension: ${transformedQuery.sortedTimeDimensions[0][0]}`);
    lines.push(
      `granularity: \`${transformedQuery.sortedTimeDimensions[0][1]}\``
    );
  }

  return {
    code: `${preAggregationName}: {\n${lines
      .map((l) => `  ${l}`)
      .join(',\n')}\n}`,
    ...members,
  };
}

export function updateQuery(
  query: Query,
  memberType: QueryMemberKey,
  key: string
) {
  const updatedQuery: Query = JSON.parse(JSON.stringify(query));

  if (memberType === 'timeDimensions') {
    if (updatedQuery.timeDimensions?.[0]?.dimension === key) {
      delete updatedQuery.timeDimensions;
    } else {
      updatedQuery.timeDimensions = [
        {
          // defafult granularity
          granularity: 'day',
          ...updatedQuery.timeDimensions?.[0],
          dimension: key,
        },
      ];
    }
  } else {
    if (updatedQuery[memberType]?.includes(key)) {
      updatedQuery[memberType] = updatedQuery[memberType]!.filter(
        (k) => key !== k
      );
    } else {
      updatedQuery[memberType] = [...(updatedQuery[memberType] || []), key];
    }
  }

  return updatedQuery;
}

// todo: refactor without Ramda
export function canUsePreAggregationForTransformedQuery(
  transformedQuery: TransformedQuery,
  query: Query
) {
  function sortTimeDimensions(timeDimensions) {
    return (
      (timeDimensions &&
        sortBy(
          // @ts-ignore
          (d) => d.join('.'),
          timeDimensions.map((d) => [d.dimension, d.granularity || 'day']) // TODO granularity shouldn't be null?
        )) ||
      []
    );
  }
  // TimeDimension :: [Dimension, Granularity]
  // TimeDimension -> [TimeDimension]
  function expandTimeDimension(timeDimension) {
    const [dimension, granularity] = timeDimension;
    const makeTimeDimension = (newGranularity) => [dimension, newGranularity];

    return (
      transformedQuery.granularityHierarchies[granularity] || [granularity]
    ).map(makeTimeDimension);
  }
  // [[TimeDimension]]
  const queryTimeDimensionsList =
    transformedQuery.sortedTimeDimensions.map(expandTimeDimension);

  function canUsePreAggregationNotAdditive(references) {
    return (
      transformedQuery.hasNoTimeDimensionsWithoutGranularity &&
      transformedQuery.allFiltersWithinSelectedDimensions &&
      equals(
        references.sortedDimensions || references.dimensions,
        transformedQuery.sortedDimensions
      ) &&
      (all(
        (m) => references.measures.includes(m),
        transformedQuery.measures
      ) ||
        all(
          (m) => references.measures.includes(m),
          transformedQuery.leafMeasures
        )) &&
      equals(
        transformedQuery.sortedTimeDimensions,
        references.sortedTimeDimensions ||
          sortTimeDimensions(references.timeDimensions)
      )
    );
  }

  function canUsePreAggregationLeafMeasureAdditive(references) {
    return (
      all(
        (d) =>
          (references.sortedDimensions || references.dimensions).includes(d),
        transformedQuery.sortedDimensions
      ) &&
      all(
        (m) => references.measures.includes(m),
        transformedQuery.leafMeasures
      ) &&
      allPass(
        queryTimeDimensionsList.map((tds) =>
          anyPass(tds.map((td) => contains(td)))
        )
      )(
        references.sortedTimeDimensions ||
          sortTimeDimensions(references.timeDimensions)
      )
    );
  }

  let canUseFn;

  if (
    transformedQuery.leafMeasureAdditive &&
    !transformedQuery.hasMultipliedMeasures
  ) {
    canUseFn = (query) =>
      canUsePreAggregationLeafMeasureAdditive(query) ||
      canUsePreAggregationNotAdditive(query);
  } else {
    canUseFn = canUsePreAggregationNotAdditive;
  }

  if (query) {
    return canUseFn(query);
  } else {
    return canUseFn;
  }
}
