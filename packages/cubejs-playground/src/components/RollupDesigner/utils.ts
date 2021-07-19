import {
  Query,
  TimeDimensionBase,
  TimeDimensionGranularity,
  TransformedQuery,
} from '@cubejs-client/core';
import { camelCase } from 'camel-case';
import * as R from 'ramda';

import { QueryMemberKey } from '../../types';

export type PreAggregationReferences = {
  measures: string[];
  dimensions: string[];
  timeDimensions: TimeDimensionBase[];
  timeDimension?: string;
  granularity?: TimeDimensionGranularity;
};

export type PreAggregationDefinition = {
  value: string;
  code: string;
  references: PreAggregationReferences;
};

export function getPreAggregationDefinition(
  transformedQuery,
  preAggregationName = 'main'
): PreAggregationDefinition {
  const references: PreAggregationReferences = {
    measures: [],
    dimensions: [],
    timeDimensions: [],
  };
  let lines: string[] = [];

  if (transformedQuery?.leafMeasures.length) {
    references.measures = [...transformedQuery.leafMeasures];
    lines.push(`measures: [${transformedQuery.leafMeasures.join(', ')}]`);
  }

  if (transformedQuery?.sortedDimensions.length) {
    references.dimensions = [...transformedQuery.sortedDimensions];
    lines.push(`dimensions: [${transformedQuery.sortedDimensions.join(', ')}]`);
  }

  if (
    transformedQuery?.sortedTimeDimensions.length &&
    transformedQuery.sortedTimeDimensions[0]?.[1] != null
  ) {
    references.timeDimension = transformedQuery.sortedTimeDimensions[0][0];
    references.granularity = transformedQuery.sortedTimeDimensions[0][1];

    if (references.timeDimension) {
      references.timeDimensions = [
        {
          dimension: references.timeDimension,
          granularity: references.granularity,
        },
      ];
    }

    lines.push(`timeDimension: ${transformedQuery.sortedTimeDimensions[0][0]}`);
    lines.push(
      `granularity: \`${transformedQuery.sortedTimeDimensions[0][1]}\``
    );
  }

  const value = `{\n${lines.map((l) => `  ${l}`).join(',\n')}\n}`;

  return {
    code: `${camelCase(preAggregationName)}: ${value}`,
    value,
    references,
  };
}

export function getPreAggregationReferences(
  transformedQuery: TransformedQuery | null
): PreAggregationReferences {
  const references: PreAggregationReferences = {
    measures: [],
    dimensions: [],
    timeDimensions: [],
  };

  if (transformedQuery?.leafMeasures.length) {
    references.measures = [...transformedQuery.leafMeasures];
  }

  if (transformedQuery?.sortedDimensions.length) {
    references.dimensions = [...transformedQuery.sortedDimensions];
  }

  if (
    transformedQuery?.sortedTimeDimensions.length &&
    transformedQuery.sortedTimeDimensions[0]?.[1] != null
  ) {
    const [dimension, granularity] = transformedQuery.sortedTimeDimensions[0];
    references.timeDimensions = [
      {
        dimension,
        granularity: <TimeDimensionGranularity>granularity,
      },
    ];
  }

  return references;
}

type PreAggregationDefinitionResult = {
  code: string;
  value: Object;
};

export function getPreAggregationDefinitionFromReferences(
  references: PreAggregationReferences,
  name: string = 'main'
): PreAggregationDefinitionResult {
  const lines: string[] = [];

  if (references.measures.length) {
    lines.push(`  measures: [${references.measures.map((m) => m).join(', ')}]`);
  }

  if (references.dimensions.length) {
    lines.push(
      `  dimensions: [${references.dimensions.map((m) => m).join(', ')}]`
    );
  }

  if (references.timeDimensions.length) {
    const { dimension, granularity } = references.timeDimensions[0];

    lines.push(`  timeDimension: ${dimension}`);

    if (granularity) {
      lines.push(`  granularity: \`${granularity}\``);
    }
  }

  const value = `{\n${lines.join(',\n')}\n}`;

  return {
    code: `${camelCase(name)}: ${value}`,
    value,
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
  transformedQuery: any,
  refs: any
) {
  function sortTimeDimensions(timeDimensions) {
    return (
      (timeDimensions &&
        R.sortBy(
          (d: any) => d.join('.'),
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

  const canUsePreAggregationNotAdditive = (references) => {
    return (
      transformedQuery.hasNoTimeDimensionsWithoutGranularity &&
      transformedQuery.allFiltersWithinSelectedDimensions &&
      R.equals(
        references.sortedDimensions || references.dimensions,
        transformedQuery.sortedDimensions
      ) &&
      (R.all(
        (m) => references.measures.indexOf(m) !== -1,
        transformedQuery.measures
      ) ||
        R.all(
          (m) => references.measures.indexOf(m) !== -1,
          transformedQuery.leafMeasures
        )) &&
      R.equals(
        transformedQuery.sortedTimeDimensions,
        references.sortedTimeDimensions ||
          sortTimeDimensions(references.timeDimensions)
      )
    );
  };

  const canUsePreAggregationLeafMeasureAdditive = (references) => {
    return (
      R.all(
        (d) =>
          (references.sortedDimensions || references.dimensions).indexOf(d) !==
          -1,
        transformedQuery.sortedDimensions
      ) &&
      R.all(
        (m) => references.measures.indexOf(m) !== -1,
        transformedQuery.leafMeasures
      ) &&
      R.allPass(
        queryTimeDimensionsList.map((tds) =>
          R.anyPass(tds.map((td) => R.contains(td)))
        )
      )(
        references.sortedTimeDimensions ||
          sortTimeDimensions(references.timeDimensions)
      )
    );
  };

  let canUseFn;
  if (
    transformedQuery.leafMeasureAdditive &&
    !transformedQuery.hasMultipliedMeasures
  ) {
    canUseFn = (r) =>
      canUsePreAggregationLeafMeasureAdditive(r) ||
      canUsePreAggregationNotAdditive(r);
  } else {
    canUseFn = canUsePreAggregationNotAdditive;
  }
  if (refs) {
    return canUseFn(refs);
  } else {
    return canUseFn;
  }
}
