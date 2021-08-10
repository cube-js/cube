import {
  Query,
  TimeDimensionBase,
  TimeDimensionGranularity,
  TransformedQuery,
} from '@cubejs-client/core';
import { camelCase } from 'camel-case';

import { QueryMemberKey } from '../../types';

export type PreAggregationReferences = {
  measures: string[];
  dimensions: string[];
  timeDimensions: TimeDimensionBase[];
  timeDimension?: string;
  granularity?: TimeDimensionGranularity;
};

export type PreAggregationDefinition = {
  preAggregationName: string;
  cubeName: string;
  code: Object;
};

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
      updatedQuery.timeDimensions = [];
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
