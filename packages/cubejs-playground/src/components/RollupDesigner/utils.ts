import { Query, TimeDimensionGranularity } from '@cubejs-client/core';

import { QueryMemberKey } from '../../types';

export type PreAggregationDefinition = {
  value: string;
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
  const members: Omit<PreAggregationDefinition, 'code' | 'value'> = {
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

  const value = `{\n${lines.map((l) => `  ${l}`).join(',\n')}\n}`;

  return {
    code: `${preAggregationName}: ${value}`,
    value,
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
