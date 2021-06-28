import { TimeDimensionGranularity } from '@cubejs-client/core';

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
    lines.push(
      `measures: [${transformedQuery.leafMeasures.join(', ')}]`
    );
  }

  if (transformedQuery?.sortedDimensions.length) {
    members.dimensions = [...transformedQuery.sortedDimensions];
    lines.push(
      `dimensions: [${transformedQuery.sortedDimensions.join(', ')}]`
    );
  }

  if (transformedQuery?.sortedTimeDimensions.length) {
    members.timeDimension = transformedQuery.sortedTimeDimensions[0][0];
    members.granularity = transformedQuery.sortedTimeDimensions[0][1];

    lines.push(
      `timeDimension: ${transformedQuery.sortedTimeDimensions[0][0]}`
    );
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
