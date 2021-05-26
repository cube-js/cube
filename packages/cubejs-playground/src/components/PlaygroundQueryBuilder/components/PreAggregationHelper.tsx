import { Typography } from 'antd';
import { TransformedQuery } from '@cubejs-client/core';

import { CodeSnippet } from '../../../atoms';

const { Paragraph, Link } = Typography;

function preAggregationsCodeString(transformedQuery) {
  let lines = ['type: `rollup`'];

  if (transformedQuery?.leafMeasures.length) {
    lines.push(
      `measureReferences: [${transformedQuery.leafMeasures.join(', ')}]`
    );
  }

  if (transformedQuery?.sortedDimensions.length) {
    lines.push(
      `dimensionReferences: [${transformedQuery.sortedDimensions.join(', ')}]`
    );
  }

  if (transformedQuery?.sortedTimeDimensions.length) {
    lines.push(
      `timeDimensionReference: ${transformedQuery.sortedTimeDimensions[0][0]}`
    );
    lines.push(
      `granularity: \`${transformedQuery.sortedTimeDimensions[0][1]}\``
    );
  }

  return `preAggregationName: {
${lines.map((l) => `  ${l}`).join(',\n')}
}`;
}

type PreAggregationHelperProps = {
  transformedQuery: TransformedQuery;
};

export function PreAggregationHelper({
  transformedQuery,
}: PreAggregationHelperProps) {
  const preAggCode =
    transformedQuery.leafMeasureAdditive &&
    !transformedQuery.hasMultipliedMeasures &&
    preAggregationsCodeString(transformedQuery);

  const cubeName =
    transformedQuery &&
    (
      transformedQuery.leafMeasures[0] ||
      transformedQuery.sortedDimensions[0] ||
      'your'
    ).split('.')[0];

  return (
    <>
      {preAggCode ? (
        <>
          <Paragraph>
            Following pre-aggregation is exact match to your query. <br />
            Use it to speed up this and other related queries.
          </Paragraph>

          <Paragraph>
            Add the following pre-aggregation to the <b>{cubeName}</b> cube.
          </Paragraph>

          <CodeSnippet code={preAggCode} />
        </>
      ) : (
        <>
          <Link
            href="!https://cube.dev/docs/pre-aggregations#rollup-rollup-selection-rules"
            target="_blank"
          >
            Current query cannot be rolled up due to it is not additive
          </Link>
          . Please consider removing not additive measures like `countDistinct`
          or `avg`. You can also try to use{' '}
          <Link
            href="!https://cube.dev/docs/pre-aggregations#original-sql"
            target="_blank"
          >
            originalSql
          </Link>{' '}
          pre-aggregation instead.
        </>
      )}
    </>
  );
}
