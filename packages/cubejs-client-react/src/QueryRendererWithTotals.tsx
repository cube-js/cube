import React from 'react';
import QueryRenderer from './QueryRenderer';

const QueryRendererWithTotals = ({ query, ...restProps }) => (
  <QueryRenderer
    queries={{
      totals: {
        ...query,
        dimensions: [],
        timeDimensions: query.timeDimensions
          ? query.timeDimensions.map(td => ({ ...td, granularity: null }))
          : undefined
      },
      main: query
    }}
    {...restProps}
  />
);

QueryRendererWithTotals.defaultProps = {
  query: null,
  render: null,
  queries: null,
  loadSql: null
};

export default QueryRendererWithTotals;
