import React from 'react';
import QueryRenderer from './QueryRenderer.jsx';

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

export default QueryRendererWithTotals;
