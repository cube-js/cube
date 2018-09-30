import React from 'react';
import QueryRenderer from './QueryRenderer.jsx';

export default ({ query, ...restProps }) => (
  <QueryRenderer queries={{
    totals: {
      ...query,
      dimensions: [],
      timeDimensions: query.timeDimensions ? query.timeDimensions.map(td => ({ ...td, granularity: null }) ) : undefined
    },
    main: query
  }} {...restProps}
  />
)