import React from 'react';
import * as PropTypes from 'prop-types';
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

QueryRendererWithTotals.propTypes = {
  render: PropTypes.func,
  cubejsApi: PropTypes.object.isRequired,
  query: PropTypes.object,
  queries: PropTypes.object,
  loadSql: PropTypes.any
};

QueryRendererWithTotals.defaultProps = {
  query: null,
  render: null,
  queries: null,
  loadSql: null
};

export default QueryRendererWithTotals;
