import React from 'react';
import type { TimeDimension } from '@cubejs-client/core';

import QueryRenderer from './QueryRenderer';
import type { QueryRendererWithTotalsProps } from './types';

const QueryRendererWithTotals = ({ query, ...restProps }: QueryRendererWithTotalsProps) => (
  <QueryRenderer
    queries={{
      // `granularity: null` is sent to the API on purpose to get totals
      totals: {
        ...query,
        dimensions: [],
        timeDimensions: query.timeDimensions
          ? query.timeDimensions.map(td => ({ ...td, granularity: null }) as unknown as TimeDimension)
          : undefined
      },
      main: query
    }}
    {...restProps}
  />
);

export default QueryRendererWithTotals;
