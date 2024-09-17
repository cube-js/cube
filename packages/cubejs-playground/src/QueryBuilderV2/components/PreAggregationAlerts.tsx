import { useMemo } from 'react';
import { SerializedResult } from '@cubejs-client/core';
import { Alert } from '@cube-dev/ui-kit';

import { useQueryBuilderContext } from '../context';

export function PreAggregationAlerts() {
  const { resultSet } = useQueryBuilderContext();
  const {
    external,
    // dbType,
    extDbType,
    usedPreAggregations = {},
  } = useMemo(() => {
    if (resultSet) {
      const { loadResponse } = resultSet?.serialize();

      return loadResponse.results[0] || {};
    }

    return {} as SerializedResult['loadResponse'];
  }, [resultSet?.rawData()]);

  // @ts-ignore
  const preAggregationType = Object.values(usedPreAggregations || {})[0]?.type;

  const isAggregated = Object.keys(usedPreAggregations).length > 0;

  return (
    <>
      {isAggregated && external && extDbType !== 'cubestore' ? (
        <Alert theme="note" padding="1x">
          Consider migrating your pre-aggregations to Cube Store for better performance with larger
          datasets
        </Alert>
      ) : null}

      {isAggregated && !external && preAggregationType !== 'originalSql' ? (
        <Alert theme="note" padding="1x">
          For optimized performance, consider using <b>external</b> {preAggregationType}{' '}
          pre-aggregation, rather than the source database (internal)
        </Alert>
      ) : null}
    </>
  );
}
