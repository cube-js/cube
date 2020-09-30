import React, { useState, useEffect } from 'react';
import { useCubeQuery } from '@cubejs-client/react';

import Line from './Line';

export default () => {
  const [data, setData] = useState([]);

  const { resultSet: result } = useCubeQuery({
    measures: ['Orders.count'],
    timeDimensions: [
      {
        dimension: 'Orders.createdAt',
        compareDateRange: [
          ['2022-01-01', '2022-12-31'],
          ['2021-01-01', '2021-12-31'],
          ['2020-01-01', '2020-12-31'],
        ],
        granularity: 'month',
      },
    ],
  });

  useEffect(() => {
    if (result) {
      const temp = result.series().map((data) => {
        return {
          name: data.key.substring(0, 4),
          data: data.series.map((item) => item.value),
        };
      });
      setData(temp);
    }
  }, [result]);

  return <Line data={data} title={'the single query'} />;
};
