import React, { useState, useEffect } from 'react';
import { useCubeQuery } from '@cubejs-client/react';

import Stack from './Stack';

export default () => {
  const [data, setData] = useState([]);

  const { resultSet: result } = useCubeQuery({
    measures: [
      'AllSales.onlineRevenuePercentage',
      'AllSales.offlineRevenuePercentage',
      'AllSales.commonPercentage',
    ],
    timeDimensions: [
      {
        dimension: 'AllSales.createdAt',
        dateRange: ['2022-01-01', '2022-12-31'],
        granularity: 'month',
      },
    ],
  });

  useEffect(() => {
    if (result) {
      const temp = [
        {
          name: 'Online',
          data: [],
          stack: 'half',
        },
        {
          name: 'Offline',
          data: [],
          stack: 'half',
        },
        {
          name: 'All sales',
          data: [],
          stack: 'common',
        },
      ];
      result.tablePivot().forEach((item) => {
        temp[0].data.push(parseFloat(item['AllSales.onlineRevenuePercentage']));
        temp[1].data.push(
          parseFloat(item['AllSales.offlineRevenuePercentage'])
        );
        temp[2].data.push(parseFloat(item['AllSales.commonPercentage']));
      });
      setData(temp);
    }
  }, [result]);

  return <Stack data={data} title={'a Data Blending cube'} />;
};
