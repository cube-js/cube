import React, { useState, useEffect } from 'react';
import { useCubeQuery } from '@cubejs-client/react';
import * as moment from 'moment';

import Line from './Line';

export default () => {
  const [data, setData] = useState([]);

  const { resultSet: result22 } = useCubeQuery({
    measures: ['Orders.count'],
    timeDimensions: [
      {
        dimension: 'Orders.createdAt',
        dateRange: ['2022-01-01', '2022-12-31'],
        granularity: 'month',
      },
    ],
  });

  const { resultSet: result21 } = useCubeQuery({
    measures: ['Orders.count'],
    timeDimensions: [
      {
        dimension: 'Orders.createdAt',
        dateRange: ['2021-01-01', '2021-12-31'],
        granularity: 'month',
      },
    ],
  });

  const { resultSet: result20 } = useCubeQuery({
    measures: ['Orders.count'],
    timeDimensions: [
      {
        dimension: 'Orders.createdAt',
        dateRange: ['2020-01-01', '2020-12-31'],
        granularity: 'month',
      },
    ],
  });

  useEffect(() => {
    const parseResultSet = (resultSet) => {
      return {
        name: moment(
          resultSet.tablePivot()[0]['Orders.createdAt.month']
        ).format('YYYY'),
        data: resultSet
          .tablePivot()
          .map((item) => parseInt(item['Orders.count'])),
      };
    };

    const temp = [
      result22 ? parseResultSet(result22) : [],
      result21 ? parseResultSet(result21) : [],
      result20 ? parseResultSet(result20) : [],
    ];

    setData(temp);
  }, [result22, result21, result20]);

  return <Line data={data} title={'multiple queries'} />;
};
