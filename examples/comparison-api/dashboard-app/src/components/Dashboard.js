import React, { useState, useEffect } from 'react';
import { Row, Col } from 'antd';
import { useCubeQuery } from '@cubejs-client/react';
import * as moment from 'moment';

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
      let temp = result.loadResponse.results.map((year, i) => {
        return {
          name: moment(year.data[0]['Orders.createdAt.month']).format('YYYY'),
          data: year.data.map((item) => parseInt(item['Orders.count'])),
        };
      });
      setData(temp);
    }
  }, [result]);

  return (
    <React.Fragment>
      <Row gutter={20} className='dashboard__row'>
        <Col sm={24} lg={16}>
          <div className='dashboard__cell'>
            <Line data={data} />
          </div>
        </Col>
      </Row>
    </React.Fragment>
  );
};
