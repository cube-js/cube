import React, { useState, useEffect } from 'react';
import { Row, Col } from 'antd';
import { useCubeQuery } from '@cubejs-client/react';
import * as moment from 'moment';

import Area from './Area';
import Map from './Map';
import Pie from './Pie';
import SolidGauge from './SolidGauge';
import Stack from './Stack';
import Stock from './Stock';

export default () => {
  const [regionsData, setRegionsData] = useState([0, 0]);
  const [pieData, setPieData] = useState([{ name: '', y: 1180 }]);
  const [ordersData, setOrdersData] = useState([]);
  const [stackedData, setStackedData] = useState({ x: [], data: [] });
  const [solidGaugeData, setSolidGaugeData] = useState();
  const [range, setRange] = useState('last year');
  const [region, setRegion] = useState(null);

  const { resultSet: regions } = useCubeQuery({
    measures: ['Orders.count'],
    dimensions: ['Users.state'],
    timeDimensions: [
      {
        dimension: 'Orders.createdAt',
        dateRange: range,
      },
    ],
  },
  { subscribe: true }
  );

  const { resultSet: pie } = useCubeQuery({
    measures: ['LineItems.quantity'],
    dimensions: ['ProductCategories.name'],
    timeDimensions: [
      {
        dimension: 'Orders.createdAt',
        dateRange: range,
      },
    ],
    order: {
      'ProductCategories.name': 'asc',
    },
    ...(region
      ? {
          filters: [
            {
              member: 'Users.state',
              operator: 'equals',
              values: [region],
            },
          ],
        }
      : {}),
  },
  { subscribe: true }
  );

  const { resultSet: orders } = useCubeQuery({
    measures: ['Orders.count'],
    dimensions: ['Orders.createdAt'],
    order: {
      'Orders.createdAt': 'asc',
    },
    timeDimensions: [
      {
        dimension: 'Orders.createdAt',
        dateRange: 'last year',
      },
    ],
    ...(region
      ? {
          filters: [
            {
              member: 'Users.state',
              operator: 'equals',
              values: [region],
            },
          ],
        }
      : {}),
  },
  { subscribe: true }
  );

  const { resultSet: stacked } = useCubeQuery({
    measures: ['LineItems.quantity'],
    dimensions: ['ProductCategories.name'],
    timeDimensions: [
      {
        dimension: 'Orders.createdAt',
        dateRange: range,
        granularity: 'month',
      },
    ],
    order: {
      'ProductCategories.name': 'asc',
    },
    ...(region
      ? {
          filters: [
            {
              member: 'Users.state',
              operator: 'equals',
              values: [region],
            },
          ],
        }
      : {}),
  });

  const { resultSet: solidGauge } = useCubeQuery({
    measures: ['Orders.count'],
    dimensions: ['Orders.status'],
    timeDimensions: [
      {
        dimension: 'Orders.createdAt',
        dateRange: range,
      },
    ],
    order: {
      'Orders.createdAt': 'asc',
    },
    ...(region
      ? {
          filters: [
            {
              member: 'Users.state',
              operator: 'equals',
              values: [region],
            },
          ],
        }
      : {}),
  });

  useEffect(() => {
    if (solidGauge) {
      let temp = [];
      let sum = 0;
      solidGauge.tablePivot().map((item) => {
        temp.push([item['Orders.status'], parseInt(item['Orders.count'])]);
        sum += parseInt(item['Orders.count']);
      });
      setSolidGaugeData({ status: temp, count: sum });
    }
  }, [solidGauge]);

  useEffect(() => {
    if (pie) {
      let temp = [];
      pie.tablePivot().map((item) => {
        temp.push({
          name: item['ProductCategories.name'],
          y: parseInt(item['LineItems.quantity']),
        });
      });
      setPieData(temp);
    }
  }, [pie]);

  useEffect(() => {
    if (orders) {
      let temp = [];
      orders.tablePivot().map((item) => {
        temp.push([
          parseInt(moment(item['Orders.createdAt']).format('x')),
          parseInt(item['Orders.count']),
        ]);
      });
      setOrdersData(temp);
    }
  }, [orders]);

  useEffect(() => {
    if (regions) {
      let temp = [];
      regions.tablePivot().map((item) => {
        temp.push([item['Users.state'], parseInt(item['Orders.count'])]);
      });
      setRegionsData(temp);
    }
  }, [regions]);

  useEffect(() => {
    if (stacked) {
      let range = {};
      let categories = new Set();
      stacked.tablePivot().map((item) => {
        categories.add(moment(item['Orders.createdAt.month']).format('MMMM'));
        if (!range[item['ProductCategories.name']]) {
          range[item['ProductCategories.name']] = {
            name: item['ProductCategories.name'],
            data: [],
          };
        }
        range[item['ProductCategories.name']].data.push(
          parseInt(item['LineItems.quantity'])
        );
      });
      setStackedData({ x: Array.from(categories), data: Object.values(range) });
    }
  }, [stacked]);

  return (
    <React.Fragment>
      <Row gutter={20} className='dashboard__row'>
        <Col sm={24} lg={8}>
          <div className='dashboard__cell'>
            <Map
              data={regionsData}
              setRegion={(data) => {
                setRegion(data);
              }}
            />
          </div>
        </Col>
        <Col sm={24} lg={16}>
          <div className='dashboard__cell'>
            <Stock
              data={ordersData}
              setRange={(data) => {
                setRange([
                  moment(data[0]).format('YYYY-MM-DD'),
                  moment(data[1]).format('YYYY-MM-DD'),
                ]);
              }}
            />
          </div>
        </Col>
      </Row>

      <Row gutter={20} className='dashboard__row'>
        <Col sm={24} lg={8}>
          <div className='dashboard__cell'>
            <Pie data={pieData} />
          </div>
        </Col>
        <Col sm={24} lg={16}>
          <div className='dashboard__cell'>
            <Stack categories={stackedData.x} data={stackedData.data} />
          </div>
        </Col>
      </Row>

      <Row gutter={20} className='dashboard__row'>
        <Col sm={24} lg={16}>
          <div className='dashboard__cell'>
            <Area categories={stackedData.x} data={stackedData.data} />
          </div>
        </Col>
        <Col sm={24} lg={8}>
          <div className='dashboard__cell'>
            <SolidGauge data={solidGaugeData} />
          </div>
        </Col>
      </Row>
    </React.Fragment>
  );
};
