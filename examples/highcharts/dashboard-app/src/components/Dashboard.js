import React, { useState, useEffect } from "react";
import { Row, Col } from "antd";
import { useCubeQuery } from "@cubejs-client/react";

import * as moment from 'moment';

import Funnel from './Funnel';
import MasterDetail from './MasterDetail';
import Map from './Map';
import Stack from './Stack';
import Lines from './Lines';
import Pie from './Pie';

export default () => {
  const [regionsData, setRegionsData] = useState([0, 0]);
  const [pieData, setPieData] = useState([{ "name": "Beauty", "y": 1180 }]);
  const [ordersData, setOrdersData] = useState([]);
  const [stackedData, setStackedData] = useState({ x: [], data: [] });
  const [funnelData, setFunnelData] = useState();
  const [range, setRange] = useState('last year');
  const [region, setRegion] = useState(null);


  const { resultSet: regions } = useCubeQuery({
    measures: ['Orders.count'],
    dimensions: [
      'Users.state',
    ],
    timeDimensions: [{
      dimension: 'Orders.createdAt',
      dateRange: range
    }]
  });

  const { resultSet: pie } = useCubeQuery({
    measures: ['LineItems.quantity'],
    dimensions: [
      'ProductCategories.name',
    ],
    timeDimensions: [{
      dimension: 'Orders.createdAt',
      dateRange: range
    }],
    ...(region ? {
      filters: [
        {
          member: "Users.state",
          operator: "equals",
          values: [region]
        }
      ]
    } : {})
  });

  const { resultSet: orders } = useCubeQuery({
    measures: ['Orders.count'],
    dimensions: [
      'Orders.createdAt',
    ],
    order: {
      'Orders.createdAt': 'asc'
    },
    ...(region ? {
      filters: [
        {
          member: "Users.state",
          operator: "equals",
          values: [region]
        }
      ]
    } : {})
  });


  const { resultSet: stacked } = useCubeQuery({
    measures: ['LineItems.quantity'],
    dimensions: [
      'ProductCategories.name',
    ],
    timeDimensions: [{
      dimension: 'Orders.createdAt',
      dateRange: range,
      granularity: 'month'
    }],
    ...(region ? {
      filters: [
        {
          member: "Users.state",
          operator: "equals",
          values: [region]
        }
      ]
    } : {})
  });

  const { resultSet: funnel } = useCubeQuery({
    measures: ['Orders.count'],
    dimensions: [
      'Orders.status',
    ],
    timeDimensions: [{
      dimension: 'Orders.createdAt',
      dateRange: range
    }],
    ...(region ? {
      filters: [
        {
          member: "Users.state",
          operator: "equals",
          values: [region]
        }
      ]
    } : {})
  });


  useEffect(() => {
    if (funnel) {
      let temp = []
      funnel.tablePivot().map(item => {
        temp.push([item['Orders.status'], parseInt(item['Orders.count'])]);
      });
      setFunnelData(temp);
    }
  }, [funnel]);

  useEffect(() => {
    if (pie) {
      let temp = []
      pie.tablePivot().map(item => {
        temp.push(
          {
            name: item['ProductCategories.name'],
            y: parseInt(item['LineItems.quantity']),
          }
        );
      })
      setPieData(temp);
    }
  }, [pie])

  useEffect(() => {
    if (orders) {
      let temp = [];
      orders.tablePivot().map(item => {
        temp.push([parseInt(moment(item['Orders.createdAt']).format('x')), parseInt(item['Orders.count'])]);
      })
      setOrdersData(temp);
    }
  }, [orders]);

  console.log(ordersData);

  useEffect(() => {
    if (regions) {
      let temp = [];
      regions.tablePivot().map(item => {
        temp.push([item['Users.state'], parseInt(item['Orders.count'])]);
      });
      setRegionsData(temp);
    }
  }, [regions]);

  useEffect(() => {
    if (stacked) {
      let range = {};
      let categories = new Set();
      stacked.tablePivot().map(item => {
        categories.add(moment(item['Orders.createdAt.month']).format('YYYY-MM-DD'));
        if (!range[item['ProductCategories.name']]) {
          range[item['ProductCategories.name']] = {
            name: item['ProductCategories.name'],
            data: []
          };
        };
        range[item['ProductCategories.name']].data.push(parseInt(item['LineItems.quantity']));
      })
      setStackedData({ x: Array.from(categories), data: Object.values(range) });
    }
  }, [stacked]);

  return (
    <React.Fragment>
      <Row gutter={20} className='dashboard__row'>
        <Col sm={24} lg={8}>
          <Map data={regionsData} setRegion={(data) => {
            setRegion(data);
          }} />
        </Col>
        <Col sm={24} lg={16}>
          <MasterDetail data={ordersData} setRange={(data) => {
            setRange([
              moment(data[0]).format('YYYY-MM-DD'),
              moment(data[1]).format('YYYY-MM-DD'),
            ]);
          }} />
        </Col>
      </Row>

      <Row gutter={20} className='dashboard__row'>
        <Col sm={24} lg={8}>
          <Pie data={pieData} />
        </Col>
        <Col sm={24} lg={16}>
          <Stack categories={stackedData.x} data={stackedData.data} />
        </Col>
      </Row>

      <Row gutter={20} className='dashboard__row'>
        <Col sm={24} lg={16}>
          <Lines categories={stackedData.x} data={stackedData.data} />
        </Col>
        <Col sm={24} lg={8}>
          <Funnel data={funnelData} />
        </Col>
      </Row>
    </React.Fragment>
  )
}