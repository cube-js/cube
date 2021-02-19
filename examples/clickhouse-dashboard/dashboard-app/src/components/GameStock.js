import React, { useState, useEffect } from 'react';
import { Row, Col, Button, Select, Space, Card, Layout } from 'antd';
import { useCubeQuery } from '@cubejs-client/react';
import Chart from 'react-apexcharts';

const GameStock = () => {
  const { resultSet } = useCubeQuery({
    dimensions: [ 'Stocks.ticker' ],
  });

  const [ tickers, setTickers ] = useState([]);
  const [ selectedTicker, setSelectedTicker ] = useState('GME');

  useEffect(() => {
    if (resultSet) {
      setTickers(resultSet.tablePivot().map(x => x['Stocks.ticker']).map(x => ({ label: x, value: x })));
    }
  }, [ resultSet ]);

  const [ dateRange, setDateRange ] = useState(dateRange2021);

  return (
    <Layout>
      <Layout.Header style={{ backgroundColor: '#43436B' }}>
        <Space size='large'>
          <a href='https://cube.dev' target='_blank'>
            <img src='https://cubejs.s3-us-west-2.amazonaws.com/downloads/logo-full.svg' alt='Cube.js' />
          </a>
          <Space>
            <Button href='https://github.com/cube-js/cube.js' target='_blank' ghost>GitHub</Button>
            <Button href='https://slack.cube.dev' target='_blank' ghost>Slack</Button>
          </Space>
        </Space>
      </Layout.Header>
      <div style={{ padding: 50 }}>
        <Row gutter={[ 50, 50 ]}>
          <Col span={24}>
            <Space>
              Ticker
              <Select
                style={{ width: 100 }}
                showSearch
                options={tickers}
                value={selectedTicker}
                loading={!selectedTicker}
                onChange={setSelectedTicker}
                filterOption={(input, option) =>
                  option.value.toLowerCase().indexOf(input.toLowerCase()) === 0
                }
              />
              or
              {prominentTickers.map(t => (
                <Button
                  key={t}
                  size='small'
                  type={t === selectedTicker ? 'primary' : 'default'}
                  onClick={() => setSelectedTicker(t)}
                >{t}</Button>
              ))}
            </Space>
          </Col>
        </Row>
        <Row gutter={[ 50, 50 ]}>
          <Col span={24}>
            <Space>
              Time frame
              {dateRanges.map(([ label, range ]) => (
                <Button
                  key={label}
                  size='small'
                  value={range}
                  onClick={() => setDateRange(range)}
                  type={range === dateRange ? 'primary' : 'default'}
                >{label}</Button>
              ))}
            </Space>
          </Col>
        </Row>
        <Row gutter={[ 50, 50 ]}>
          <Col span={24}>
            <Card style={{ maxWidth: dateRange === dateRange2021 ? '900px' : '100%' }}>
              {selectedTicker && (
                <CandlestickChart ticker={selectedTicker} dateRange={dateRange} />
              )}
            </Card>
          </Col>
        </Row>
      </div>
    </Layout>
  );
};

const CandlestickChart = ({ ticker, dateRange }) => {
  const granularity = dateRange !== undefined ? 'day' : 'month';

  const { resultSet } = useCubeQuery({
    measures: [ 'Stocks.open', 'Stocks.close', 'Stocks.high', 'Stocks.low' ],
    timeDimensions: [ {
      dimension: 'Stocks.date',
      granularity,
      dateRange,
    } ],
    filters: [ {
      dimension: 'Stocks.ticker',
      operator: 'equals',
      values: [ ticker ],
    } ],
  });

  const pivotConfig = {
    x: [ `Stocks.date.${granularity}` ],
    y: [ 'measures' ],
    joinDateRange: false,
    fillMissingDates: false,
  };

  const data = resultSet === null ? [] : resultSet.chartPivot(pivotConfig).map(row => {
    const max = Math.max(row['Stocks.open'], row['Stocks.high'], row['Stocks.low'], row['Stocks.close']);
    const precision = max >= 100 ? 0 : max >= 10 ? 1 : 2;

    return {
      x: new Date(row.x),
      y: [
        row['Stocks.open'].toFixed(precision),
        row['Stocks.high'].toFixed(precision),
        row['Stocks.low'].toFixed(precision),
        row['Stocks.close'].toFixed(precision),
      ],
    };
  });

  const options = {
    title: { text: '', align: 'left' },
    chart: { animations: { enabled: false }, toolbar: { show: false } },
    xaxis: { type: 'datetime' },
    yaxis: { labels: { formatter: v => Math.round(v) }, tooltip: { enabled: true } },
  };

  return <Chart
    options={options}
    series={[ { data } ]}
    type='candlestick'
    height={300} />;
};

const prominentTickers = [ 'BYND', 'GME', 'IRM', 'MAC', 'NOK', 'SPCE' ];

const dateRange202x = [ '2020-01-01', '2021-03-01' ];
const dateRange2021 = [ '2021-01-01', '2021-03-01' ];

const dateRanges = [
  [ '2021', dateRange2021 ],
  [ '2020 – 2021', dateRange202x ],
  [ 'All time', undefined ],
];

export default GameStock;
