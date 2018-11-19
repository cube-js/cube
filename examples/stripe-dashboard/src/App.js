import React, { Component } from 'react';
import { Container, Row, Col } from 'reactstrap';
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer, PieChart, Pie, Cell, Legend, BarChart, Bar } from 'recharts';
import moment from 'moment';
import numeral from 'numeral';
import cubejs from '@cubejs-client/core';
import Chart from './Chart.js';
import GithubCorner from 'react-github-corner';

const cubejsApi = cubejs(process.env.REACT_APP_CUBEJS_TOKEN);
const currencyFormatter = (item) => numeral(item).format('$0,0')
const dateFormatter = (item) => moment(item).format('MMM YY')

class App extends Component {
  render() {
    return (
      <Container fluid>
        <Row>
          <Col md="8">
            <Chart
              cubejsApi={cubejsApi}
              title="MRR Over Time"
              query={{
                measures: ['StripeSaaSMetrics.mrr'],
                timeDimensions: [{
                  dimension: 'StripeSaaSMetrics.time',
                  dateRange: ['2016-01-01', '2017-12-31'],
                  granularity: 'month'
                }]
              }}
              render={(resultSet) => (
                <ResponsiveContainer width="100%" height={300}>
                  <AreaChart data={resultSet.chartPivot()}>
                    <XAxis dataKey="category" tickFormatter={dateFormatter} fontSize={12} />
                    <YAxis tickFormatter={currencyFormatter} fontSize={12}/>
                    <Tooltip formatter={currencyFormatter} labelFormatter={dateFormatter} />
                    <Area type="monotone" dataKey="StripeSaaSMetrics.mrr" name="MRR" stroke="rgb(106, 110, 229)" fill="rgba(106, 110, 229, .16)" />
                  </AreaChart>
                </ResponsiveContainer>
              )}
            />
          </Col>
          <Col md="4">
            <Chart
              cubejsApi={cubejsApi}
              title="Current MRR"
              query={{
                measures: ['StripeSaaSMetrics.mrr'],
                timeDimensions: [{
                  dimension: 'StripeSaaSMetrics.time',
                  dateRange: [new Date().toISOString().substring(0,10)],
                  granularity: null
                }]
              }}
              render={(resultSet) => (
                <h1 height={300}>
                  { numeral(resultSet.chartPivot()[0]['StripeSaaSMetrics.mrr']).format('$0,0.00') }
                </h1>
              )}
            />
          </Col>
        </Row>
        <Row>
          <Col md="6">
            <Chart
              cubejsApi={cubejsApi}
              title="MRR by Plans Breakout"
              query={{
                measures: ['StripeSaaSMetrics.mrr'],
                dimensions: ['StripeSaaSMetrics.plan']
              }}
              render={(resultSet) => {
                const colors = ['#7DB3FF', '#49457B', '#FF7C78', '#FED3D0'];
                return (
                  <ResponsiveContainer width="100%" height={300}>
                    <PieChart>
                      <Pie
                        data={resultSet.chartPivot()}
                        nameKey="category"
                        dataKey="StripeSaaSMetrics.mrr"
                      >
                        {
                          resultSet.chartPivot().map((entry, index) => (
                            <Cell fill={colors[index % colors.length]}/>
                          ))
                        }
                      </Pie>
                      <Legend verticalAlign="middle" align="right" layout="vertical" />
                      <Tooltip formatter={currencyFormatter} />
                    </PieChart>
                  </ResponsiveContainer>
                )
              }}
            />
          </Col>
          <Col md="6">
            <Chart
              cubejsApi={cubejsApi}
              title="Active Customers"
              query={{
                measures: ['StripeSaaSMetrics.activeCustomers'],
                timeDimensions: [{
                  dimension: 'StripeSaaSMetrics.time',
                  dateRange: ['2016-01-01', '2017-12-30'],
                  granularity: 'month'
                }]
              }}
              render={(resultSet) => (
                <ResponsiveContainer width="100%" height={300}>
                  <BarChart data={resultSet.chartPivot()}>
                    <XAxis dataKey="category" tickFormatter={dateFormatter} fontSize={12} />
                    <YAxis fontSize={12} />
                    <Tooltip labelFormatter={dateFormatter} />
                    <Bar dataKey="StripeSaaSMetrics.activeCustomers" name="Active Customers" fill="rgb(106, 110, 229)" />
                  </BarChart>
                </ResponsiveContainer>
              )}
            />
          </Col>
        </Row>
        <GithubCorner size={120} href="https://github.com/statsbotco/cubejs-client/tree/master/examples/stripe-dashboard" />
      </Container>
    );
  }
}

export default App;
