import React from "react";
import { Link } from 'react-router-dom'
import { Row, Col, Card, Spin, Statistic, Table, Layout, List, Icon } from "antd";
import "antd/dist/antd.css";
import cubejs from "@cubejs-client/core";
import { QueryRenderer } from "@cubejs-client/react";
import { Chart, Axis, Tooltip, Geom, Coord, Legend } from "bizcharts";
import moment from "moment";
const { Header, Footer, Sider, Content } = Layout;

const Dashboard = ({ children }) => (
  <Row type="flex" justify="space-around" align="top" gutter={24}>
    {children}
  </Row>
);

const DashboardItem = ({ children, title, size }) => (
  <Col span={24} lg={size || 12}>
    <Card
      title={title}
      style={{
        marginBottom: "24px"
      }}
    >
      {children}
    </Card>
  </Col>
);

const stackedChartData = resultSet => {
  const data = resultSet
    .pivot()
    .map(({ xValues, yValuesArray }) =>
      yValuesArray.map(([yValues, m]) => ({
        x: resultSet.axisValuesString(xValues, ", "),
        color: resultSet.axisValuesString(yValues, ", "),
        measure: m && Number.parseFloat(m)
      }))
    )
    .reduce((a, b) => a.concat(b));
  return data;
};

const lineRender = ({ resultSet }) => (
  <Chart
    scale={{
      x: {
        tickCount: 8
      }
    }}
    height={400}
    data={stackedChartData(resultSet)}
    forceFit
  >
    <Axis name="x" />
    <Axis name="measure" />
    <Tooltip
      crosshairs={{
        type: "y"
      }}
    />
    <Geom type="line" position={`x*measure`} size={2} color="color" />
  </Chart>
);

const renderChart = Component => ({ resultSet, error }) =>
  (resultSet && <Component resultSet={resultSet} />) ||
  (error && error.toString()) || <Spin />;

const IndexPage = ({ cubejsApi }) => {
  return (
    <Dashboard>
      <DashboardItem size={12} title="Score on front page by day of week and hour">
        <QueryRenderer
          query={{
            "measures": [
              "AverageVelocity.averageScorePerHour"
            ],
            "timeDimensions": [],
            "dimensions": [
              "AverageVelocity.day",
              "AverageVelocity.hour"
            ],
            order: {
              "AverageVelocity.day": 'asc',
              "AverageVelocity.hour": 'asc'
            },
            "filters": []
          }}
          cubejsApi={cubejsApi}
          render={renderChart(lineRender)}
        />
      </DashboardItem>
      <DashboardItem size={12} title="Score on front page by rank">
        <QueryRenderer
          query={{
            "measures": [
              "AverageVelocity.averageScorePerHour"
            ],
            "timeDimensions": [],
            "dimensions": [
              "AverageVelocity.rank"
            ],
            order: {
              "AverageVelocity.rank": 'asc'
            },
            "filters": []
          }}
          cubejsApi={cubejsApi}
          render={renderChart(lineRender)}
        />
      </DashboardItem>
      <DashboardItem size={12} title="Score on /newest page">
        <QueryRenderer
          query={{
            measures: ["Events.scoreChangeBeforeAddedToFrontPage"],
            timeDimensions: [
              {
                dimension: "Events.timestamp",
                granularity: "hour",
                dateRange: "Today"
              }
            ],
            filters: [
              {
                dimension: "Events.page",
                operator: "equals",
                values: ["newest"]
              }
            ]
          }}
          cubejsApi={cubejsApi}
          render={renderChart(lineRender)}
        />
      </DashboardItem>
      <DashboardItem size={12} title="Karma on /newest page">
        <QueryRenderer
          query={{
            measures: ["Events.karmaChangeBeforeAddedToFrontPage"],
            timeDimensions: [
              {
                dimension: "Events.timestamp",
                granularity: "hour",
                dateRange: "Today"
              }
            ],
            filters: [
              {
                dimension: "Events.page",
                operator: "equals",
                values: ["newest"]
              }
            ]
          }}
          cubejsApi={cubejsApi}
          render={renderChart(lineRender)}
        />
      </DashboardItem>
    </Dashboard>
  );
};

export default IndexPage;
