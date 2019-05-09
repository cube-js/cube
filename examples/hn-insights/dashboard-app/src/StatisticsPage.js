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

const tableRender = ({ resultSet }) => (
  <Table
    pagination={false}
    columns={resultSet.tableColumns().map(c => ({ ...c, dataIndex: c.key }))}
    dataSource={resultSet.tablePivot()}
  />
);

const velocityListRender = ({ resultSet }) => {
  const columns = [{
    title: 'Story',
    key: 'story',
    render: (text, item) => (
      <span>
        {item['Events.currentRank'] || '-'}.&nbsp;
        <Link to={`/stories/${item['Stories.id']}`}>{item['Stories.title']}</Link>
      </span>
    ),
  }, {
    title: 'Points last/prev hour',
    key: 'scoreChange',
    render: (text, item) => {
      const scoreLastHour = item['Events.scoreChangeLastHour'] && parseInt(item['Events.scoreChangeLastHour'], 10);
      const scorePrevHour = item['Events.scoreChangePrevHour'] && parseInt(item['Events.scoreChangePrevHour'], 10) || null;
      return <Statistic
        value={`+${scoreLastHour}`}
        valueStyle={{ color: scorePrevHour && (scoreLastHour >= scorePrevHour ? '#3f8600' : '#cf1322') }}
        prefix={scorePrevHour && <Icon
          type={scoreLastHour >= scorePrevHour ? 'arrow-up' : 'arrow-down'}/>}
        suffix={scorePrevHour && `+${scorePrevHour}`}
      />
    },
  }];

  return (
    <Table dataSource={resultSet.tablePivot()} columns={columns} pagination={false} />
  );
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
