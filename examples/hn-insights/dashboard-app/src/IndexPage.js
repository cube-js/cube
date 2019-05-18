import React, { useState } from "react";
import { Link } from 'react-router-dom'
import { Row, Col, Card, Spin, Statistic, Table, Layout, List, Icon, Input } from "antd";
import "antd/dist/antd.css";
import "./index.css";
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
        {item['Stories.currentRank'] || '-'}.&nbsp;
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
        value={scoreLastHour && `+${scoreLastHour}` || 'N/A'}
        valueStyle={{ color: scorePrevHour && (scoreLastHour >= scorePrevHour ? '#3f8600' : '#cf1322') }}
        prefix={scorePrevHour && <Icon
          type={scoreLastHour >= scorePrevHour ? 'arrow-up' : 'arrow-down'}/>}
        suffix={scorePrevHour && `+${scorePrevHour}`}
      />
    },
  }, {
    title: 'Rank Points',
    key: 'currentRankPoints',
    render: (text, item) => {
      const score = item["Stories.currentRankScore"] && Math.round(item["Stories.currentRankScore"] * 1000);
      return <Statistic
        value={score || 'N/A'}
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
  const leaderBoard = (
    <Dashboard>
      <DashboardItem size={12} title="Top Stories">
        <QueryRenderer
          query={{
            measures: [
              "Events.scoreChangeLastHour",
              "Events.scoreChangePrevHour"
            ],
            dimensions: [
              "Stories.id",
              "Stories.title",
              "Stories.currentRank",
              "Stories.currentRankScore"
            ],
            order: {
              "Stories.currentRank": 'asc'
            },
            filters: [{
              dimension: "Stories.currentRank",
              operator: 'set'
            }],
            limit: 20
          }}
          cubejsApi={cubejsApi}
          render={renderChart(velocityListRender)}
        />
      </DashboardItem>
      <DashboardItem size={12} title="Recently Added to Front Page">
        <QueryRenderer
          query={{
            measures: [
              "Events.scoreChangeLastHour",
              "Events.scoreChangePrevHour"
            ],
            filters: [
              {
                dimension: "Stories.minutesToFrontPage",
                operator: "set"
              }
            ],
            dimensions: [
              "Stories.id",
              "Stories.title",
              "Stories.postedTime",
              "Stories.addedToFrontPage",
              "Stories.currentRank",
              "Stories.currentRankScore"
            ],
            order: {
              "Stories.addedToFrontPage": "desc"
            },
            limit: 20
          }}
          cubejsApi={cubejsApi}
          render={renderChart(velocityListRender)}
        />
      </DashboardItem>
    </Dashboard>
  );

  const [search, setSearch] = useState('');

  const idMatch = search.match(/\?id=(\d+)|^(\d+)$/);

  return (
    <div>
      <Input.Search
        placeholder="Story id, url or title"
        onSearch={setSearch}
        size="large"
        enterButton
        style={{ marginBottom: 16 }}
      />
      {search ? <Dashboard>
        <DashboardItem size={24} title="Search Results">
          <QueryRenderer
            query={{
              measures: [
                "Events.scoreChangeLastHour",
                "Events.scoreChangePrevHour",
              ],
              dimensions: [
                "Stories.id",
                "Stories.title",
                "Stories.currentRank"
              ],
              filters: [idMatch ? {
                dimension:  'Stories.id',
                operator: 'equals',
                values: [idMatch[1] || idMatch[2]]
              } : {
                dimension:  'Stories.title',
                operator: 'contains',
                values: [search]
              }],
              order: {
                "Stories.postedTime": 'desc'
              },
              limit: 20
            }}
            cubejsApi={cubejsApi}
            render={renderChart(velocityListRender)}
          />
        </DashboardItem>
      </Dashboard> : <div><h2 style={{ textAlign: 'center' }}>Search story to track its performance</h2>{leaderBoard}</div>}
    </div>
  );
};

export default IndexPage;
