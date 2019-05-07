import React from "react";
import { Row, Col, Card, Spin, Statistic, Table, Layout } from "antd";
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

const lineRender = ({ resultSet }) => (
  <Chart scale={{ x: { tickCount: 8 } }} height={400} data={stackedChartData(resultSet)} forceFit>
    <Axis name="x" />
    <Axis name="measure" />
    <Tooltip crosshairs={{type : 'y'}} />
    <Geom type="line" position={`x*measure`} size={2} color="color" />
  </Chart>
);

const StoryCardMeta = ({ span, title, description }) => (
  <Col span={span} style={{ marginBottom: 16 }}>
    <Card.Meta
      title={title}
      description={description}
    />
  </Col>
);

const storyCardRender = ({ resultSet }) => {
  const data = resultSet.tablePivot()[0];

  return (
    <Row type="flex" gutter={24}>
      <StoryCardMeta
        span={12}
        title="Title"
        description={
          <span>
            {data['Events.currentRank'] && `${data['Events.currentRank']}. `}
            <a
              href={data['Stories.href'].indexOf('http') !== 0 ? `https://news.ycombinator.com/${data['Stories.href']}` : data['Stories.href']}
              target="_blank" rel="noopener noreferrer"
            >
              {data['Stories.title']}
            </a>
            </span>
        }
      />
      <StoryCardMeta
        span={12}
        title="Top Rank"
        description={<span>{data['Events.topRank']}</span>}
      />
      <StoryCardMeta
        span={12}
        title="Posted Time"
        description={<span>{data['Stories.postedTime']}</span>}
      />
      <StoryCardMeta
        span={12}
        title="Added to Front Page"
        description={<span>{data['Stories.addedToFrontPage']}</span>}
      />
      <StoryCardMeta
        span={12}
        title="Minutes to Front Page"
        description={<span>{data['Stories.minutesToFrontPage']}</span>}
      />
      <StoryCardMeta
        span={12}
        title="Points Before Front Page"
        description={<span>{data['Events.scoreChangeBeforeAddedToFrontPage']}</span>}
      />
      <StoryCardMeta
        span={12}
        title="Comments Before Front Page"
        description={<span>{data['Events.commentsBeforeAddedToFrontPage']}</span>}
      />
      <StoryCardMeta
        span={12}
        title="Minutes on First Page"
        description={<span>{data['Events.minutesOnFirstPage']}</span>}
      />
    </Row>
  );
};

const API_URL = "http://localhost:4000";
const cubejsApi = cubejs(
  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1NTYxMzU5NDIsImV4cCI6MTU1NjIyMjM0Mn0.aW71JQ-eVm7N7XXCsdbzK2FPxLpqamL8QFD0h8BHaoU",
  {
    apiUrl: API_URL + "/cubejs-api/v1"
  }
);

const renderChart = Component => ({ resultSet, error }) =>
  (resultSet && <Component resultSet={resultSet} />) ||
  (error && error.toString()) || <Spin />;

const StoryPage = ({ match: { params: { storyId } } }) => {
  return (
    <Dashboard>
      <DashboardItem size={12} title="Story">
        <QueryRenderer
          query={{
            measures: [
              "Events.scoreChangeBeforeAddedToFrontPage",
              "Events.commentsBeforeAddedToFrontPage",
              "Events.minutesOnFirstPage",
              "Events.topRank",
              "Events.currentRank"
            ],
            timeDimensions: [
              {
                dimension: "Events.timestamp",
                dateRange: "from 7 days ago to now"
              }
            ],
            dimensions: [
              "Stories.id",
              "Stories.title",
              "Stories.href",
              "Stories.postedTime",
              "Stories.addedToFrontPage",
              "Stories.minutesToFrontPage"
            ],
            "filters": [
              {
                "dimension": "Stories.id",
                "operator": "equals",
                "values": [storyId]
              }
            ]
          }}
          cubejsApi={cubejsApi}
          render={renderChart(storyCardRender)}
        />
      </DashboardItem>
      <DashboardItem size={12} title="Points per hour">
        <QueryRenderer
          query={{
            "measures": [
              "Events.scoreChange"
            ],
            "timeDimensions": [
              {
                "dimension": "Events.timestamp",
                "granularity": "hour",
                dateRange: "today"
              }
            ],
            "filters": [
              {
                "dimension": "Stories.id",
                "operator": "equals",
                "values": [storyId]
              },
              {
                "dimension": "Events.page",
                "operator": "equals",
                "values": [
                  "front"
                ]
              }
            ]
          }}
          cubejsApi={cubejsApi}
          render={renderChart(lineRender)}
        />
      </DashboardItem>
      <DashboardItem size={12} title="Comments per hour">
        <QueryRenderer
          query={{
            "measures": [
              "Events.commentsChange"
            ],
            "timeDimensions": [
              {
                "dimension": "Events.timestamp",
                "granularity": "hour",
                dateRange: "today" // TODO
              }
            ],
            "filters": [
              {
                "dimension": "Stories.id",
                "operator": "equals",
                "values": [storyId]
              },
              {
                "dimension": "Events.page",
                "operator": "equals",
                "values": [
                  "front"
                ]
              }
            ]
          }}
          cubejsApi={cubejsApi}
          render={renderChart(lineRender)}
        />
      </DashboardItem>
      <DashboardItem size={12} title="Rank">
        <QueryRenderer
          query={{
            "measures": [
              "Events.topRank"
            ],
            "timeDimensions": [
              {
                "dimension": "Events.timestamp",
                "granularity": "hour",
                dateRange: "today" // TODO
              }
            ],
            "filters": [
              {
                "dimension": "Stories.id",
                "operator": "equals",
                "values": [storyId]
              },
              {
                "dimension": "Events.page",
                "operator": "equals",
                "values": [
                  "front"
                ]
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

export default StoryPage;
