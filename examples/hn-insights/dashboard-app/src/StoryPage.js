import React from "react";
import { Row, Col, Card, Spin, Statistic, Table, Layout, Icon } from "antd";
import "antd/dist/antd.css";
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
          <a
            href={`https://news.ycombinator.com/item?id=${data['Stories.id']}`}
            target="_blank" rel="noopener noreferrer"
          >
            {data['Stories.title']}
          </a>
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
      <StoryCardMeta
        span={12}
        title="Score"
        description={<span>{data['Events.currentScore']}</span>}
      />
      <StoryCardMeta
        span={12}
        title="Comments"
        description={<span>{data['Events.currentComments']}</span>}
      />
      <StoryCardMeta
        span={12}
        title="Rank"
        description={<span>{data['Events.currentRank']}</span>}
      />
      <StoryCardMeta
        span={12}
        title="User"
        description={<a
          href={`https://news.ycombinator.com/user?id=${data['Stories.user']}`}
          target="_blank" rel="noopener noreferrer"
        >
          {data['Stories.user']}
        </a>}
      />
    </Row>
  );
};

const renderStatisticCard = (currentField, prevField, isRank) => ({ resultSet }) => {
  const scoreLastHour = resultSet.totalRow()[currentField] && parseInt(resultSet.totalRow()[currentField], 10);
  const scorePrevHour = resultSet.totalRow()[prevField] && parseInt(resultSet.totalRow()[prevField], 10) || null;
  const positiveDiff = isRank ? scoreLastHour <= scorePrevHour : scoreLastHour >= scorePrevHour;
  const prefix = isRank ? '' : '+';
  return <div style={{ textAlign: 'center' }}><Statistic
    value={`${prefix}${scoreLastHour}`}
    valueStyle={{ color: scorePrevHour && (positiveDiff ? '#3f8600' : '#cf1322') }}
    prefix={scorePrevHour && <Icon
      type={positiveDiff ? 'arrow-up' : 'arrow-down'}/>}
    suffix={scorePrevHour && <span>&nbsp;{`/ ${prefix}${scorePrevHour}`}</span>}
  /></div>
};

const renderChart = Component => ({ resultSet, error }) =>
  (resultSet && <Component resultSet={resultSet} />) ||
  (error && error.toString()) || <Spin />;

const StoryPage = ({ match: { params: { storyId } }, cubejsApi }) => {
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
              "Events.currentRank",
              "Events.currentScore",
              "Events.currentComments"
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
              "Stories.user",
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
      <Col span={12}>
        <Dashboard>
          <DashboardItem size={12} title="Points last/prev hour">
            <QueryRenderer
              query={{
                measures: [
                  "Events.scoreChangeLastHour",
                  "Events.scoreChangePrevHour"
                ],
                timeDimensions: [
                  {
                    dimension: "Events.timestamp",
                    dateRange: "from 7 days ago to now"
                  }
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
              render={renderChart(renderStatisticCard("Events.scoreChangeLastHour", "Events.scoreChangePrevHour"))}
            />
          </DashboardItem>
          <DashboardItem size={12} title="Comments last/prev hour">
            <QueryRenderer
              query={{
                measures: [
                  "Events.commentsChangeLastHour",
                  "Events.commentsChangePrevHour"
                ],
                timeDimensions: [
                  {
                    dimension: "Events.timestamp",
                    dateRange: "from 7 days ago to now"
                  }
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
              render={renderChart(renderStatisticCard("Events.commentsChangeLastHour", "Events.commentsChangePrevHour"))}
            />
          </DashboardItem>
          <DashboardItem size={12} title="Rank current/hour ago">
            <QueryRenderer
              query={{
                measures: [
                  "Events.currentRank",
                  "Events.rankHourAgo"
                ],
                timeDimensions: [
                  {
                    dimension: "Events.timestamp",
                    dateRange: "from 7 days ago to now"
                  }
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
              render={renderChart(renderStatisticCard("Events.currentRank", "Events.rankHourAgo", true))}
            />
          </DashboardItem>
          <DashboardItem size={12} title="Comments last/prev hour">
            <QueryRenderer
              query={{
                measures: [
                  "Events.commentsChangeLastHour",
                  "Events.commentsChangePrevHour"
                ],
                timeDimensions: [
                  {
                    dimension: "Events.timestamp",
                    dateRange: "from 7 days ago to now"
                  }
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
              render={renderChart(renderStatisticCard("Events.commentsChangeLastHour", "Events.commentsChangePrevHour"))}
            />
          </DashboardItem>
        </Dashboard>
      </Col>
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
