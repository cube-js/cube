import React from "react";
import { Row, Col, Card, Spin, Statistic, Table, Layout, Icon, Popover, Tabs } from "antd";
import { Link } from 'react-router-dom'
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

const velocityListRender = (storyResult) => ({ resultSet }) => {
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
    title: 'Points Added Last/Prev Hour',
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
    title: 'Rank Score',
    key: 'currentRankPoints',
    render: (text, item) => {
      const score = item["Stories.currentRankScore"] && Math.round(item["Stories.currentRankScore"] * 1000);
      return <Statistic
        value={score || 'N/A'}
      />
    }
  }, {
    title: 'Score Required for this Place',
    key: 'currentRankPoints',
    render: (text, item) => {
      const score = item["Stories.currentRankScore"];
      const storyRow = storyResult.resultSet.tablePivot()[0];
      const scoreToGet = Math.round(Math.pow(score * Math.pow(storyRow['Stories.ageInHours'] + 2, 1.8), 1 / 0.8));
      return <Statistic
        value={scoreToGet || 'N/A'}
      />
    }
  }];

  return (
    <Table dataSource={resultSet.tablePivot()} columns={columns} pagination={false} />
  );
};

const lineRender = ({ resultSet }) => (
  <Chart scale={{ x: { tickCount: 8 } }} height={400} data={stackedChartData(resultSet)} forceFit>
    <Axis name="x" />
    <Axis name="measure" />
    <Tooltip crosshairs={{type : 'y'}} />
    <Geom type="line" position={`x*measure`} size={2} color="color" />
  </Chart>
);

const StoryCardMeta = ({ span, title, description }) => (
  <Col lg={span} style={{ marginBottom: 16 }} span={24}>
    <Card.Meta
      title={title}
      description={description}
    />
  </Col>
);

const storyCardRender = ({ resultSet }) => {
  const data = resultSet.tablePivot()[0];

  if (!data) {
    return <h2>Not found</h2>
  }

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
        description={<span>{data['Events.topRank'] || 'N/A'}</span>}
      />
      <StoryCardMeta
        span={12}
        title="Posted Time"
        description={<span>{data['Stories.postedTime'] && moment(data['Stories.postedTime']).format('LLL')}</span>}
      />
      <StoryCardMeta
        span={12}
        title="Added to Front Page"
        description={<span>{data['Stories.addedToFrontPage'] && moment(data['Stories.addedToFrontPage']).format('LLL') || 'N/A'}</span>}
      />
      <StoryCardMeta
        span={12}
        title="Minutes to Front Page"
        description={<span>{data['Stories.minutesToFrontPage'] || 'N/A'}</span>}
      />
      <StoryCardMeta
        span={12}
        title="Points Before Front Page"
        description={<span>{data['Events.scoreChangeBeforeAddedToFrontPage']}</span>}
      />
      <StoryCardMeta
        span={12}
        title="Karma Added Before Front Page"
        description={<span>{data['Events.karmaChangeBeforeAddedToFrontPage']}</span>}
      />
      <StoryCardMeta
        span={12}
        title="Comments Before Front Page"
        description={<span>{data['Events.commentsBeforeAddedToFrontPage']}</span>}
      />
      <StoryCardMeta
        span={12}
        title="Minutes on First Page"
        description={<span>{data['Events.minutesOnFirstPage'] && parseInt(data['Events.minutesOnFirstPage'], 10) || 'N/A'}</span>}
      />
      <StoryCardMeta
        span={12}
        title="Score"
        description={<span>{data['Stories.currentScore']}</span>}
      />
      <StoryCardMeta
        span={12}
        title="Comments"
        description={<span>{data['Stories.currentComments']}</span>}
      />
      <StoryCardMeta
        span={12}
        title="Age"
        description={<span>{data['Stories.ageInHours'] && Math.round(data['Stories.ageInHours'] * 10) / 10 || 'N/A'} hours</span>}
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
  const totalRow = resultSet.tablePivot()[0];
  const scoreLastHour = totalRow[currentField] && parseInt(totalRow[currentField], 10);
  const scorePrevHour = totalRow[prevField] && parseInt(totalRow[prevField], 10) || null;
  const positiveDiff = isRank ? scoreLastHour <= scorePrevHour : scoreLastHour >= scorePrevHour;
  const prefix = isRank ? '' : '+';
  return <div style={{ textAlign: 'center' }}><Statistic
    value={scoreLastHour && `${prefix}${scoreLastHour}` || 'N/A'}
    valueStyle={{ color: scorePrevHour && (positiveDiff ? '#3f8600' : '#cf1322') }}
    prefix={scorePrevHour && <Icon
      type={positiveDiff ? 'arrow-up' : 'arrow-down'}/>}
    suffix={scorePrevHour && <span>&nbsp;{`/ ${prefix}${scorePrevHour}`}</span>}
  /></div>
};

const renderScoreCard = (currentField, prevField, isRank) => ({ resultSet }) => {
  const totalRow = resultSet.tablePivot()[0];
  const scoreLastHour = totalRow[currentField] && Math.round(totalRow[currentField] * 1000);
  const scorePrevHour = totalRow[prevField] && Math.round(totalRow[prevField] * 1000);
  const positiveDiff = isRank ? scoreLastHour <= scorePrevHour : scoreLastHour >= scorePrevHour;
  return <div style={{ textAlign: 'center' }}><Statistic
    value={scoreLastHour || 'N/A'}
    valueStyle={{ color: scorePrevHour && (positiveDiff ? '#3f8600' : '#cf1322') }}
    prefix={scorePrevHour && <Icon
      type={positiveDiff ? 'arrow-up' : 'arrow-down'}/>}
    suffix={scorePrevHour && <span>&nbsp;{`/ ${scorePrevHour}`}</span>}
  /></div>
};

const renderChart = Component => ({ resultSet, error }) =>
  (resultSet && <Component resultSet={resultSet} />) ||
  (error && error.toString()) || <Spin />;

const StoryPage = ({ match: { params: { storyId } }, cubejsApi }) => {
  let propQuery = {
    measures: [
      "Events.scoreChangeBeforeAddedToFrontPage",
      "Events.karmaChangeBeforeAddedToFrontPage",
      "Events.commentsBeforeAddedToFrontPage",
      "Events.minutesOnFirstPage",
      "Events.topRank",
      "Events.scoreChangeLastHour",
      "Events.scoreChangePrevHour",
      "Events.commentsChangeLastHour",
      "Events.commentsChangePrevHour",
      "Events.rankHourAgo",
      "Events.rankScoreHourAgo",
      "Events.karmaChangeLastHour",
      "Events.karmaChangePrevHour",
      "Events.scoreEstimateLastHour"
    ],
    dimensions: [
      "Stories.id",
      "Stories.title",
      "Stories.href",
      "Stories.user",
      "Stories.postedTime",
      "Stories.lastEventTime",
      "Stories.addedToFrontPage",
      "Stories.minutesToFrontPage",
      "Stories.ageInHours",
      "Stories.currentRank",
      "Stories.currentRankScore",
      "Stories.currentScore",
      "Stories.currentComments"
    ],
    "filters": [
      {
        "dimension": "Stories.id",
        "operator": "equals",
        "values": [storyId]
      }
    ]
  };

  const historyDashboard = (propRes) => {
    const chartQuery = (query) => ({
      "timeDimensions": [
        {
          "dimension": "Events.timestamp",
          "granularity": "hour",
          dateRange: [
            propRes.resultSet.tablePivot()[0]['Stories.postedTime'],
            propRes.resultSet.tablePivot()[0]['Stories.lastEventTime'],
          ]
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
      ],
      ...query
    });

    return <Dashboard>
      <DashboardItem size={12} title="Points per Hour" key="1">
        <QueryRenderer
          query={chartQuery({
            "measures": [
              "Events.scoreChange",
              "Events.averageScoreEstimate"
            ]
          })}
          cubejsApi={cubejsApi}
          render={renderChart(lineRender)}
        />
      </DashboardItem>
      <DashboardItem size={12} title="Penalty" key="penalty">
        <QueryRenderer
          query={chartQuery({
            "measures": [
              "Events.topPenalty"
            ]
          })}
          cubejsApi={cubejsApi}
          render={renderChart(lineRender)}
        />
      </DashboardItem>
      <DashboardItem size={12} title="Average Rank Score" key="penalty">
        <QueryRenderer
          query={chartQuery({
            "measures": [
              "Events.avgRankScore"
            ]
          })}
          cubejsApi={cubejsApi}
          render={renderChart(lineRender)}
        />
      </DashboardItem>
      <DashboardItem size={12} title="Rank" key="4">
        <QueryRenderer
          query={chartQuery({
            "measures": [
              "Events.avgRank"
            ]
          })}
          cubejsApi={cubejsApi}
          render={renderChart(lineRender)}
        />
      </DashboardItem>
      <DashboardItem size={12} title="Karma per Hour" key="2">
        <QueryRenderer
          query={chartQuery({
            "measures": [
              "Events.karmaChange"
            ]
          })}
          cubejsApi={cubejsApi}
          render={renderChart(lineRender)}
        />
      </DashboardItem>
      <DashboardItem size={12} title="Comments per Hour" key="3">
        <QueryRenderer
          query={chartQuery({
            "measures": [
              "Events.commentsChange"
            ]
          })}
          cubejsApi={cubejsApi}
          render={renderChart(lineRender)}
        />
      </DashboardItem>
    </Dashboard>
  };

  const pageDashboard = (propRes) => ([
    <Dashboard>
      <DashboardItem size={12} title="Story">
        {renderChart(storyCardRender)(propRes)}
      </DashboardItem>
      <Col span={24} lg={12}>
        <Dashboard>
          <DashboardItem size={12} title="Points Added Last/Prev Hour">
            {renderChart(renderStatisticCard("Events.scoreChangeLastHour", "Events.scoreChangePrevHour"))(propRes)}
          </DashboardItem>
          <DashboardItem size={12} title={
            <span>
              Rank Score&nbsp;
              <Popover content={
                <span>
                  Calculated as<br />
                  <img src="http://static.righto.com/images/rank2.gif"/><br/>
                  Result is multiplied by 1000.<br/>
                  <a
                    href="http://www.righto.com/2013/11/how-hacker-news-ranking-really-works.html"
                    target="_blank" rel="noopener noreferrer"
                  >Learn more</a>.
                </span>
              }>
                <Icon type="info-circle" />
              </Popover>
            </span>
          }>
            {renderChart(renderScoreCard("Stories.currentRankScore", "Events.rankScoreHourAgo"))(propRes)}
          </DashboardItem>
          <DashboardItem size={12} title="Comments Added Last/Prev Hour">
            {renderChart(renderStatisticCard("Events.commentsChangeLastHour", "Events.commentsChangePrevHour"))(propRes)}
          </DashboardItem>
          <DashboardItem size={12} title="Rank Current/Hour Ago">
            {renderChart(renderStatisticCard("Stories.currentRank", "Events.rankHourAgo", true))(propRes)}
          </DashboardItem>
          <DashboardItem size={12} title="Karma Last/Prev Hour">
            {renderChart(renderStatisticCard("Events.karmaChangeLastHour", "Events.karmaChangePrevHour"))(propRes)}
          </DashboardItem>
          <DashboardItem size={12} title={
            <span>
              Points Last Hour / Average&nbsp;
              <Popover content="Points added last hour vs average performance estimate based on rank of this story">
                <Icon type="info-circle" />
              </Popover>
            </span>
          }>
            {renderChart(renderStatisticCard("Events.scoreChangeLastHour", "Events.scoreEstimateLastHour"))(propRes)}
          </DashboardItem>
        </Dashboard>
      </Col>
    </Dashboard>,
    propRes.resultSet && propRes.resultSet.tablePivot()[0] && (
      <Tabs defaultActiveKey="competition">
        <Tabs.TabPane tab="Competition" key="competition">
          <Dashboard>
            <DashboardItem size={24} title="Top Stories">
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
                  filters: propRes.resultSet.tablePivot()[0]['Stories.currentRank'] ? [{
                    dimension: "Stories.currentRank",
                    operator: 'lt',
                    values: [`${propRes.resultSet.tablePivot()[0]['Stories.currentRank']}`]
                  }] : [{
                    dimension: "Stories.currentRank",
                    operator: 'set'
                  }, {
                    dimension: "Stories.currentRankScore",
                    operator: 'gt',
                    values: [`${propRes.resultSet.tablePivot()[0]['Stories.currentRankScore']}`]
                  }],
                  limit: 20
                }}
                cubejsApi={cubejsApi}
                render={renderChart(velocityListRender(propRes))}
              />
            </DashboardItem>
          </Dashboard>
        </Tabs.TabPane>
        <Tabs.TabPane tab="History" key="history">
          {historyDashboard(propRes)}
        </Tabs.TabPane>
      </Tabs>
    ) || null
  ]);

  return (
    <QueryRenderer
      query={propQuery}
      cubejsApi={cubejsApi}
      render={pageDashboard}
    />
  )
};

export default StoryPage;
