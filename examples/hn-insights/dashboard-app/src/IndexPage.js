import React, { useState } from "react";
import { Link } from 'react-router-dom'
import { Row, Col, Card, Spin, Statistic, Table, Layout, List, Icon, Input } from "antd";
import "antd/dist/antd.css";
import "./index.css";
import { QueryRenderer } from "@cubejs-client/react";
import moment from "moment";

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

const searchListRender = ({ resultSet }) => {
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
    title: 'Score',
    key: 'score',
    render: (text, item) => {
      return <Statistic
        value={item['Stories.currentScore']}
      />
    },
  }, {
    title: 'Top Rank on Front Page',
    key: 'topRank',
    render: (text, item) => {
      return <Statistic
        value={item["Events.topRank"] || 'N/A'}
      />
    },
  }, {
    title: 'Posted Time',
    key: 'postedTime',
    render: (text, item) => {
      return item['Stories.postedTime'] && moment(item['Stories.postedTime']).format('LLL')
    },
  }];

  return (
    <Table dataSource={resultSet.tablePivot()} columns={columns} pagination={false} />
  );
};

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
    },
  }];

  return (
    <Table dataSource={resultSet.tablePivot()} columns={columns} pagination={false} />
  );
};

const renderChart = Component => ({ resultSet, error }) =>
  (resultSet && <Component resultSet={resultSet} />) ||
  (error && error.toString()) || <Spin />;

const IndexPage = ({ cubejsApi }) => {
  const [search, setSearch] = useState('');
  const [searchInputValue, setSearchInputValue] = useState('');
  const recentSearchKey = 'RECENT_SEARCH';
  const [recentSearch, setRecentSearch] = useState(
    window &&
    window.localStorage &&
    window.localStorage.getItem(recentSearchKey) &&
    JSON.parse(window.localStorage.getItem(recentSearchKey)) ||
    []
  );

  const onSearch = (searchInput) => {
    setSearch(searchInput);
    setSearchInputValue(searchInput);
    if (searchInput && searchInput.trim() && recentSearch.indexOf(searchInput) === -1) {
      const newRecentSearch = recentSearch.concat(searchInput);
      setRecentSearch(newRecentSearch);
      if (window.localStorage) {
        window.localStorage.setItem(recentSearchKey, JSON.stringify(newRecentSearch));
      }
    }
  };

  const leaderBoard = (
    <Dashboard>
      <DashboardItem size={12} title="Recent Searches">
        <Table dataSource={recentSearch.concat([]).reverse()} columns={[
          {
            title: 'Search',
            key: 'Search',
            render: (text, item) => {
              return <a onClick={() => onSearch(item)}>{item}</a>
            },
          }
        ]} />
      </DashboardItem>
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
    </Dashboard>
  );

  const idMatch = search.match(/\?id=(\d+)|^(\d+)$/);

  return (
    <div>
      <h1 style={{ textAlign: 'center' }}>Track HN Stories in Real Time</h1>
      <Input.Search
        value={searchInputValue}
        onChange={(e) => setSearchInputValue(e.target.value)}
        placeholder="Story id, url or title"
        onSearch={onSearch}
        size="large"
        enterButton
        style={{ marginBottom: 16 }}
      />
      {search ? <Dashboard>
        <DashboardItem size={24} title="Search Results">
          <QueryRenderer
            query={{
              measures: [
                "Events.topRank",
              ],
              dimensions: [
                "Stories.id",
                "Stories.title",
                "Stories.currentRank",
                "Stories.postedTime",
                "Stories.currentScore"
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
            render={renderChart(searchListRender)}
          />
        </DashboardItem>
      </Dashboard> : <div><h3 style={{ textAlign: 'center', marginBottom: 100 }}>Search for your story to see how it's performing and standing against competing posts</h3>{leaderBoard}</div>}
    </div>
  );
};

export default IndexPage;
