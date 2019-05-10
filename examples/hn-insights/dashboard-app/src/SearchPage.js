import React, { useState } from "react";
import { Link } from 'react-router-dom'
import { Row, Col, Card, Spin, Statistic, Table, Icon, Input } from "antd";
import "antd/dist/antd.css";
import { QueryRenderer } from "@cubejs-client/react";

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

const velocityListRender = ({ resultSet }) => {
  const rows = resultSet.tablePivot();

  if (!rows.length) {
    return (
      <h2 style={{ textAlign: 'center' }}>
        Nothing found. We either lack this history data or your story wasn't fetched yet
      </h2>
    );
  }

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
        value={scoreLastHour && `+${scoreLastHour}`}
        valueStyle={{ color: scorePrevHour && (scoreLastHour >= scorePrevHour ? '#3f8600' : '#cf1322') }}
        prefix={scorePrevHour && <Icon
          type={scoreLastHour >= scorePrevHour ? 'arrow-up' : 'arrow-down'}/>}
        suffix={scorePrevHour && `+${scorePrevHour}`}
      />
    },
  }];

  return (
    <Table dataSource={rows} columns={columns} pagination={false} />
  );
};

const renderChart = Component => ({ resultSet, error }) =>
  (resultSet && <Component resultSet={resultSet} />) ||
  (error && error.toString()) || <Spin />;

const IndexPage = ({ cubejsApi }) => {
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
                "Events.currentRank"
              ],
              dimensions: [
                "Stories.id",
                "Stories.title"
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
                "Events.scoreChangeLastHour": 'desc'
              },
              limit: 20
            }}
            cubejsApi={cubejsApi}
            render={renderChart(velocityListRender)}
          />
        </DashboardItem>
      </Dashboard> : <h2 style={{ textAlign: 'center' }}>Search story to track its performance</h2>}
    </div>
  );
};

export default IndexPage;
