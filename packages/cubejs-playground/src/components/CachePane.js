import { CheckOutlined, CloseOutlined } from '@ant-design/icons';
import { Table, Tabs } from 'antd';
import { QueryRenderer } from '@cubejs-client/react';
import sqlFormatter from 'sql-formatter';

import PrismCode from '../PrismCode';

const CachePane = ({ query }) => (
  <QueryRenderer
    loadSql
    query={{ ...query, renewQuery: true }}
    render={({ sqlQuery, resultSet: rs }) => (
      <Tabs
        defaultActiveKey="refreshKeys"
        tabBarExtraContent={
          <span>
            Last Refresh Time:&nbsp;
            <b>{rs && rs.loadResponse.lastRefreshTime}</b>
          </span>
        }
      >
        <Tabs.TabPane tab="Refresh Keys" key="refreshKeys">
          <Table
            loading={!sqlQuery}
            pagination={false}
            scroll={{ x: true }}
            columns={[
              {
                title: 'Refresh Key SQL',
                key: 'refreshKey',
                render: (text, record) => (
                  <PrismCode code={sqlFormatter.format(record[0])} />
                ),
              },
              {
                title: 'Value',
                key: 'value',
                render: (text, record) => (
                  <PrismCode
                    code={
                      rs &&
                      rs.loadResponse.refreshKeyValues &&
                      JSON.stringify(
                        rs.loadResponse.refreshKeyValues[
                          sqlQuery.sqlQuery.sql.cacheKeyQueries.queries.indexOf(
                            record
                          )
                        ],
                        null,
                        2
                      )
                    }
                  />
                ),
              },
            ]}
            dataSource={
              sqlQuery &&
              sqlQuery.sqlQuery.sql &&
              sqlQuery.sqlQuery.sql.cacheKeyQueries.queries
            }
          />
        </Tabs.TabPane>
        <Tabs.TabPane tab="Pre-aggregations" key="preAggregations">
          <Table
            loading={!sqlQuery}
            pagination={false}
            scroll={{ x: true }}
            columns={[
              {
                title: 'Table Name',
                key: 'tableName',
                dataIndex: 'tableName',
                render: (text) => <b>{text}</b>,
              },
              {
                title: 'Refresh Key SQL',
                key: 'refreshKey',
                dataIndex: 'invalidateKeyQueries',
                render: (refreshKeyQueries) =>
                  refreshKeyQueries.map((q) => (
                    <PrismCode key={q[0]} code={sqlFormatter.format(q[0])} />
                  )),
              },
              {
                title: 'Refresh Key Value',
                key: 'value',
                render: (text, record) =>
                  rs &&
                  rs.loadResponse.usedPreAggregations &&
                  rs.loadResponse.usedPreAggregations[record.tableName]
                    .refreshKeyValues &&
                  rs.loadResponse.usedPreAggregations[
                    record.tableName
                  ].refreshKeyValues.map((k) => (
                    <PrismCode
                      key={JSON.stringify(k)}
                      code={JSON.stringify(k, null, 2)}
                    />
                  )),
              },
            ]}
            dataSource={
              sqlQuery &&
              sqlQuery.sqlQuery.sql &&
              sqlQuery.sqlQuery.sql.preAggregations
            }
          />
        </Tabs.TabPane>
        <Tabs.TabPane tab="Rollup Match Results" key="rollupMatchResults">
          <Table
            loading={!sqlQuery}
            pagination={false}
            scroll={{ x: true }}
            columns={[
              {
                title: 'Rollup Name',
                key: 'name',
                dataIndex: 'name',
                render: (text) => <b>{text}</b>,
              },
              {
                title: 'Rollup Definition',
                key: 'rollup',
                dataIndex: 'references',
                render: (text) => (
                  <PrismCode code={JSON.stringify(text, null, 2)} />
                ),
              },
              {
                title: 'Can Be Used',
                key: 'canUsePreAggregation',
                dataIndex: 'canUsePreAggregation',
                render: (text) =>
                  text ? (
                    <CheckOutlined
                      style={{ color: '#52c41a', fontSize: '2em' }}
                    />
                  ) : (
                    <CloseOutlined
                      style={{ color: '#c2371b', fontSize: '2em' }}
                    />
                  ),
              },
            ]}
            dataSource={
              sqlQuery &&
              sqlQuery.sqlQuery.sql &&
              sqlQuery.sqlQuery.sql.rollupMatchResults
            }
          />
        </Tabs.TabPane>
      </Tabs>
    )}
  />
);

export default CachePane;
