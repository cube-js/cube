import React from 'react';
import * as PropTypes from 'prop-types';
import {
  Row, Col, Button, Menu, Dropdown, Divider, Icon, Card, Select, Input
} from 'antd';
import { QueryBuilder } from '@cubejs-client/react';
import { ChartRenderer } from './ChartRenderer';
import { playgroundAction } from './events';

// Can't be a Pure Component due to Dropdown lookups overlay component type to set appropriate styles
const memberMenu = (onClick, availableMembers) => (
  <Menu>
    {availableMembers.length ? availableMembers.map(m => (
      <Menu.Item key={m.name} onClick={() => onClick(m)}>
        {m.title}
      </Menu.Item>
    )) : <Menu.Item disabled>No members found</Menu.Item>}
  </Menu>
);

const MemberGroup = ({
  members, availableMembers, addMemberName, updateMethods
}) => (
  <span>
    {members.map(m => (
      <Button.Group style={{ marginRight: 8 }} key={m.index || m.name}>
        <Dropdown
          overlay={
            memberMenu(updateWith => {
              playgroundAction('Update Member', { memberName: addMemberName });
              updateMethods.update(m, updateWith);
            }, availableMembers)
          }
          placement="bottomLeft"
          trigger={['click']}
        >
          <Button>{m.title}</Button>
        </Dropdown>
        <Button
          type="danger"
          icon="close"
          onClick={() => {
            playgroundAction('Remove Member', { memberName: addMemberName });
            updateMethods.remove(m);
          }}
        />
      </Button.Group>
    ))}
    <Dropdown
      overlay={
        memberMenu(m => {
          playgroundAction('Add Member', { memberName: addMemberName });
          updateMethods.add(m);
        }, availableMembers)
      }
      placement="bottomLeft"
      trigger={['click']}
    >
      <Button type="dashed" icon="plus">{addMemberName}</Button>
    </Dropdown>
  </span>
);

MemberGroup.propTypes = {
  members: PropTypes.array.isRequired,
  availableMembers: PropTypes.array.isRequired,
  addMemberName: PropTypes.string.isRequired,
  updateMethods: PropTypes.object.isRequired
};

const filterInputs = {
  string: ({ values, onChange }) => (
    <Select
      key="input"
      style={{ width: 300 }}
      mode="tags"
      onChange={onChange}
      value={values}
    />
  ),
  number: ({ values, onChange }) => (
    <Input
      key="input"
      style={{ width: 300 }}
      onChange={e => onChange([e.target.value])}
      value={values && values[0] || ''}
    />
  )
};

filterInputs.string.propTypes = {
  values: PropTypes.array,
  onChange: PropTypes.func.isRequired
};

filterInputs.string.defaultProps = {
  values: []
};

filterInputs.number.propTypes = {
  values: PropTypes.array,
  onChange: PropTypes.func.isRequired
};

filterInputs.number.defaultProps = {
  values: []
};

const FilterInput = ({ member, updateMethods, addMemberName }) => {
  const Filter = filterInputs[member.dimension.type] || filterInputs.string;
  return (
    <Filter
      key="filter"
      values={member.values}
      onChange={(values) => {
        playgroundAction('Update Filter Values', { memberName: addMemberName });
        updateMethods.update(member, { ...member, values });
      }}
    />
  );
};

FilterInput.propTypes = {
  member: PropTypes.object.isRequired,
  addMemberName: PropTypes.string.isRequired,
  updateMethods: PropTypes.object.isRequired
};

const FilterGroup = ({
  members, availableMembers, addMemberName, updateMethods
}) => (
  <span>
    {members.map(m => (
      <div style={{ marginBottom: 12 }} key={m.index}>
        <Button.Group style={{ marginRight: 8 }}>
          <Dropdown
            overlay={
              memberMenu(updateWith => {
                playgroundAction('Update Member', { memberName: addMemberName });
                updateMethods.update(m, { ...m, dimension: updateWith });
              }, availableMembers)
            }
            placement="bottomLeft"
            trigger={['click']}
          >
            <Button
              style={{
                width: 150,
                textOverflow: 'ellipsis',
                overflow: 'hidden'
              }}
            >
              {m.dimension.title}
            </Button>
          </Dropdown>
          <Button
            type="danger"
            icon="close"
            onClick={() => {
              playgroundAction('Remove Member', { memberName: addMemberName });
              updateMethods.remove(m);
            }}
          />
        </Button.Group>
        <Select
          value={m.operator}
          onChange={(operator) => updateMethods.update(m, { ...m, operator })}
          style={{ width: 200, marginRight: 8 }}
        >
          {m.operators.map(operator => (
            <Select.Option
              key={operator.name}
              value={operator.name}
            >
              {operator.title}
            </Select.Option>
          ))}
        </Select>
        <FilterInput member={m} key="filterInput" updateMethods={updateMethods} addMemberName={addMemberName}/>
      </div>
    ))}
    <Dropdown
      overlay={memberMenu(
        (m) => {
          playgroundAction('Add Member', { memberName: addMemberName });
          updateMethods.add({ dimension: m });
        },
        availableMembers
      )}
      placement="bottomLeft"
      trigger={['click']}
    >
      <Button type="dashed" icon="plus">{addMemberName}</Button>
    </Dropdown>
  </span>
);

FilterGroup.propTypes = {
  members: PropTypes.array.isRequired,
  availableMembers: PropTypes.array.isRequired,
  addMemberName: PropTypes.string.isRequired,
  updateMethods: PropTypes.object.isRequired
};

const TimeGroup = ({
  members, availableMembers, addMemberName, updateMethods
}) => {
  const granularityMenu = (member, onClick) => (
    <Menu>
      {member.granularities.length ? member.granularities.map(m => (
        <Menu.Item key={m.name} onClick={() => onClick(m)}>
          {m.title}
        </Menu.Item>
      )) : <Menu.Item disabled>No members found</Menu.Item>}
    </Menu>
  );

  const last30DaysFrom = new Date();
  last30DaysFrom.setDate(last30DaysFrom.getDate() - 31);

  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);

  const dateRanges = [
    { title: 'All time', value: undefined },
    { value: 'Today' },
    { value: 'Yesterday' },
    { value: 'This week' },
    { value: 'This month' },
    { value: 'This quarter' },
    { value: 'This year' },
    { value: 'Last 7 days' },
    { value: 'Last 30 days' },
    { value: 'Last week' },
    { value: 'Last month' },
    { value: 'Last quarter' },
    { value: 'Last year' }
  ];

  const dateRangeMenu = (onClick) => (
    <Menu>
      {dateRanges.map(m => (
        <Menu.Item key={m.title || m.value} onClick={() => onClick(m)}>
          {m.title || m.value}
        </Menu.Item>
      ))}
    </Menu>
  );

  return (
    <span>
      {members.map(m => [
        <Button.Group style={{ marginRight: 8 }}>
          <Dropdown
            overlay={memberMenu(updateWith => {
              playgroundAction('Update Member', { memberName: addMemberName });
              updateMethods.update(m, { ...m, dimension: updateWith });
            }, availableMembers)}
            placement="bottomLeft"
            trigger={['click']}
          >
            <Button>{m.dimension.title}</Button>
          </Dropdown>
          <Button
            type="danger"
            icon="close"
            onClick={() => {
              playgroundAction('Remove Member', { memberName: addMemberName });
              updateMethods.remove(m);
            }}
          />
        </Button.Group>,
        <b>FOR</b>,
        <Dropdown
          overlay={dateRangeMenu(
            dateRange => {
              playgroundAction('Update Date Range', { memberName: addMemberName });
              updateMethods.update(m, { ...m, dateRange: dateRange.value });
            }
          )}
          placement="bottomLeft"
          trigger={['click']}
        >
          <Button style={{ marginLeft: 8, marginRight: 8 }}>
            {m.dateRange || 'All time'}
          </Button>
        </Dropdown>,
        <b>BY</b>,
        <Dropdown
          overlay={granularityMenu(
            m.dimension,
            granularity => {
              playgroundAction('Update Granularity', { memberName: addMemberName });
              updateMethods.update(m, { ...m, granularity: granularity.name });
            }
          )}
          placement="bottomLeft"
          trigger={['click']}
        >
          <Button style={{ marginLeft: 8 }}>
            {
              m.dimension.granularities.find(g => g.name === m.granularity)
              && m.dimension.granularities.find(g => g.name === m.granularity).title
            }
          </Button>
        </Dropdown>
      ])}
      {!members.length && (
        <Dropdown
          overlay={memberMenu(member => {
            playgroundAction('Add Member', { memberName: addMemberName });
            updateMethods.add({ dimension: member, granularity: 'day' });
          }, availableMembers)}
          placement="bottomLeft"
          trigger={['click']}
        >
          <Button type="dashed" icon="plus">{addMemberName}</Button>
        </Dropdown>
      )}
    </span>
  );
};

TimeGroup.propTypes = {
  members: PropTypes.array.isRequired,
  availableMembers: PropTypes.array.isRequired,
  addMemberName: PropTypes.string.isRequired,
  updateMethods: PropTypes.object.isRequired
};

const ChartType = ({ chartType, updateChartType }) => {
  const chartTypes = [
    { name: 'line', title: 'Line', icon: 'line-chart' },
    { name: 'area', title: 'Area', icon: 'area-chart' },
    { name: 'bar', title: 'Bar', icon: 'bar-chart' },
    { name: 'pie', title: 'Pie', icon: 'pie-chart' },
    { name: 'table', title: 'Table', icon: 'table' },
    { name: 'number', title: 'Number', icon: 'info-circle' }
  ];

  const menu = (
    <Menu>
      {chartTypes.map(m => (
        <Menu.Item
          key={m.title}
          onClick={() => {
            playgroundAction('Change Chart Type');
            updateChartType(m.name);
          }}
        >
          <Icon type={m.icon} />
          {m.title}
        </Menu.Item>
      ))}
    </Menu>
  );

  const foundChartType = chartTypes.find(t => t.name === chartType);
  return (
    <Dropdown
      overlay={menu}
      placement="bottomLeft"
      trigger={['click']}
    >
      <Button icon={foundChartType.icon}>{foundChartType.title}</Button>
    </Dropdown>
  );
};

ChartType.propTypes = {
  chartType: PropTypes.string.isRequired,
  updateChartType: PropTypes.func.isRequired
};

const PlaygroundQueryBuilder = ({
  query, cubejsApi, apiUrl, cubejsToken, dashboardSource, setQuery
}) => (
  <QueryBuilder
    query={query}
    setQuery={setQuery}
    cubejsApi={cubejsApi}
    render={({
      resultSet, error, validatedQuery, isQueryPresent, chartType, updateChartType,
      measures, availableMeasures, updateMeasures,
      dimensions, availableDimensions, updateDimensions,
      segments, availableSegments, updateSegments,
      filters, updateFilters,
      timeDimensions, availableTimeDimensions, updateTimeDimensions
    }) => [
      <Row type="flex" justify="space-around" align="top" gutter={24} style={{ marginBottom: 12 }} key="1">
        <Col span={24}>
          <Card>
            <Row type="flex" justify="space-around" align="top" gutter={24} style={{ marginBottom: 12 }}>
              <Col span={24}>
                <MemberGroup
                  members={measures}
                  availableMembers={availableMeasures}
                  addMemberName="Measure"
                  updateMethods={updateMeasures}
                />
                <Divider type="vertical"/>
                <MemberGroup
                  members={dimensions}
                  availableMembers={availableDimensions}
                  addMemberName="Dimension"
                  updateMethods={updateDimensions}
                />
                <Divider type="vertical"/>
                <MemberGroup
                  members={segments}
                  availableMembers={availableSegments}
                  addMemberName="Segment"
                  updateMethods={updateSegments}
                />
                <Divider type="vertical"/>
                <TimeGroup
                  members={timeDimensions}
                  availableMembers={availableTimeDimensions}
                  addMemberName="Time"
                  updateMethods={updateTimeDimensions}
                />
              </Col>
            </Row>
            <Row type="flex" justify="space-around" align="top" gutter={24} style={{ marginBottom: 12 }}>
              <Col span={24}>
                <FilterGroup
                  members={filters}
                  availableMembers={availableDimensions.concat(availableMeasures)}
                  addMemberName="Filter"
                  updateMethods={updateFilters}
                />
              </Col>
            </Row>
            <Row type="flex" justify="space-around" align="top" gutter={24}>
              <Col span={24}>
                <ChartType chartType={chartType} updateChartType={updateChartType}/>
              </Col>
            </Row>
          </Card>
        </Col>
      </Row>,
      <Row type="flex" justify="space-around" align="top" gutter={24} key="2">
        <Col span={24}>
          {isQueryPresent ? (
            <ChartRenderer
              query={validatedQuery}
              resultSet={resultSet}
              error={error}
              apiUrl={apiUrl}
              cubejsToken={cubejsToken}
              chartType={chartType}
              cubejsApi={cubejsApi}
              dashboardSource={dashboardSource}
            />
          ) : <h2 style={{ textAlign: 'center' }}>Choose a measure or dimension to get started</h2>}
        </Col>
      </Row>
    ]}
  />
);

PlaygroundQueryBuilder.propTypes = {
  query: PropTypes.object,
  setQuery: PropTypes.func,
  cubejsApi: PropTypes.object,
  dashboardSource: PropTypes.object,
  apiUrl: PropTypes.string,
  cubejsToken: PropTypes.string
};

PlaygroundQueryBuilder.defaultProps = {
  query: {},
  setQuery: null,
  cubejsApi: null,
  dashboardSource: null,
  apiUrl: '/cubejs-api/v1',
  cubejsToken: null
};

export default PlaygroundQueryBuilder;
