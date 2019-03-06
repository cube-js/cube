import React, { Component } from 'react';
import { Row, Col, Button, Menu, Dropdown, Divider, Icon } from "antd";
import { QueryBuilder } from '@cubejs-client/react';
import { fetch } from 'whatwg-fetch';
import ChartRenderer from './ChartRenderer';

const renderMemberGroup = (members, availableMembers, addMemberName, updateMethods) => {
  const menu = (onClick) => (
    <Menu>
      {availableMembers.length ? availableMembers.map(m => (<Menu.Item key={m.name} onClick={() => onClick(m)}>
        {m.title}
      </Menu.Item>)) : <Menu.Item disabled>No members found</Menu.Item>}
    </Menu>
  );

  return (<span>
      {members.map(m => (
        <Button.Group style={{ marginRight: 8 }}>
          <Dropdown overlay={menu(updateWith => updateMethods.update(m, updateWith))} placement="bottomLeft" trigger={['click']}>
            <Button>{m.title}</Button>
          </Dropdown>
          <Button type="danger" icon='close' onClick={() => updateMethods.remove(m)} />
        </Button.Group>
      ))}
    <Dropdown overlay={menu(updateMethods.add)} placement="bottomLeft" trigger={['click']}>
        <Button type="dashed" icon='plus'>{addMemberName}</Button>
      </Dropdown>
    </span>);
};

const renderTimeGroup = (members, availableMembers, addMemberName, updateMethods) => {
  const menu = (onClick) => (
    <Menu>
      {availableMembers.length ? availableMembers.map(m => (<Menu.Item key={m.name} onClick={() => onClick(m)}>
        {m.title}
      </Menu.Item>)) : <Menu.Item disabled>No members found</Menu.Item>}
    </Menu>
  );

  const granularityMenu = (member, onClick) => (
    <Menu>
      {member.granularities.length ? member.granularities.map(m => (
          <Menu.Item key={m.name} onClick={() => onClick(m)}>
            {m.title}
          </Menu.Item>
        )
      ) : <Menu.Item disabled>No members found</Menu.Item>}
    </Menu>
  );

  const last30DaysFrom = new Date();
  last30DaysFrom.setDate(last30DaysFrom.getDate() - 31);

  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);

  const dateRanges = [
    { title: 'All time', value: undefined },
    { title: 'Last 30 days', value: [
        last30DaysFrom.toISOString().substring(0, 10), yesterday.toISOString().substring(0, 10)
      ] }
  ];

  const dateRangeMenu = (onClick) => (
    <Menu>
      {dateRanges.map(m => (
          <Menu.Item key={m.title} onClick={() => onClick(m)}>
            {m.title}
          </Menu.Item>
        )
      )}
    </Menu>
  );

  return (<span>
      {members.map(m => [
        <Button.Group style={{ marginRight: 8 }}>
          <Dropdown
            overlay={menu(updateWith => updateMethods.update(m, { ...m, dimension: updateWith }))}
            placement="bottomLeft"
            trigger={['click']}
          >
            <Button>{m.dimension.title}</Button>
          </Dropdown>
          <Button type="danger" icon='close' onClick={() => updateMethods.remove(m)} />
        </Button.Group>,
        <b>FOR</b>,
        <Dropdown
          overlay={dateRangeMenu(
            dateRange => updateMethods.update(m, { ...m, dateRange: dateRange.value })
          )}
          placement="bottomLeft"
          trigger={['click']}
        >
          <Button style={{ marginLeft: 8, marginRight: 8 }}>{
            m.dateRange && m.dateRange.join(' - ') || 'All time'
          }</Button>
        </Dropdown>,
        <b>BY</b>,
        <Dropdown
          overlay={granularityMenu(
            m.dimension,
            granularity => updateMethods.update(m, { ...m, granularity: granularity.name })
          )}
          placement="bottomLeft"
          trigger={['click']}
        >
          <Button style={{ marginLeft: 8 }}>{
            m.dimension.granularities.find(g => g.name === m.granularity) &&
            m.dimension.granularities.find(g => g.name === m.granularity).title
          }</Button>
        </Dropdown>
      ])}
    {!members.length && <Dropdown
      overlay={menu(member => updateMethods.add({ dimension: member, granularity: 'day' }))}
      placement="bottomLeft"
      trigger={['click']}
    >
      <Button type="dashed" icon='plus'>{addMemberName}</Button>
    </Dropdown>}
    </span>);
};

const renderChartType = (chartType, updateChartType) => {
  const chartTypes = [
    { name: 'line', title: 'Line', icon: 'line-chart' },
    { name: 'bar', title: 'Bar', icon: 'bar-chart' },
    { name: 'pie', title: 'Pie', icon: 'pie-chart' }
  ];

  const menu = <Menu>
    {chartTypes.map(m => (
        <Menu.Item key={m.title} onClick={() => updateChartType(m.name)}>
          <Icon type={m.icon} />
          {m.title}
        </Menu.Item>
      )
    )}
  </Menu>;

  let foundChartType = chartTypes.find(t => t.name === chartType);
  return <Dropdown
    overlay={menu}
    placement="bottomLeft"
    trigger={['click']}
  >
    <Button icon={foundChartType.icon}>{foundChartType.title}</Button>
  </Dropdown>
};

export default ({ query, cubejsApi, apiUrl, cubejsToken }) => {
  return (<QueryBuilder
    query={query}
    cubejsApi={cubejsApi}
    render={({
               resultSet, error, query, chartType, updateChartType,
               measures, availableMeasures, updateMeasures,
               dimensions, availableDimensions, updateDimensions,
               segments, availableSegments, updateSegments,
               timeDimensions, availableTimeDimensions, updateTimeDimensions
             }) => [
      <Row type="flex" justify="space-around" align="top" gutter={24} style={{ marginBottom: 12 }}>
        <Col span={24}>
          {renderMemberGroup(measures, availableMeasures, 'Measure', updateMeasures)}
          <Divider type="vertical" />
          {renderMemberGroup(dimensions, availableDimensions, 'Dimension', updateDimensions)}
          <Divider type="vertical" />
          {renderMemberGroup(segments, availableSegments, 'Segment', updateSegments)}
          <Divider type="vertical" />
          {renderTimeGroup(timeDimensions, availableTimeDimensions, 'Time', updateTimeDimensions)}
        </Col>
      </Row>,
      <Row type="flex" justify="space-around" align="top" gutter={24} style={{ marginBottom: 12 }}>
        <Col span={24}>
          {renderChartType(chartType, updateChartType)}
        </Col>
      </Row>,
      <Row type="flex" justify="space-around" align="top" gutter={24}>
        <Col span={24}>
          <ChartRenderer
            query={query}
            resultSet={resultSet}
            error={error}
            title="Chart"
            apiUrl={apiUrl}
            cubejsToken={cubejsToken}
            chartType={chartType}
            chartLibrary="bizcharts"
          />
        </Col>
      </Row>
    ]}
  />)
}