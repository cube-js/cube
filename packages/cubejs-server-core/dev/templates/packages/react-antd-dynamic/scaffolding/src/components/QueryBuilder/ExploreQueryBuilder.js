import React from 'react';
import * as PropTypes from 'prop-types';
import {
  Row, Col, Divider, Card
} from 'antd';
import { QueryBuilder } from '@cubejs-client/react';
import ChartRenderer from '../ChartRenderer';
import MemberGroup from './MemberGroup';
import FilterGroup from './FilterGroup';
import TimeGroup from './TimeGroup';
import SelectChartType from './SelectChartType';

const ExploreQueryBuilder = ({
  vizState, cubejsApi, setVizState, chartExtra
}) => (
  <QueryBuilder
    vizState={vizState}
    setVizState={setVizState}
    cubejsApi={cubejsApi}
    wrapWithQueryRenderer={false}
    render={({
      validatedQuery, isQueryPresent, chartType, updateChartType,
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
                <SelectChartType
                  chartType={chartType}
                  updateChartType={updateChartType}
                />
              </Col>
            </Row>
          </Card>
        </Col>
      </Row>,
      <Row type="flex" justify="space-around" align="top" gutter={24} key="2">
        <Col span={24}>
          {isQueryPresent ? (
            <Card
              style={{ minHeight: 420 }}
              extra={chartExtra}
            >
              <ChartRenderer
                vizState={{ query: validatedQuery, chartType }}
                cubejsApi={cubejsApi}
              />
            </Card>
          ) : <h2 style={{ textAlign: "center" }}>Choose a measure or dimension to get started</h2>}
        </Col>
      </Row>
    ]}
  />
);

ExploreQueryBuilder.propTypes = {
  vizState: PropTypes.object,
  setVizState: PropTypes.func,
  cubejsApi: PropTypes.object,
  chartExtra: PropTypes.array
};

ExploreQueryBuilder.defaultProps = {
  vizState: {},
  setVizState: null,
  cubejsApi: null,
  chartExtra: null
};

export default ExploreQueryBuilder;
