import React from 'react';
import * as PropTypes from 'prop-types';
import { Col, Row } from 'antd';
import { QueryBuilder } from '@cubejs-client/react';
import { ChartRenderer } from './ChartRenderer';
import { playgroundAction } from './events';
import MemberGroup from './QueryBuilder/MemberGroup';
import FilterGroup from './QueryBuilder/FilterGroup';
import TimeGroup from './QueryBuilder/TimeGroup';
import SelectChartType from './QueryBuilder/SelectChartType';
import Settings from './components/Settings/Settings';
import { Card, SectionHeader, SectionRow } from './components';
import styled from 'styled-components';

const Section = styled.div`
  display: flex;
  flex-flow: column;
  margin-right: 24px;
  margin-bottom: 16px;
  
  > *:first-child {
    margin-bottom: 8px;
  }
`;

const playgroundActionUpdateMethods = (updateMethods, memberName) =>
  Object.keys(updateMethods)
    .map((method) => ({
      [method]: (member, values, ...rest) => {
        let actionName = `${method
          .split('')
          .map((c, i) => (i === 0 ? c.toUpperCase() : c))
          .join('')} Member`;
        if (values && values.values) {
          actionName = 'Update Filter Values';
        }
        if (values && values.dateRange) {
          actionName = 'Update Date Range';
        }
        if (values && values.granularity) {
          actionName = 'Update Granularity';
        }
        playgroundAction(actionName, { memberName });
        return updateMethods[method].apply(null, [member, values, ...rest]);
      },
    }))
    .reduce((a, b) => ({ ...a, ...b }), {});

export default function PlaygroundQueryBuilder({
                                                 query,
                                                 cubejsApi,
                                                 apiUrl,
                                                 cubejsToken,
                                                 dashboardSource,
                                                 setQuery,
                                               }) {
  return (
    <QueryBuilder
      query={query}
      setQuery={setQuery}
      cubejsApi={cubejsApi}
      render={({
                 resultSet,
                 error,
                 validatedQuery,
                 isQueryPresent,
                 chartType,
                 updateChartType,
                 measures,
                 availableMeasures,
                 updateMeasures,
                 dimensions,
                 availableDimensions,
                 updateDimensions,
                 segments,
                 availableSegments,
                 updateSegments,
                 filters,
                 updateFilters,
                 timeDimensions,
                 availableTimeDimensions,
                 updateTimeDimensions,
                 orderMembers,
                 updateOrder,
                 pivotConfig,
                 updatePivotConfig,
               }) => {
        let parsedDateRange = null;

        if (resultSet) {
          const { timeDimensions = [] } =
          resultSet.pivotQuery() || resultSet.query() || {};
          parsedDateRange = timeDimensions[0]?.dateRange;
        }

        return (
          <>
            <Row
              justify="space-around"
              align="top"
              gutter={24}
              style={{ marginBottom: 12 }}
            >
              <Col span={24}>
                <Card bordered={false} style={{ borderRadius: 0 }}>
                  <Row
                    justify="stretch"
                    align="top"
                    gutter={0}
                    style={{ marginBottom: -12 }}
                  >
                    <Section>
                      <SectionHeader>
                        Measures
                      </SectionHeader>
                      <MemberGroup
                        members={measures}
                        availableMembers={availableMeasures}
                        addMemberName="Measure"
                        updateMethods={playgroundActionUpdateMethods(
                          updateMeasures,
                          'Measure',
                        )}
                      />
                    </Section>
                    <Section>
                      <SectionHeader>
                        Dimensions
                      </SectionHeader>
                      <MemberGroup
                        members={dimensions}
                        availableMembers={availableDimensions}
                        addMemberName="Dimension"
                        updateMethods={playgroundActionUpdateMethods(
                          updateDimensions,
                          'Dimension',
                        )}
                      />
                    </Section>
                    <Section>
                      <SectionHeader>
                        Segment
                      </SectionHeader>
                      <MemberGroup
                        members={segments}
                        availableMembers={availableSegments}
                        addMemberName="Segment"
                        updateMethods={playgroundActionUpdateMethods(
                          updateSegments,
                          'Segment',
                        )}
                      />
                    </Section>
                    <Section>
                      <SectionHeader>
                        Time
                      </SectionHeader>
                      <TimeGroup
                        members={timeDimensions}
                        availableMembers={availableTimeDimensions}
                        addMemberName="Time"
                        updateMethods={playgroundActionUpdateMethods(
                          updateTimeDimensions,
                          'Time',
                        )}
                        parsedDateRange={parsedDateRange}
                      />
                    </Section>

                    <Section>
                      <SectionHeader>
                        Filters
                      </SectionHeader>
                      <FilterGroup
                        members={filters}
                        availableMembers={availableDimensions.concat(
                          availableMeasures,
                        )}
                        addMemberName="Filter"
                        updateMethods={playgroundActionUpdateMethods(
                          updateFilters,
                          'Filter',
                        )}
                      />
                    </Section>
                  </Row>
                </Card>

                <SectionRow style={{ marginTop: 16, marginLeft: 16 }}>
                  <SelectChartType
                    chartType={chartType}
                    updateChartType={(type) => {
                      playgroundAction('Change Chart Type');
                      updateChartType(type);
                    }}
                  />

                  <Settings
                    isQueryPresent={isQueryPresent}
                    limit={query.limit}
                    pivotConfig={pivotConfig}
                    orderMembers={orderMembers}
                    onReorder={updateOrder.reorder}
                    onOrderChange={updateOrder.set}
                    onMove={updatePivotConfig.moveItem}
                    onUpdate={updatePivotConfig.update}
                  />
                </SectionRow>
              </Col>
            </Row>

            <Row justify="space-around" align="top" gutter={24} style={{ marginRight: 0, marginLeft: 0 }}>
              <Col span={24} style={{ paddingLeft: 16, paddingRight: 16 }}>
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
                    pivotConfig={pivotConfig}
                  />
                ) : (
                  <h2 style={{ textAlign: 'center' }}>
                    Choose a measure or dimension to get started
                  </h2>
                )}
              </Col>
            </Row>
          </>
        );
      }}
    />
  );
}

PlaygroundQueryBuilder.propTypes = {
  query: PropTypes.object,
  setQuery: PropTypes.func,
  cubejsApi: PropTypes.object,
  dashboardSource: PropTypes.object,
  apiUrl: PropTypes.string,
  cubejsToken: PropTypes.string,
};

PlaygroundQueryBuilder.defaultProps = {
  query: {},
  setQuery: null,
  cubejsApi: null,
  dashboardSource: null,
  apiUrl: '/cubejs-api/v1',
  cubejsToken: null,
};
