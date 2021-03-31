import { useState, useRef, useEffect } from 'react';
import { Col, Row, Divider } from 'antd';
import { LockOutlined, PlaySquareOutlined } from '@ant-design/icons';
import { QueryBuilder } from '@cubejs-client/react';
import {
  areQueriesEqual,
  PivotConfig,
  Query,
  ChartType,
} from '@cubejs-client/core';
import styled from 'styled-components';

import { playgroundAction } from './events';
import MemberGroup from './QueryBuilder/MemberGroup';
import FilterGroup from './QueryBuilder/FilterGroup';
import TimeGroup from './QueryBuilder/TimeGroup';
import SelectChartType from './QueryBuilder/SelectChartType';
import Settings from './components/Settings/Settings';
import ChartRenderer from './components/ChartRenderer/ChartRenderer';
import { Card, SectionHeader, SectionRow, Button } from './components';
import ChartContainer from './ChartContainer';
import { dispatchPlaygroundEvent } from './utils';
import { useSecurityContext } from './hooks';
import { FatalError } from './atoms';
import { UIFramework } from './types';

const Section = styled.div`
  display: flex;
  flex-flow: column;
  margin-right: 24px;
  margin-bottom: 16px;

  > *:first-child {
    margin-bottom: 8px;
  }
`;

export const frameworkChartLibraries: Record<
  UIFramework,
  Array<{ value: string; title: string }>
> = {
  react: [
    {
      value: 'bizcharts',
      title: 'Bizcharts',
    },
    {
      value: 'recharts',
      title: 'Recharts',
    },
    {
      value: 'd3',
      title: 'D3',
    },
    {
      value: 'chartjs',
      title: 'Chart.js',
    },
  ],
  angular: [
    {
      value: 'angular-ng2-charts',
      title: 'ng2',
    },
  ],
  vue: [
    {
      value: 'chartkick',
      title: 'Chartkick',
    },
  ],
};

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

type THandleRunButtonClickProps = {
  query: Query;
  pivotConfig?: PivotConfig;
  chartType: ChartType;
};

export default function PlaygroundQueryBuilder({
  apiUrl,
  cubejsToken,
  defaultQuery,
  dashboardSource,
  schemaVersion = 0,
  initialVizState,
  onSchemaChange,
  onVizStateChanged,
}: any) {
  const ref = useRef<HTMLIFrameElement>(null);
  const queryRef = useRef<Query | null>(null);
  const [framework, setFramework] = useState('react');
  const [chartingLibrary, setChartingLibrary] = useState('bizcharts');
  const [isChartRendererReady, setChartRendererReady] = useState(false);
  const [isQueryLoading, setQueryLoading] = useState(false);
  const [queryError, setQueryError] = useState<Error | null>(null);
  const { token, setIsModalOpen } = useSecurityContext();

  useEffect(() => {
    if (isChartRendererReady && ref.current) {
      dispatchPlaygroundEvent(ref.current.contentDocument, 'credentials', {
        token: cubejsToken,
        apiUrl,
      });
    }
  }, [ref, cubejsToken, apiUrl, isChartRendererReady]);

  function handleRunButtonClick({
    query,
    pivotConfig,
    chartType,
  }: THandleRunButtonClickProps) {
    if (ref.current) {
      if (areQueriesEqual(query, queryRef.current)) {
        dispatchPlaygroundEvent(ref.current.contentDocument, 'chart', {
          pivotConfig,
          query,
          chartType,
          chartingLibrary,
        });
        dispatchPlaygroundEvent(ref.current.contentDocument, 'refetch', {});
      } else {
        dispatchPlaygroundEvent(ref.current.contentDocument, 'chart', {
          pivotConfig,
          query,
          chartType,
          chartingLibrary,
        });
      }
    }

    setQueryError(null);
    queryRef.current = query;
  }

  return (
    <QueryBuilder
      defaultQuery={defaultQuery}
      initialVizState={initialVizState}
      wrapWithQueryRenderer={false}
      schemaVersion={schemaVersion}
      onSchemaChange={onSchemaChange}
      onVizStateChanged={onVizStateChanged}
      render={({
        query,
        error,
        metaError,
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
        missingMembers,
        isFetchingMeta,
        dryRunResponse,
      }) => {
        let parsedDateRange;

        if (dryRunResponse) {
          const { timeDimensions = [] } = dryRunResponse.pivotQuery || {};
          parsedDateRange = timeDimensions[0]?.dateRange;
        } else if (Array.isArray(query.timeDimensions?.[0]?.dateRange)) {
          // @ts-ignore
          parsedDateRange = query.timeDimensions[0].dateRange;
        }

        return (
          <>
            <Row>
              <Col span={24}>
                <Card
                  bordered={false}
                  style={{
                    borderRadius: 0,
                    borderBottom: 1,
                  }}
                >
                  <Button.Group>
                    <Button
                      icon={<LockOutlined />}
                      size="small"
                      type={token ? 'primary' : 'default'}
                      onClick={() => setIsModalOpen(true)}
                    >
                      {token ? 'Edit' : 'Add'} Security Context
                    </Button>
                  </Button.Group>
                </Card>
              </Col>
            </Row>

            <Divider style={{ margin: 0 }} />

            <Row
              justify="space-around"
              align="top"
              gutter={24}
              style={{ marginBottom: 12 }}
            >
              <Col span={24}>
                <Card bordered={false} style={{ borderRadius: 0 }}>
                  <Row align="top" gutter={0} style={{ marginBottom: -12 }}>
                    <Section>
                      <SectionHeader>Measures</SectionHeader>
                      <MemberGroup
                        disabled={isFetchingMeta}
                        members={measures}
                        availableMembers={availableMeasures}
                        missingMembers={missingMembers}
                        addMemberName="Measure"
                        updateMethods={playgroundActionUpdateMethods(
                          updateMeasures,
                          'Measure'
                        )}
                      />
                    </Section>

                    <Section>
                      <SectionHeader>Dimensions</SectionHeader>
                      <MemberGroup
                        disabled={isFetchingMeta}
                        members={dimensions}
                        availableMembers={availableDimensions}
                        missingMembers={missingMembers}
                        addMemberName="Dimension"
                        updateMethods={playgroundActionUpdateMethods(
                          updateDimensions,
                          'Dimension'
                        )}
                      />
                    </Section>

                    <Section>
                      <SectionHeader>Segment</SectionHeader>
                      <MemberGroup
                        disabled={isFetchingMeta}
                        members={segments}
                        availableMembers={availableSegments}
                        missingMembers={missingMembers}
                        addMemberName="Segment"
                        updateMethods={playgroundActionUpdateMethods(
                          updateSegments,
                          'Segment'
                        )}
                      />
                    </Section>

                    <Section>
                      <SectionHeader>Time</SectionHeader>
                      <TimeGroup
                        disabled={isFetchingMeta}
                        members={timeDimensions}
                        availableMembers={availableTimeDimensions}
                        missingMembers={missingMembers}
                        addMemberName="Time"
                        updateMethods={playgroundActionUpdateMethods(
                          updateTimeDimensions,
                          'Time'
                        )}
                        parsedDateRange={parsedDateRange}
                      />
                    </Section>

                    <Section>
                      <SectionHeader>Filters</SectionHeader>
                      <FilterGroup
                        disabled={isFetchingMeta}
                        members={filters}
                        availableMembers={availableDimensions.concat(
                          availableMeasures as any
                        )}
                        missingMembers={missingMembers}
                        addMemberName="Filter"
                        updateMethods={playgroundActionUpdateMethods(
                          updateFilters,
                          'Filter'
                        )}
                      />
                    </Section>

                    <Section>
                      <SectionHeader>Execute</SectionHeader>

                      <Button
                        type="primary"
                        loading={isQueryLoading}
                        icon={<PlaySquareOutlined />}
                        onClick={() => {
                          if (
                            isChartRendererReady &&
                            ref.current &&
                            missingMembers.length === 0
                          ) {
                            handleRunButtonClick({
                              query,
                              pivotConfig,
                              chartType: chartType || 'line',
                            });
                          }
                        }}
                      >
                        Run
                      </Button>
                    </Section>
                  </Row>
                </Card>

                <SectionRow
                  style={{
                    marginTop: 16,
                    marginLeft: 16,
                  }}
                >
                  <SelectChartType
                    chartType={chartType || 'line'}
                    updateChartType={(type) => {
                      playgroundAction('Change Chart Type');
                      updateChartType(type);

                      if (ref.current) {
                        dispatchPlaygroundEvent(
                          ref.current.contentDocument,
                          'chart',
                          {
                            chartType: type,
                            chartingLibrary
                          }
                        );
                      }
                    }}
                  />

                  <Settings
                    isQueryPresent={isQueryPresent}
                    limit={query.limit}
                    pivotConfig={pivotConfig}
                    disabled={isFetchingMeta}
                    orderMembers={orderMembers}
                    onReorder={updateOrder.reorder}
                    onOrderChange={updateOrder.set}
                    onMove={updatePivotConfig.moveItem}
                    onUpdate={updatePivotConfig.update}
                  />
                </SectionRow>
              </Col>
            </Row>

            <Row
              justify="space-around"
              align="top"
              gutter={24}
              style={{
                marginRight: 0,
                marginLeft: 0,
              }}
            >
              <Col
                span={24}
                style={{
                  paddingLeft: 16,
                  paddingRight: 16,
                }}
              >
                {!isQueryPresent && metaError ? (
                  <Card>
                    <FatalError error={metaError} />
                  </Card>
                ) : null}

                {!isQueryPresent && !metaError && (
                  <h2 style={{ textAlign: 'center' }}>
                    Choose a measure or dimension to get started
                  </h2>
                )}

                {isQueryPresent && (
                  <ChartContainer
                    apiUrl={apiUrl}
                    cubejsToken={cubejsToken}
                    iframeRef={ref}
                    isChartRendererReady={isChartRendererReady}
                    query={query}
                    error={error}
                    chartType={chartType}
                    pivotConfig={pivotConfig}
                    framework={framework}
                    chartingLibrary={chartingLibrary}
                    setFramework={setFramework}
                    setChartLibrary={(value) => {
                      if (ref.current) {
                        dispatchPlaygroundEvent(
                          ref.current.contentDocument,
                          'chart',
                          {
                            chartingLibrary: value,
                          }
                        );
                      }
                      setChartingLibrary(value);
                    }}
                    chartLibraries={frameworkChartLibraries}
                    dashboardSource={dashboardSource}
                    isFetchingMeta={isFetchingMeta}
                    render={({ framework }) => {
                      if (metaError) {
                        return <FatalError error={metaError} />;
                      }

                      return (
                        <ChartRenderer
                          isQueryLoading={isQueryLoading}
                          isChartRendererReady={
                            isChartRendererReady && !isFetchingMeta
                          }
                          queryError={queryError}
                          framework={framework}
                          chartType={chartType || 'line'}
                          query={query}
                          pivotConfig={pivotConfig}
                          iframeRef={ref}
                          queryHasMissingMembers={missingMembers.length > 0}
                          onQueryStatusChange={({
                            isLoading,
                            resultSet,
                            error,
                          }) => {
                            if (resultSet) {
                              setQueryError(null);
                            }
                            if (error) {
                              setQueryError(error);
                            }

                            setQueryLoading(isLoading);
                          }}
                          onChartRendererReadyChange={setChartRendererReady}
                        />
                      );
                    }}
                    onChartRendererReadyChange={setChartRendererReady}
                  />
                )}
              </Col>
            </Row>
          </>
        );
      }}
    />
  );
}
