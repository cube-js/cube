import {
  areQueriesEqual,
  ChartType,
  PivotConfig,
  PreAggregationType,
  Query,
  validateQuery,
  TransformedQuery,
} from '@cubejs-client/core';
import {
  QueryBuilder,
  SchemaChangeProps,
  VizState,
} from '@cubejs-client/react';
import { Col, Row, Space } from 'antd';
import React, { RefObject, useEffect, useRef, useState } from 'react';
import styled from 'styled-components';

import { Card, FatalError } from '../../../atoms';
import ChartContainer from '../../../ChartContainer';
import { SectionHeader, SectionRow } from '../../../components';
import ChartRenderer from '../../../components/ChartRenderer/ChartRenderer';
import Settings from '../../../components/Settings/Settings';
import DashboardSource from '../../../DashboardSource';
import { playgroundAction } from '../../../events';
import {
  useDeepEffect,
  useIsMounted,
  useSecurityContext,
  useServerCoreVersionGte,
} from '../../../hooks';
import FilterGroup from '../../../QueryBuilder/FilterGroup';
import MemberGroup from '../../../QueryBuilder/MemberGroup';
import SelectChartType from '../../../QueryBuilder/SelectChartType';
import TimeGroup from '../../../QueryBuilder/TimeGroup';
import { UIFramework } from '../../../types';
import { dispatchPlaygroundEvent } from '../../../utils';
import {
  useChartRendererState,
  useChartRendererStateMethods,
} from '../../QueryTabs/ChartRendererStateProvider';
import { PreAggregationStatus } from './PreAggregationStatus';

const Section = styled.div`
  display: flex;
  flex-flow: column;
  margin-right: 24px;
  margin-bottom: 16px;

  > *:first-child {
    margin-bottom: 8px;
  }
`;

const Wrapper = styled.div`
  background-color: var(--layout-body-background);
  padding-bottom: 16px;
`;

export const frameworkChartLibraries: Record<
  UIFramework,
  Array<{ value: string; title: string }>
> = {
  react: [
    {
      value: 'chartjs',
      title: 'Chart.js',
    },
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
        if (values?.values) {
          actionName = 'Update Filter Values';
        }
        if (values?.dateRange) {
          actionName = 'Update Date Range';
        }
        if (values?.granularity) {
          actionName = 'Update Granularity';
        }
        playgroundAction(actionName, { memberName });
        return updateMethods[method].apply(null, [member, values, ...rest]);
      },
    }))
    .reduce((a, b) => ({ ...a, ...b }), {});

type PivotChangeEmitterProps = {
  iframeRef: RefObject<HTMLIFrameElement> | null;
  chartType: ChartType;
  pivotConfig?: PivotConfig;
};

function PivotChangeEmitter({
  iframeRef,
  pivotConfig,
  chartType,
}: PivotChangeEmitterProps) {
  useDeepEffect(() => {
    if (iframeRef?.current && ['table', 'bar'].includes(chartType)) {
      dispatchPlaygroundEvent(iframeRef.current.contentDocument, 'chart', {
        pivotConfig,
      });
    }
  }, [iframeRef, pivotConfig]);

  return null;
}

type QueryChangeEmitterProps = {
  query1: Query | null;
  query2: Query | null;
  onChange: () => void;
};

function QueryChangeEmitter({
  query1,
  query2,
  onChange,
}: QueryChangeEmitterProps) {
  useDeepEffect(() => {
    if (!areQueriesEqual(validateQuery(query1), validateQuery(query2))) {
      onChange();
    }
  }, [query1, query2]);

  return null;
}

type HandleRunButtonClickProps = {
  query: Query;
  pivotConfig?: PivotConfig;
  chartType: ChartType;
};

export type PlaygroundQueryBuilderProps = {
  apiUrl: string;
  cubejsToken: string;
  queryId: string;
  defaultQuery?: Query;
  dashboardSource?: DashboardSource;
  schemaVersion?: number;
  initialVizState?: VizState;
  extra?: React.FC<any>;
  onVizStateChanged?: (vizState: VizState) => void;
  onSchemaChange?: (props: SchemaChangeProps) => void;
};

export type QueryStatus = {
  timeElapsed: number;
  isAggregated: boolean;
  external: boolean | null;
  extDbType: string;
  preAggregationType?: PreAggregationType;
  transformedQuery?: TransformedQuery;
};

export function PlaygroundQueryBuilder({
  apiUrl,
  cubejsToken,
  defaultQuery,
  queryId,
  dashboardSource,
  schemaVersion = 0,
  initialVizState,
  extra: Extra,
  onSchemaChange,
  onVizStateChanged,
}: PlaygroundQueryBuilderProps) {
  const isMounted = useIsMounted();

  const isGraphQLSupported = useServerCoreVersionGte('0.29.0');

  const { isChartRendererReady, queryStatus, queryError, queryRequestId } =
    useChartRendererState(queryId);
  const {
    setQueryStatus,
    setQueryLoading,
    setChartRendererReady,
    setQueryError,
  } = useChartRendererStateMethods();
  
  const { refreshToken } = useSecurityContext();

  const iframeRef = useRef<HTMLIFrameElement>(null);
  const queryRef = useRef<Query | null>(null);

  const [tokenRefreshed, setTokenRefreshed] = useState<boolean>(false);
  const [framework, setFramework] = useState<UIFramework>('react');
  const [chartingLibrary, setChartingLibrary] = useState<string>('chartjs');

  useEffect(() => {
    (async () => {
      await refreshToken();

      if (isMounted()) {
        setTokenRefreshed(true);
      }
    })();
  }, [isMounted]);

  useEffect(() => {
    if (isChartRendererReady && iframeRef.current) {
      dispatchPlaygroundEvent(
        iframeRef.current.contentDocument,
        'credentials',
        {
          token: cubejsToken,
          apiUrl,
        }
      );
    }
  }, [iframeRef, cubejsToken, apiUrl, isChartRendererReady]);

  function handleRunButtonClick({
    query,
    pivotConfig,
    chartType,
  }: HandleRunButtonClickProps) {
    if (iframeRef.current) {
      if (areQueriesEqual(query, queryRef.current)) {
        dispatchPlaygroundEvent(iframeRef.current.contentDocument, 'chart', {
          pivotConfig,
          query,
          chartType,
          chartingLibrary,
        });
        dispatchPlaygroundEvent(iframeRef.current.contentDocument, 'refetch');
      } else {
        dispatchPlaygroundEvent(iframeRef.current.contentDocument, 'chart', {
          pivotConfig,
          query,
          chartType,
          chartingLibrary,
        });
      }
    }

    setQueryError(queryId, null);
    queryRef.current = query;
  }

  if (!tokenRefreshed) {
    return null;
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
        richMetaError,
        metaErrorStack,
        meta,
        isQueryPresent,
        chartType,
        updateChartType,
        measures,
        updateMeasures,
        dimensions,
        updateDimensions,
        segments,
        updateSegments,
        filters,
        updateFilters,
        timeDimensions,
        updateTimeDimensions,
        orderMembers,
        updateOrder,
        pivotConfig,
        updatePivotConfig,
        missingMembers,
        isFetchingMeta,
        dryRunResponse,
        availableMembers,
        availableFilterMembers,
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
          <Wrapper data-testid={`query-builder-${queryId}`}>
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
                        availableMembers={availableMembers?.measures || []}
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
                        availableMembers={availableMembers?.dimensions || []}
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
                        availableMembers={availableMembers?.segments || []}
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
                        availableMembers={
                          availableMembers?.timeDimensions || []
                        }
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
                        availableMembers={availableFilterMembers}
                        missingMembers={missingMembers}
                        addMemberName="Filter"
                        updateMethods={playgroundActionUpdateMethods(
                          updateFilters,
                          'Filter'
                        )}
                      />
                    </Section>
                  </Row>
                </Card>

                <SectionRow
                  style={{
                    margin: 16,
                    marginBottom: 0,
                  }}
                >
                  <SelectChartType
                    chartType={chartType || 'line'}
                    updateChartType={(type) => {
                      playgroundAction('Change Chart Type');
                      updateChartType(type);

                      if (iframeRef.current) {
                        dispatchPlaygroundEvent(
                          iframeRef.current.contentDocument,
                          'chart',
                          {
                            chartType: type,
                            chartingLibrary,
                          }
                        );
                      }
                    }}
                  />

                  <Settings
                    isQueryPresent={isQueryPresent}
                    limit={query.limit || 5000}
                    pivotConfig={pivotConfig}
                    disabled={isFetchingMeta}
                    orderMembers={orderMembers}
                    onReorder={updateOrder.reorder}
                    onOrderChange={updateOrder.set}
                    onMove={updatePivotConfig.moveItem}
                    onUpdate={updatePivotConfig.update}
                  />

                  <Space style={{ marginLeft: 'auto' }}>
                    {Extra ? (
                      <Extra
                        queryRequestId={queryRequestId || queryError?.response?.requestId}
                        queryStatus={queryStatus}
                        error={queryError}
                      />
                    ) : null}
                    {queryStatus ? (
                      <PreAggregationStatus
                        apiUrl={apiUrl}
                        availableMembers={availableMembers}
                        query={query}
                        {...(queryStatus as QueryStatus)}
                      />
                    ) : null}
                  </Space>
                </SectionRow>
              </Col>
            </Row>

            <Row
              justify="space-around"
              align="top"
              gutter={32}
              style={{ margin: 0 }}
            >
              <Col span={24}>
                {!isQueryPresent && richMetaError ? (
                  <Card>
                    <FatalError error={richMetaError} stack={metaErrorStack} />
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
                    meta={meta}
                    isGraphQLSupported={isGraphQLSupported}
                    iframeRef={iframeRef}
                    isChartRendererReady={isChartRendererReady}
                    query={query}
                    error={error}
                    chartType={chartType || 'line'}
                    pivotConfig={pivotConfig}
                    framework={framework}
                    chartingLibrary={chartingLibrary}
                    dashboardSource={dashboardSource}
                    setFramework={(currentFramework) => {
                      if (currentFramework !== framework) {
                        setQueryLoading(queryId, false);
                        setFramework(currentFramework);
                      }
                    }}
                    setChartLibrary={(value) => {
                      if (iframeRef.current) {
                        dispatchPlaygroundEvent(
                          iframeRef.current.contentDocument,
                          'chart',
                          {
                            chartingLibrary: value,
                          }
                        );
                      }
                      setChartingLibrary(value);
                    }}
                    chartLibraries={frameworkChartLibraries}
                    isFetchingMeta={isFetchingMeta}
                    render={({ framework }) => {
                      if (richMetaError) {
                        return <FatalError error={richMetaError} stack={metaErrorStack} />;
                      }

                      return (
                        <ChartRenderer
                          queryId={queryId}
                          areQueriesEqual={areQueriesEqual(
                            validateQuery(query),
                            validateQuery(queryRef.current)
                          )}
                          isFetchingMeta={isFetchingMeta}
                          queryError={queryError}
                          framework={framework}
                          chartType={chartType || 'line'}
                          query={query}
                          pivotConfig={pivotConfig}
                          iframeRef={iframeRef}
                          queryHasMissingMembers={missingMembers.length > 0}
                          onRunButtonClick={async () => {
                            if (
                              isChartRendererReady &&
                              iframeRef.current &&
                              missingMembers.length === 0
                            ) {
                              await refreshToken();

                              handleRunButtonClick({
                                query: validateQuery(query),
                                pivotConfig,
                                chartType: chartType || 'line',
                              });
                            }
                          }}
                        />
                      );
                    }}
                    onChartRendererReadyChange={(isReady) =>
                      setChartRendererReady(queryId, isReady)
                    }
                  />
                )}
              </Col>
            </Row>

            <PivotChangeEmitter
              iframeRef={iframeRef}
              chartType={chartType || 'line'}
              pivotConfig={pivotConfig}
            />

            <QueryChangeEmitter
              query1={query}
              query2={queryRef.current}
              onChange={() => {
                setQueryLoading(queryId, false);
                setQueryStatus(queryId, null);
              }}
            />
          </Wrapper>
        );
      }}
    />
  );
}
