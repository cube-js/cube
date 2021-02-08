import { useEffect } from 'react';
import { Alert, Spin, Typography } from 'antd';
import styled from 'styled-components';

import { dispatchPlaygroundEvent } from '../../utils';
import {
  useDeepCompareMemoize,
  useSlowQuery,
  useIsPreAggregationBuildInProgress,
} from '../../hooks';

const { Text } = Typography;

const ChartContainer = styled.div`
  visibility: ${(props) => (props.hidden ? 'hidden' : 'visible')};

  & > iframe {
    width: 100%;
    min-height: 400px;
    border: none;
  }
`;

const RequestMessage = styled.div`
  display: flex;
  width: 100%;
  min-height: 400px;
  align-items: center;
  justify-content: center;
`;

export default function ChartRenderer({
  iframeRef,
  framework,
  isChartRendererReady,
  chartingLibrary,
  chartType,
  query,
  pivotConfig,
  onChartRendererReadyChange,
}) {
  const slowQuery = useSlowQuery();
  const isPreAggregationBuildInProgress = useIsPreAggregationBuildInProgress();

  useEffect(() => {
    return () => {
      onChartRendererReadyChange(false);
    };
    // eslint-disable-next-line
  }, []);

  useEffect(() => {
    if (isChartRendererReady && iframeRef.current) {
      dispatchPlaygroundEvent(iframeRef.current.contentDocument, 'chart', {
        pivotConfig,
        query,
        chartType,
        chartingLibrary,
      });
    }
    // eslint-disable-next-line
  }, useDeepCompareMemoize([iframeRef, isChartRendererReady, pivotConfig, query, chartType]));

  return (
    <>
      {slowQuery ? (
        <Alert
          style={{ marginBottom: 24 }}
          message="Query is too slow to be renewed during the user request and was served from the cache. Please consider using low latency pre-aggregations."
          type="warning"
        />
      ) : null}

      {isPreAggregationBuildInProgress ? (
        <RequestMessage>
          <Text strong style={{ fontSize: 18 }}>
            Building pre-aggregations...
          </Text>
        </RequestMessage>
      ) : !isChartRendererReady ? (
        <Spin />
      ) : null}

      <ChartContainer
        hidden={!isChartRendererReady || isPreAggregationBuildInProgress}
      >
        <iframe
          ref={iframeRef}
          title="Chart renderer"
          src={`/chart-renderers/${framework}/index.html`}
        />
      </ChartContainer>
    </>
  );
}
