import { useEffect, useLayoutEffect, useState } from 'react';
import { Alert, Spin, Typography } from 'antd';
import styled from 'styled-components';

import { dispatchPlaygroundEvent } from '../../utils';
import { useDeepCompareMemoize } from '../../hooks';

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
  queryHasMissingMembers,
  onChartRendererReadyChange,
}) {
  const [slowQuery, setSlowQuery] = useState(false);
  const [isPreAggregationBuildInProgress, setBuildInProgress] = useState(false);

  useEffect(() => {
    return () => {
      onChartRendererReadyChange(false);
    };
    // eslint-disable-next-line
  }, []);

  useEffect(() => {
    if (isChartRendererReady && iframeRef.current && !queryHasMissingMembers) {
      dispatchPlaygroundEvent(iframeRef.current.contentDocument, 'chart', {
        pivotConfig,
        query,
        chartType,
        chartingLibrary,
      });
    }
    // eslint-disable-next-line
  }, useDeepCompareMemoize([iframeRef, isChartRendererReady, pivotConfig, query, chartType, queryHasMissingMembers]));

  useLayoutEffect(() => {
    window['__cubejsPlayground'] = {
      ...window['__cubejsPlayground'],
      onQueryLoad: (data) => {
        let resultSet;

        if (data?.resultSet !== undefined) {
          resultSet = data.resultSet;
        } else {
          resultSet = data;
        }

        if (resultSet) {
          const { loadResponse } = resultSet.serialize();

          setSlowQuery(Boolean(loadResponse.slowQuery));
        }
      },
      onQueryProgress: (progress) => {
        setBuildInProgress(
          Boolean(progress?.stage?.stage.includes('pre-aggregation'))
        );
      },
      onChartRendererReady() {
        onChartRendererReadyChange(true);
      },
    };
  }, [onChartRendererReadyChange]);

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
      ) : !isChartRendererReady || queryHasMissingMembers ? (
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
