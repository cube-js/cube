import { useEffect, useLayoutEffect, useState } from 'react';
import { Alert, Typography } from 'antd';
import styled from 'styled-components';

import { CubeLoader } from '../../atoms';
import { dispatchPlaygroundEvent } from '../../utils';
import { useDeepCompareMemoize } from '../../hooks';

const { Text } = Typography;

const Positioner = styled.div`
  position: absolute;
  top: 0;
  bottom: 0;
  right: 0;
  left: 0;
`;

const ChartContainer = styled.div`
  visibility: ${(props) => (props.invisible ? 'hidden' : 'visible')};
  min-height: 400px;

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
  const [isQueryLoading, setQueryLoading] = useState(true);
  const [queryError, setQueryError] = useState(null);

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
      setQueryLoading(true);
      setQueryError(null);
    }
    // eslint-disable-next-line
  }, useDeepCompareMemoize([iframeRef, isChartRendererReady, pivotConfig, query, chartType, queryHasMissingMembers]));

  useLayoutEffect(() => {
    window['__cubejsPlayground'] = {
      ...window['__cubejsPlayground'],
      onQueryLoad: (data) => {
        let resultSet;
        let error = null;

        if (data?.resultSet !== undefined) {
          resultSet = data.resultSet;
          error = data.error;
        } else {
          resultSet = data;
        }

        if (resultSet) {
          const { loadResponse } = resultSet.serialize();

          setSlowQuery(Boolean(loadResponse.slowQuery));
          setQueryLoading(false);
          setQueryError(null);
        }
        if (error) {
          setQueryLoading(false);
          setQueryError(error);
        }
      },
      onQueryProgress: (progress) => {
        setBuildInProgress(
          Boolean(progress?.stage?.stage.includes('pre-aggregation'))
        );
        // setSlowQuery(
        //   Boolean(progress?.stage?.stage.includes('Executing query'))
        // );
      },
      onChartRendererReady() {
        onChartRendererReadyChange(true);
      },
    };
  }, [onChartRendererReadyChange]);

  const invisible =
    !isChartRendererReady ||
    isPreAggregationBuildInProgress ||
    queryError ||
    queryHasMissingMembers;
  const loading =
    !isChartRendererReady || queryHasMissingMembers || isQueryLoading;

  const extras = () => {
    if (queryError) {
      return <div>{queryError?.toString()}</div>;
    }

    if (isPreAggregationBuildInProgress) {
      return (
        <Positioner>
          <RequestMessage>
            <Text strong style={{ fontSize: 18 }}>
              Building pre-aggregations...
            </Text>
          </RequestMessage>
        </Positioner>
      );
    }

    if (loading) {
      return (
        <Positioner key="loader">
          <CubeLoader />
        </Positioner>
      );
    }

    return null;
  };

  return (
    <>
      {slowQuery && (
        <Alert
          style={{ marginBottom: 24 }}
          message="Query is too slow to be renewed during the user request and was served from the cache. Please consider using low latency pre-aggregations."
          type="warning"
        />
      )}

      {extras()}

      <ChartContainer invisible={invisible}>
        <iframe
          ref={iframeRef}
          title="Chart renderer"
          src={`/chart-renderers/${framework}/index.html`}
        />
      </ChartContainer>
    </>
  );
}
