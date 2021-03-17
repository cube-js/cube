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

type TChartContainerProps = {
  invisible: boolean;
}

const ChartContainer = styled.div<TChartContainerProps>`
  visibility: ${(props) => (props.invisible ? 'hidden' : 'visible')};
  min-height: 400px;

  & > iframe {
    width: 100%;
    min-height: 400px;
    border: none;
  }
`;

const RequestMessage = styled.div`
  position: absolute;
  bottom: 24px;
  width: 100%;
  bottom: -4em;
  animation: fadeIn 0.3s;

  @keyframes fadeIn {
    from {
      opacity: 0;
    }

    to {
      opacity: 1;
    }
  }
`;

const Centered = styled.div`
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
`;

const Wrapper = styled.div`
  position: relative;
  width: 100%;
  text-align: center;
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
  const [slowQueryFromCache, setSlowQueryFromCache] = useState(false);
  const [isPreAggregationBuildInProgress, setBuildInProgress] = useState(false);
  const [isQueryLoading, setQueryLoading] = useState(true);
  const [queryError, setQueryError] = useState<Error | null>(null);

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

          setSlowQueryFromCache(Boolean(loadResponse.slowQuery));
          Boolean(loadResponse.slowQuery) && setSlowQuery(false);
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

        const isQuerySlow =
          progress?.stage?.stage.includes('Executing query') &&
          (progress.stage.timeElapsed || 0) >= 5000;

        setSlowQuery(isQuerySlow);
        isQuerySlow && setSlowQueryFromCache(false);
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
    !isChartRendererReady ||
    queryHasMissingMembers ||
    isQueryLoading ||
    isPreAggregationBuildInProgress;

  const renderExtras = () => {
    if (queryError) {
      return <div>{queryError?.toString()}</div>;
    }

    if (queryHasMissingMembers) {
      return (
        <div>
          At least of the query members is missing from your data schema. Please
          update your query or data schema.
        </div>
      );
    }

    if (loading) {
      return (
        <Positioner>
          <Centered>
            <Wrapper>
              <CubeLoader full={false} />

              {isPreAggregationBuildInProgress && (
                <RequestMessage>
                  <Text strong style={{ fontSize: 18 }}>
                    Building pre-aggregations...
                  </Text>
                </RequestMessage>
              )}
            </Wrapper>
          </Centered>
        </Positioner>
      );
    }

    return null;
  };

  const slowQueryMsg = slowQuery
    ? 'This query takes more than 5 seconds to execute. Please consider using pre-aggregations to improve its performance. '
    : slowQueryFromCache
    ? 'This query takes more than 5 seconds to execute. It was served from the cache because Cube.js wasn\'t able to renew it in less than 5 seconds. Please consider using pre-aggregations to improve its performance. '
    : '';

  return (
    <>
      {(slowQuery || slowQueryFromCache) && (
        <Alert
          style={{ marginBottom: 24 }}
          message={slowQueryMsg}
          type="warning"
        />
      )}

      {renderExtras()}

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
