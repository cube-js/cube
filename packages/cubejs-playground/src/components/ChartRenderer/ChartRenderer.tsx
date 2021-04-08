import { RefObject, useEffect, useLayoutEffect, useRef, useState } from 'react';
import { Alert, Typography } from 'antd';
import { PlaySquareOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { ResultSet } from '@cubejs-client/core';
import { useHotkeys } from 'react-hotkeys-hook';
import type { PivotConfig, Query, ChartType } from '@cubejs-client/core';

import { Button, CubeLoader } from '../../atoms';
import { UIFramework } from '../../types';

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
};

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

export type TQueryLoadResult = {
  isLoading: boolean;
  resultSet?: ResultSet;
  error?: Error | null;
};

type TChartRendererProps = {
  query: Query;
  queryError: Error | null;
  isQueryLoading: boolean;
  areQueriesEqual: boolean;
  isChartRendererReady: boolean;
  queryHasMissingMembers: boolean;
  chartType: ChartType;
  pivotConfig?: PivotConfig;
  iframeRef: RefObject<HTMLIFrameElement>;
  framework: UIFramework;
  onQueryStatusChange: (result: TQueryLoadResult) => void;
  onChartRendererReadyChange: (isReady: boolean) => void;
  onRunButtonClick: () => void;
  onQueryChange: () => void;
};

export default function ChartRenderer({
  areQueriesEqual,
  queryError,
  iframeRef,
  framework,
  isChartRendererReady,
  queryHasMissingMembers,
  isQueryLoading,
  onChartRendererReadyChange,
  onQueryStatusChange,
  onRunButtonClick,
  onQueryChange
}: TChartRendererProps) {
  const runButtonRef = useRef<HTMLButtonElement>(null);
  const [slowQuery, setSlowQuery] = useState(false);
  const [resultSetExists, setResultSet] = useState(false);
  const [slowQueryFromCache, setSlowQueryFromCache] = useState(false);
  const [isPreAggregationBuildInProgress, setBuildInProgress] = useState(false);

  // for you, ovr :)
  useHotkeys('cmd+enter', () => {
    runButtonRef.current?.click();
  });

  useEffect(() => {
    return () => {
      onChartRendererReadyChange(false);
    };
    // eslint-disable-next-line
  }, []);

  useEffect(() => {
    onQueryChange();
  }, [areQueriesEqual])

  useEffect(() => {
    setResultSet(false);
  }, [framework]);

  useLayoutEffect(() => {
    window['__cubejsPlayground'] = {
      ...window['__cubejsPlayground'],
      onQueryStart: () => {
        onQueryStatusChange({ isLoading: true });
      },
      onQueryLoad: ({ resultSet, error }: TQueryLoadResult) => {
        if (resultSet) {
          const { loadResponse } = resultSet.serialize();

          setSlowQueryFromCache(Boolean(loadResponse.slowQuery));
          Boolean(loadResponse.slowQuery) && setSlowQuery(false);
          setResultSet(true);
        }

        if (resultSet || error) {
          onQueryStatusChange({ resultSet, error, isLoading: false });
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
  }, [framework, onChartRendererReadyChange]);

  const loading: boolean =
    !isChartRendererReady ||
    queryHasMissingMembers ||
    isQueryLoading ||
    isPreAggregationBuildInProgress;

  const invisible: boolean =
    !isChartRendererReady ||
    isPreAggregationBuildInProgress ||
    Boolean(queryError) ||
    queryHasMissingMembers ||
    loading ||
    !areQueriesEqual ||
    !resultSetExists;

  const renderExtras = () => {
    if (queryError) {
      return <div>{queryError?.toString()}</div>;
    }

    if (queryHasMissingMembers) {
      return (
        <div>
          At least one of the query members is missing from your data schema.
          Please update your query or data schema.
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

    if (!areQueriesEqual || !resultSetExists) {
      return (
        <Positioner>
          <Centered>
            <Button
              data-test-id="query-execute-button"
              ref={runButtonRef}
              size="large"
              type="primary"
              loading={isQueryLoading}
              icon={<PlaySquareOutlined />}
              onClick={onRunButtonClick}
            >
              Run
            </Button>
          </Centered>
        </Positioner>
      );
    }

    return null;
  };

  const slowQueryMsg = slowQuery
    ? 'This query takes more than 5 seconds to execute. Please consider using pre-aggregations to improve its performance. '
    : slowQueryFromCache
    ? "This query takes more than 5 seconds to execute. It was served from the cache because Cube.js wasn't able to renew it in less than 5 seconds. Please consider using pre-aggregations to improve its performance. "
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
