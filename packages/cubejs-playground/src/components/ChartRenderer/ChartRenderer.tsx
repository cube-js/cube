import {
  RefObject,
  useContext,
  useEffect,
  useLayoutEffect,
  useRef,
  useState,
} from 'react';
import { Alert, Typography } from 'antd';
import { PlaySquareOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import type { ChartType, PivotConfig, Query } from '@cubejs-client/core';
import { ResultSet } from '@cubejs-client/core';
import { useHotkeys } from 'react-hotkeys-hook';
import { CubeContext } from '@cubejs-client/react';

import { Button, CubeLoader, FatalError } from '../../atoms';
import { UIFramework } from '../../types';
import { event } from '../../events';
import { QueryStatus } from '../PlaygroundQueryBuilder/components/PlaygroundQueryBuilder';

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

export type QueryLoadResult = {
  isLoading: boolean;
  resultSet?: ResultSet;
  error?: Error | null;
} & Partial<QueryStatus>;

type ChartRendererProps = {
  queryId: string;
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
  onQueryStatusChange: (result: QueryLoadResult) => void;
  onChartRendererReadyChange: (isReady: boolean) => void;
  onRunButtonClick: () => Promise<void>;
};

export default function ChartRenderer({
  queryId,
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
}: ChartRendererProps) {
  const { cubejsApi } = useContext(CubeContext);

  const runButtonRef = useRef<HTMLButtonElement>(null);
  const [isTokenRefreshing, setTokenRefreshing] = useState<boolean>(false);
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
    setResultSet(false);
  }, [framework]);

  useLayoutEffect(() => {
    let queryStartTime: number;
    window['__cubejsPlayground'] = {
      ...window['__cubejsPlayground'],
      onQueryStart: () => {
        queryStartTime = Date.now();
        onQueryStatusChange({ isLoading: true });
      },
      onQueryLoad: ({ resultSet, error }: QueryLoadResult) => {
        let isAggregated;
        const timeElapsed = Date.now() - queryStartTime;

        if (resultSet) {
          const { loadResponse } = resultSet.serialize();
          const {
            external,
            dbType,
            extDbType,
            usedPreAggregations = {},
          } = loadResponse.results[0] || {};

          setSlowQueryFromCache(Boolean(loadResponse.slowQuery));
          Boolean(loadResponse.slowQuery) && setSlowQuery(false);
          setResultSet(true);

          isAggregated = Object.keys(usedPreAggregations).length > 0;

          event(
            isAggregated
              ? 'load_request_success_aggregated:frontend'
              : 'load_request_success:frontend',
            {
              dbType,
              ...(isAggregated ? { external } : null),
              ...(external ? { extDbType } : null),
            }
          );
        }

        if (resultSet || error) {
          onQueryStatusChange({
            resultSet,
            error,
            isLoading: false,
            timeElapsed,
            isAggregated,
          });
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
  }, [framework]);

  const loading: boolean =
    queryHasMissingMembers ||
    isQueryLoading ||
    isPreAggregationBuildInProgress ||
    !cubejsApi;

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
      return <FatalError error={queryError} />;
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
              data-testid="run-query-btn"
              ref={runButtonRef}
              size="large"
              type="primary"
              loading={!isChartRendererReady || isTokenRefreshing}
              icon={<PlaySquareOutlined />}
              onClick={async () => {
                setTokenRefreshing(true);
                await onRunButtonClick();
                setTokenRefreshing(false);
              }}
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
        {cubejsApi ? (
          <iframe
            id={`iframe-${queryId}`}
            data-testid="chart-renderer"
            ref={iframeRef}
            title="Chart renderer"
            src={`/chart-renderers/${framework}/index.html`}
          />
        ) : null}
      </ChartContainer>
    </>
  );
}
