import { PlaySquareOutlined } from '@ant-design/icons';
import type { ChartType, PivotConfig, Query } from '@cubejs-client/core';
import { ResultSet } from '@cubejs-client/core';
import { Alert, Typography } from 'antd';
import { RefObject, useContext, useEffect, useRef, useState } from 'react';
import { useHotkeys } from 'react-hotkeys-hook';
import styled from 'styled-components';
import { CubeContext } from '@cubejs-client/react';

import { Button, CubeLoader, FatalError } from '../../atoms';
import { UIFramework } from '../../types';
import { QueryStatus } from '../PlaygroundQueryBuilder/components/PlaygroundQueryBuilder';
import {
  useChartRendererState,
  useChartRendererStateMethods,
} from '../QueryTabs/ChartRendererStateProvider';
import { useWindowSize } from '../../hooks';

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
  resultSet?: ResultSet;
  error?: Error | null;
} & Partial<QueryStatus>;

type ChartRendererProps = {
  queryId: string;
  query: Query;
  queryError: Error | null;
  isFetchingMeta: boolean;
  areQueriesEqual: boolean;
  queryHasMissingMembers: boolean;
  chartType: ChartType;
  pivotConfig?: PivotConfig;
  iframeRef: RefObject<HTMLIFrameElement>;
  framework: UIFramework;
  onRunButtonClick: () => Promise<void>;
};

export default function ChartRenderer({
  queryId,
  areQueriesEqual,
  isFetchingMeta,
  iframeRef,
  framework,
  queryHasMissingMembers,
  onRunButtonClick,
}: ChartRendererProps) {
  const { cubejsApi } = useContext(CubeContext);
  const [containerSize, setContainerSize] = useState('auto');

  useWindowSize(); // triggers the following useEffect() on window size change

  useEffect(() => {
    if (iframeRef?.current) {
      const container = iframeRef?.current;
      const height = window.innerHeight - container.getBoundingClientRect().y - 40;

      setContainerSize(`${height}px`);
    }
  }); // no deps, it's better to re-check position on every render

  const {
    isChartRendererReady,
    isQueryLoading,
    resultSetExists,
    queryError,
    isBuildInProgress,
    slowQuery,
    slowQueryFromCache,
  } = useChartRendererState(queryId);
  const { setResultSetExists, setChartRendererReady, setQueryError } =
    useChartRendererStateMethods();

  const runButtonRef = useRef<HTMLButtonElement>(null);

  // for you, ovr :)
  useHotkeys('cmd+enter', () => {
    runButtonRef.current?.click();
  });

  useEffect(() => {
    return () => {
      setChartRendererReady(queryId, false);
    };
    // eslint-disable-next-line
  }, []);

  useEffect(() => {
    if (!areQueriesEqual && queryError) {
      setQueryError(queryId, null);
    }
  }, [queryId, areQueriesEqual, queryError]);

  useEffect(() => {
    setResultSetExists(queryId, false);
  }, [framework]);

  const loading: boolean =
    queryHasMissingMembers || isQueryLoading || isBuildInProgress || !cubejsApi;

  const invisible: boolean =
    !isChartRendererReady ||
    isBuildInProgress ||
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

              {isBuildInProgress && (
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
              loading={!isChartRendererReady || isFetchingMeta}
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
        {cubejsApi ? (
          <iframe
            id={`iframe-${queryId}`}
            data-testid="chart-renderer"
            ref={iframeRef}
            style={{ height: containerSize }}
            title="Chart renderer"
            src={`/chart-renderers/${framework}/index.html#queryId=${queryId}`}
          />
        ) : null}
      </ChartContainer>
    </>
  );
}
