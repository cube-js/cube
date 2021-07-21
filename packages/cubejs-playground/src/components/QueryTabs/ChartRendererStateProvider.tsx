import { createContext, ReactNode, useContext, useState } from 'react';

import { QueryLoadResult } from '../ChartRenderer/ChartRenderer';

type QueryStatusContextProps = {
  chartRendererState: Record<string, boolean>;
  setChartRendererReady: (queryId: string, isReady: boolean) => void;
  queryStatus: Record<string, QueryLoadResult | null>;
  setQueryStatus: (queryId: string, status: QueryLoadResult | null) => void;
  resultSetExists: Record<string, boolean>;
  setResultSetExists: (queryId: string, exists: boolean) => void;
  isQueryLoading: Record<string, boolean>;
  setQueryLoading: (queryId: string, loading: boolean) => void;
  queryError: Record<string, Error | null>;
  setQueryError: (queryId: string, error: Error | null) => void;
  isBuildInProgress: Record<string, boolean>;
  setBuildInProgress: (queryId: string, inProgress: boolean) => void;

  slowQuery: Record<string, boolean>;
  setSlowQuery: (queryId: string, isSlow: boolean) => void;

  slowQueryFromCache: Record<string, boolean>;
  setSlowQueryFromCache: (queryId: string, isSlow: boolean) => void;
};

const ChartRendererStateContext = createContext({} as QueryStatusContextProps);

type ChartRendererStateProviderProps = {
  children: ReactNode;
};

export function ChartRendererStateProvider({
  children,
}: ChartRendererStateProviderProps) {
  const [chartRendererState, setChartRendererStateMap] = useState<
    Record<string, boolean>
  >({});
  const [queryStatus, setQueryStatus] = useState<
    Record<string, QueryLoadResult | null>
  >({});
  const [resultSetExists, setResultSetExists] = useState<
    Record<string, boolean>
  >({});
  const [isQueryLoading, setQueryLoading] = useState<Record<string, boolean>>(
    {}
  );
  const [queryError, setQueryError] = useState<Record<string, Error | null>>(
    {}
  );
  const [isBuildInProgress, setBuildInProgress] = useState<
    Record<string, boolean>
  >({});
  const [slowQuery, setSlowQuery] = useState<Record<string, boolean>>({});
  const [slowQueryFromCache, setSlowQueryFromCache] = useState<
    Record<string, boolean>
  >({});

  return (
    <ChartRendererStateContext.Provider
      value={{
        chartRendererState,
        setChartRendererReady(queryId, isReady) {
          setChartRendererStateMap((prev) => ({
            ...prev,
            [queryId]: isReady,
          }));
        },
        queryStatus,
        setQueryStatus(queryId, status) {
          setQueryStatus((prev) => ({
            ...prev,
            [queryId]: status,
          }));
        },
        resultSetExists,
        setResultSetExists(queryId, exists) {
          setResultSetExists((prev) => ({
            ...prev,
            [queryId]: exists,
          }));
        },
        isQueryLoading,
        setQueryLoading(queryId, exists) {
          setQueryLoading((prev) => ({
            ...prev,
            [queryId]: exists,
          }));
        },
        queryError,
        setQueryError(queryId, error) {
          setQueryError((prev) => ({
            ...prev,
            [queryId]: error,
          }));
        },
        isBuildInProgress,
        setBuildInProgress(queryId, inProgress) {
          setBuildInProgress((prev) => ({
            ...prev,
            [queryId]: inProgress,
          }));
        },

        slowQuery,
        setSlowQuery(queryId, isSlow) {
          setSlowQuery((prev) => ({
            ...prev,
            [queryId]: isSlow,
          }));
        },

        slowQueryFromCache,
        setSlowQueryFromCache(queryId, isSlow) {
          setSlowQueryFromCache((prev) => ({
            ...prev,
            [queryId]: isSlow,
          }));
        },
      }}
    >
      {children}
    </ChartRendererStateContext.Provider>
  );
}

export function useChartRendererState(queryId: string) {
  const {
    chartRendererState,
    queryStatus,
    resultSetExists,
    isQueryLoading,
    queryError,
    isBuildInProgress,
    slowQuery,
    slowQueryFromCache,
  } = useContext(ChartRendererStateContext);

  return {
    isChartRendererReady: Boolean(chartRendererState[queryId]),
    queryStatus: queryStatus[queryId],
    resultSetExists: resultSetExists[queryId],
    isQueryLoading: Boolean(isQueryLoading[queryId]),
    queryError: queryError[queryId] || null,
    isBuildInProgress: Boolean(isBuildInProgress[queryId]),
    slowQuery: Boolean(slowQuery[queryId]),
    slowQueryFromCache: Boolean(slowQueryFromCache[queryId]),
  };
}

export function useChartRendererStateMethods() {
  const {
    setChartRendererReady,
    setQueryStatus,
    setResultSetExists,
    setQueryLoading,
    setQueryError,
    setBuildInProgress,
    setSlowQuery,
    setSlowQueryFromCache,
  } = useContext(ChartRendererStateContext);

  return {
    setChartRendererReady,
    setQueryStatus,
    setResultSetExists,
    setQueryLoading,
    setQueryError,
    setBuildInProgress,
    setSlowQuery,
    setSlowQueryFromCache,
  };
}
