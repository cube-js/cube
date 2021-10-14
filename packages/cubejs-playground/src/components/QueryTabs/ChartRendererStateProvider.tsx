import { createContext, ReactNode, useContext, useState } from 'react';

import { QueryLoadResult } from '../ChartRenderer/ChartRenderer';

type BooleanMap = Record<string, boolean>;

type QueryStatusContextProps = {
  chartRendererState: BooleanMap;
  setChartRendererReady: (queryId: string, isReady: boolean) => void;
  queryStatus: Record<string, QueryLoadResult | null>;
  setQueryStatus: (queryId: string, status: QueryLoadResult | null) => void;
  resultSetExists: BooleanMap;
  setResultSetExists: (queryId: string, exists: boolean) => void;
  isQueryLoading: BooleanMap;
  setQueryLoading: (queryId: string, loading: boolean) => void;
  queryError: Record<string, Error | null>;
  setQueryError: (queryId: string, error: Error | null) => void;
  isBuildInProgress: BooleanMap;
  setBuildInProgress: (queryId: string, inProgress: boolean) => void;

  slowQuery: BooleanMap;
  setSlowQuery: (queryId: string, isSlow: boolean) => void;

  slowQueryFromCache: BooleanMap;
  setSlowQueryFromCache: (queryId: string, isSlow: boolean) => void;

  queryRequestId: Record<string, string>;
  setQueryRequestId: (queryId: string, requestId: string) => void;
};

const ChartRendererStateContext = createContext({} as QueryStatusContextProps);

type ChartRendererStateProviderProps = {
  children: ReactNode;
};

export function ChartRendererStateProvider({
  children,
}: ChartRendererStateProviderProps) {
  const [chartRendererState, setChartRendererStateMap] = useState<BooleanMap>(
    {}
  );
  const [queryStatus, setQueryStatus] = useState<
    Record<string, QueryLoadResult | null>
  >({});
  const [resultSetExists, setResultSetExists] = useState<BooleanMap>({});
  const [isQueryLoading, setQueryLoading] = useState<BooleanMap>({});
  const [queryError, setQueryError] = useState<Record<string, Error | null>>(
    {}
  );
  const [isBuildInProgress, setBuildInProgress] = useState<BooleanMap>({});
  const [slowQuery, setSlowQuery] = useState<BooleanMap>({});
  const [slowQueryFromCache, setSlowQueryFromCache] = useState<BooleanMap>({});
  const [queryRequestId, setQueryRequestId] = useState<Record<string, string>>(
    {}
  );

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

        queryRequestId,
        setQueryRequestId(queryId, requestId) {
          setQueryRequestId((prev) => ({
            ...prev,
            [queryId]: requestId,
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
    queryRequestId,
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
    queryRequestId: queryRequestId[queryId],
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
    setQueryRequestId,
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
    setQueryRequestId,
  };
}
