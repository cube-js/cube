import { ChartType, PivotConfig, Query } from '@cubejs-client/core';
import { Tabs } from 'antd';
import equals from 'fast-deep-equal';
import { ReactNode, useEffect, useLayoutEffect, useState } from 'react';
import styled from 'styled-components';

import { event } from '../../events';
import { useLocalStorage } from '../../hooks';
import { QueryLoadResult } from '../ChartRenderer/ChartRenderer';
import { DrilldownModal } from '../DrilldownModal/DrilldownModal';
import { useChartRendererStateMethods } from './ChartRendererStateProvider';

const { TabPane } = Tabs;

const StyledTabs = styled(Tabs)`
  & .ant-tabs-nav {
    background: #fff;
    padding: 12px 16px 0;
    margin: 0;
  }

  & .ant-tabs-extra-content {
    margin-left: 32px;
  }
`;

type QueryTab = {
  id: string;
  query: Query;
  chartType?: ChartType;
};

type QueryTabs = {
  activeId: string;
  tabs: QueryTab[];
};

export type QueryTabsProps = {
  query: Query | null;
  children: (
    tab: QueryTab,
    saveTab: (tab: Omit<QueryTab, 'id'>) => void
  ) => ReactNode;
  sidebar?: ReactNode | null;
  onTabChange?: (tab: QueryTab) => void;
};

export function QueryTabs({
  query,
  children,
  sidebar = null,
  onTabChange,
}: QueryTabsProps) {
  const {
    setChartRendererReady,
    setQueryStatus,
    setQueryError,
    setResultSetExists,
    setQueryLoading,
    setBuildInProgress,
    setSlowQuery,
    setSlowQueryFromCache,
  } = useChartRendererStateMethods();

  const [ready, setReady] = useState<boolean>(false);
  const [queryTabs, saveTabs] = useLocalStorage<QueryTabs>('queryTabs', {
    activeId: '1',
    tabs: [
      {
        id: '1',
        query: query || {},
      },
    ],
  });

  const [drilldownConfig, setDrilldownConfig] = useState<{
    query?: Query | null;
    pivotConfig?: PivotConfig | null;
  }>({});

  useEffect(() => {
    window['__cubejsPlayground'] = {
      ...window['__cubejsPlayground'],
      forQuery(queryId: string) {
        return {
          onChartRendererReady() {
            setChartRendererReady(queryId, true);
          },
          onQueryStart: () => {
            setQueryLoading(queryId, true);
          },
          onQueryLoad: ({ resultSet, error }: QueryLoadResult) => {
            let isAggregated;

            if (resultSet) {
              const { loadResponse } = resultSet.serialize();
              const {
                external,
                dbType,
                extDbType,
                usedPreAggregations = {},
              } = loadResponse.results[0] || {};

              setSlowQueryFromCache(queryId, Boolean(loadResponse.slowQuery));
              Boolean(loadResponse.slowQuery) && setSlowQuery(queryId, false);
              setResultSetExists(queryId, true);

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

              const response = resultSet.serialize();
              const [result] = response.loadResponse.results;

              const preAggregationType = Object.values(
                result.usedPreAggregations || {}
              )[0]?.type;
              const transformedQuery = result.transformedQuery;

              setQueryStatus(queryId, {
                resultSet,
                error,
                isAggregated,
                preAggregationType,
                transformedQuery,
                extDbType,
                external,
              });
            }

            if (error) {
              setQueryStatus(queryId, null);
              setQueryError(queryId, error);
            }

            if (resultSet || error) {
              setQueryLoading(queryId, false);
            }
          },
          onQueryProgress: (progress) => {
            setBuildInProgress(
              queryId,
              Boolean(progress?.stage?.stage.includes('pre-aggregation'))
            );

            const isQuerySlow =
              progress?.stage?.stage.includes('Executing query') &&
              (progress.stage.timeElapsed || 0) >= 5000;

            setSlowQuery(queryId, isQuerySlow);
            isQuerySlow && setSlowQueryFromCache(queryId, false);
          },
          onQueryDrilldown: (query, pivotConfig) => {
            setDrilldownConfig({
              query,
              pivotConfig,
            });
          },
        };
      },
    };
  }, []);

  useEffect(() => {
    if (ready) {
      return;
    }

    const currentTab = queryTabs.tabs.find(
      (tab) => tab.id === queryTabs.activeId
    );

    if (query && !equals(currentTab?.query, query)) {
      const id = getNextId();

      saveTabs({
        activeId: id,
        tabs: [...queryTabs.tabs, { id, query }],
      });
    }

    setReady(true);
  }, [ready]);

  useEffect(() => {
    if (ready && queryTabs.activeId) {
      const activeTab = queryTabs.tabs.find(
        (tab) => tab.id === queryTabs.activeId
      );
      activeTab && onTabChange?.(activeTab);
    }
  }, [ready, queryTabs.activeId]);

  const { activeId, tabs } = queryTabs;

  function getNextId(): string {
    const ids = tabs.map(({ id }) => id);

    for (let index = 1; index <= tabs.length + 1; index++) {
      if (!ids.includes(index.toString())) {
        return index.toString();
      }
    }

    return (tabs.length + 1).toString();
  }

  function handleTabSave(tab: Omit<QueryTab, 'id'>) {
    saveTabs({
      ...queryTabs,
      tabs: tabs.map((currentTab) => {
        return activeId === currentTab.id
          ? {
              ...currentTab,
              ...tab,
            }
          : currentTab;
      }),
    });
  }

  function setActiveId(activeId: string) {
    saveTabs({ activeId, tabs });
  }

  function handleDrilldownModalClose() {
    setDrilldownConfig({});
  }

  if (!ready || !queryTabs.activeId) {
    return null;
  }

  return (
    <StyledTabs
      data-testid="query-tabs"
      activeKey={activeId}
      type="editable-card"
      tabBarExtraContent={{
        right: sidebar,
      }}
      hideAdd={false}
      onChange={setActiveId}
      onEdit={(event) => {
        if (typeof event === 'string') {
          let closedIndex = Number.MAX_VALUE;
          const nextTabs = tabs.filter(({ id }, index) => {
            if (id === event) {
              closedIndex = index;
            }
            return id !== event;
          });

          saveTabs({
            activeId: nextTabs[Math.min(closedIndex, nextTabs.length - 1)].id,
            tabs: nextTabs,
          });
        } else {
          const nextId = getNextId();

          saveTabs({
            activeId: nextId,
            tabs: [
              ...tabs,
              {
                id: nextId,
                query: {},
              },
            ],
          });
        }
      }}
    >
      {tabs.map((tab) => (
        <TabPane
          key={tab.id}
          data-testid={`query-tab-${tab.id}`}
          tab={`Query ${tab.id}`}
          closable={tabs.length > 1}
        >
          {children(tab, handleTabSave)}
          {drilldownConfig.query ? (
            <DrilldownModal
              query={drilldownConfig.query}
              pivotConfig={drilldownConfig.pivotConfig}
              onClose={handleDrilldownModalClose}
            />
          ) : null}
        </TabPane>
      ))}
    </StyledTabs>
  );
}
