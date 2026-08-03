import {
  ChartType,
  PivotConfig,
  Query,
  validateQuery,
} from '@cubejs-client/core';
import { Input, Tabs } from 'antd';
import equals from 'fast-deep-equal';
import { ReactNode, useEffect, useState } from 'react';
import styled from 'styled-components';

import { event } from '../../events';
import { useLocalStorage } from '../../hooks';
import { QueryLoadResult } from '../ChartRenderer/ChartRenderer';
import { DrilldownModal } from '../DrilldownModal/DrilldownModal';
import { useChartRendererStateMethods } from './ChartRendererStateProvider';

const { TabPane } = Tabs;

export const StyledTabs = styled(Tabs)`
  margin-top: 0;
  display: grid;
  max-width: 100%;
  grid-template-rows: min-content 1fr;
  overflow: hidden;

  & .ant-tabs-nav {
    padding: 0;
    margin: 0;
    overflow: hidden;
    background-color: white;
  }

  & .ant-tabs-nav-wrap {
    padding: 8px 8px 0;
  }

  & .ant-tabs-extra-content {
    padding: 8px;
    place-self: start;
  }

  & .ant-tabs-tab {
    margin: 0 24px 0 0;
  }

  & .ant-tabs-content-holder {
    position: relative;
    display: flex;
  }

  & .ant-tabs-tab {
    border-radius: var(--radius) var(--radius) 0 0 !important;
  }
`;


type QueryTab = {
  id: string;
  query: Query;
  chartType?: ChartType;
  name?: string;
};

type QueryTabs = {
  activeId: string;
  tabs: QueryTab[];
};

type DrillDownConfig = {
  query?: Query | null;
  pivotConfig?: PivotConfig | null;
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
    setQueryRequestId,
  } = useChartRendererStateMethods();

  const [editableTabId, setEditableTabId] = useState<string>();
  const [editableTabValue, setEditableTabValue] = useState<string>('');
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

  const [drilldownConfig, setDrilldownConfig] = useState<DrillDownConfig>({});

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
                requestId,
                external,
                dbType,
                extDbType,
                usedPreAggregations = {},
              } = loadResponse.results[0] || {};

              if (requestId) {
                setQueryRequestId(queryId, requestId);
              }

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

    if (
      query &&
      !equals(validateQuery(currentTab?.query), validateQuery(query))
    ) {
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

  function setTabName(tabId: string, name: string) {
    saveTabs({
      ...queryTabs,
      tabs: tabs.map((currentTab) => {
        return tabId === currentTab.id
          ? {
              ...currentTab,
              name
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
          closable={tabs.length > 1}
          tab={
            editableTabId === tab.id ? (
              <Input
                autoFocus
                size="small"
                value={editableTabValue}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') {
                    setEditableTabId(undefined);

                    if (editableTabValue.trim()) {
                      setTabName(tab.id, editableTabValue.trim());
                      setEditableTabValue('');
                    }
                  }
                  e.stopPropagation();
                }}
                onChange={(e) => setEditableTabValue(e.target.value)}
                onBlur={() => {
                  if (editableTabValue.trim()) {
                    setTabName(tab.id, editableTabValue.trim());
                    setEditableTabValue('');
                  }
                  setEditableTabId(undefined);
                }}
              />
            ) : (
              <span
                style={{ userSelect: 'none' }}
                onDoubleClick={() => {
                  setEditableTabValue(tab.name || `Query ${tab.id}`);
                  setEditableTabId(tab.id);
                }}
              >
                {tab.name ? tab.name : `Query ${tab.id}`}
              </span>
            )
          }
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
