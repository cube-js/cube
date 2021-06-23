import { Tabs } from 'antd';
import { ReactNode, useEffect } from 'react';
import { ChartType, Query } from '@cubejs-client/core';
import styled from 'styled-components';

import { useLocalStorage } from '../../hooks';

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

type QueryTabsProps = {
  query: Query;
  children: (
    tab: QueryTab,
    saveTab: (tab: Omit<QueryTab, 'id'>) => void
  ) => ReactNode;
  sidebar?: ReactNode | null;
};

export function QueryTabs({ query, children, sidebar = null }: QueryTabsProps) {
  const [queryTabs, saveTabs] = useLocalStorage<QueryTabs>('queryTabs', {
    activeId: '1',
    tabs: [
      {
        id: '1',
        query,
      },
    ],
  });

  // tmp transition to new format
  useEffect(() => {
    if (!queryTabs.activeId && (queryTabs as any).length > 0) {
      saveTabs({
        activeId: queryTabs[0].id,
        tabs: queryTabs as any,
      });
    }
  }, [queryTabs]);

  if (!queryTabs.activeId) {
    return null;
  }

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
        </TabPane>
      ))}
    </StyledTabs>
  );
}
