import { Tabs } from 'antd';
import { ReactNode, useState } from 'react';
import { ChartType, Query } from '@cubejs-client/core';
import styled from 'styled-components';

import { useLocalStorage } from '../../hooks';
import { useHistory } from 'react-router';

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

type QueryTabsProps = {
  query: Query;
  children: (
    tab: QueryTab,
    saveTab: (tab: Omit<QueryTab, 'id'>) => void
  ) => ReactNode;
  sidebar?: ReactNode | null;
};

export function QueryTabs({ query, children, sidebar = null }: QueryTabsProps) {
  const [tabs, saveTabs] = useLocalStorage<QueryTab[]>('queryTabs', [
    {
      id: '1',
      query
    },
  ]);
  const [activeId, setActiveId] = useState<string>(tabs[0].id);

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
    saveTabs(
      tabs.map((currentTab) => {
        return activeId === currentTab.id
          ? {
              ...currentTab,
              ...tab,
            }
          : currentTab;
      })
    );
  }

  return (
    <StyledTabs
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

          saveTabs(nextTabs);
          setActiveId(nextTabs[Math.min(closedIndex, nextTabs.length - 1)].id);
        } else {
          const nextId = getNextId();

          saveTabs([
            ...tabs,
            {
              id: nextId,
              query: {},
            },
          ]);

          setActiveId(nextId);
        }
      }}
    >
      {tabs.map((tab) => (
        <TabPane
          key={tab.id}
          tab={`Query ${tab.id}`}
          closable={tabs.length > 1}
        >
          {children(tab, handleTabSave)}
        </TabPane>
      ))}
    </StyledTabs>
  );
}
