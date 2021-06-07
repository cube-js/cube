import { ReactNode, useState } from 'react';
import { ChartType, Query } from '@cubejs-client/core';
import { PlusOutlined } from '@ant-design/icons';

import { useLocalStorage } from '../../hooks';
import styled from 'styled-components';
import RemoveButtonGroup from '../../QueryBuilder/RemoveButtonGroup';
import { Button } from '../../atoms';

const Wrapper = styled.div`
  display: flex;
  align-items: center;
  
  & > div {
    margin-right: 16px;
  }
`;

type QueryTab = {
  id: number;
  query: Query;
  chartType?: ChartType;
};

type QueryTabsProps = {
  children: (
    tab: QueryTab,
    saveTab: (tab: Omit<QueryTab, 'id'>) => void
  ) => ReactNode;
  onTabChange?: (tab: QueryTab) => void;
};

export function QueryTabs({ children, onTabChange }: QueryTabsProps) {
  const [activeId, setActiveId] = useState<number>(1);

  const [tabs, saveTabs] = useLocalStorage<QueryTab[]>('queryTabs', [
    {
      id: 1,
      query: {},
    },
    {
      id: 2,
      query: {
        measures: ['Sales.count'],
        dimensions: ['Sales.status'],
      },
    },
  ]);

  function getNextId(): number {
    const ids = tabs.map(({ id }) => id);

    for (let index = 1; index <= tabs.length + 1; index++) {
      if (!ids.includes(index)) {
        return index;
      }
    }

    return tabs.length + 1;
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

  const activeTab = tabs.find(({ id }) => id === activeId) as QueryTab;

  return (
    <>
      <Wrapper>
        {tabs.map((item) => {
          return (
            <RemoveButtonGroup
              key={item.id.toString()}
              onRemoveClick={() => {
                saveTabs(tabs.filter(({ id }) => id !== item.id));
              }}
            >
              <Button
                onClick={() => {
                  setActiveId(item.id);
                  // onTabChange(activeTab);
                }}
              >
                Query {item.id}
              </Button>
            </RemoveButtonGroup>
          );
        })}

        <Button
          onClick={() => {
            const nextId = getNextId();

            saveTabs([
              ...tabs,
              {
                id: nextId,
                query: {},
              },
            ]);

            setActiveId(nextId);
          }}
        >
          <PlusOutlined />
        </Button>
      </Wrapper>

      <div key={activeId.toString()}>{children(activeTab, handleTabSave)}</div>
    </>
  );
}
