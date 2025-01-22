import { Flex, Flow, Panel, tasty } from '@cube-dev/ui-kit';
import { memo, useEffect, useMemo, useRef, useState } from 'react';

import { QUERY_BUILDER_COLOR_TOKENS } from './color-tokens';
import { useAutoSize, useEvent, useLocalStorage } from './hooks';
import { useQueryBuilderContext } from './context';
import { Tabs, Tab } from './components/Tabs';
import { QueryBuilderFilters } from './QueryBuilderFilters';
import { QueryBuilderChart } from './QueryBuilderChart';
import { QueryBuilderResults } from './QueryBuilderResults';
import { QueryBuilderToolBar } from './QueryBuilderToolBar';
import { QueryBuilderGeneratedSQL } from './QueryBuilderGeneratedSQL';
import { QueryBuilderSQL } from './QueryBuilderSQL';
import { QueryBuilderRest } from './QueryBuilderRest';
import { QueryBuilderGraphQL } from './QueryBuilderGraphQL';
import { QueryBuilderSidePanel } from './QueryBuilderSidePanel';
import { QueryBuilderExtras } from './QueryBuilderExtras';

// The minimum size of the area below the top edge of the chart
// when we can show both results and the chart at the same time.
const CHART_THRESHOLD = 448;

const Divider = tasty({
  styles: {
    width: '100%',
    height: '1ow 1ow',
    fill: '#border',
  },
});

type Tab = 'results' | 'generated-sql' | 'json' | 'graphql' | 'sql';

const QueryBuilderPanel = tasty(Panel, {
  isFlex: true,
  isStretched: true,
  qa: 'QueryBuilder',
  styles: {
    fill: '#white',

    ...QUERY_BUILDER_COLOR_TOKENS,
  },
});

const QueryBuilderInternals = memo(function QueryBuilderInternals() {
  const { error, resultSet, queryHash, dateRanges } = useQueryBuilderContext();
  const [isChartExpanded, setIsChartExpanded] = useLocalStorage(
    'QueryBuilder:Chart:expanded',
    false
  );
  const [tab, setTab] = useState<Tab>('results');
  const ref = useRef<HTMLDivElement>(null);
  const chartRef = useRef<HTMLDivElement>(null);
  const [isFiltersExpanded, setIsFiltersExpanded] = useState(true);
  const [chartSize, updateChartSize] = useAutoSize(chartRef, 0);

  const ResultsAndSQL = useMemo(() => {
    return (
      <>
        <Divider />

        <Tabs
          activeKey={tab}
          extra={<QueryBuilderExtras />}
          styles={{ padding: '0 1x' }}
          onChange={(tab: string) => setTab(tab as Tab)}
        >
          <Tab id="results" title="Results" />
          <Tab id="generated-sql" title="Generated SQL" />
          <Tab id="sql" title="SQL API" />
          <Tab id="json" title="REST API" />
          <Tab id="graphql" title="GraphQL API" />
        </Tabs>
        {tab === 'results' && <QueryBuilderResults forceMinHeight={!isChartExpanded} />}
        {tab === 'generated-sql' && <QueryBuilderGeneratedSQL />}
        {tab === 'json' && <QueryBuilderRest />}
        {tab === 'sql' && <QueryBuilderSQL />}
        {tab === 'graphql' && <QueryBuilderGraphQL />}
      </>
    );
  }, [tab, isChartExpanded]);

  const onToggle = useEvent((isExpanded: boolean) => {
    setIsFiltersExpanded(isExpanded);
  });

  useEffect(() => {
    updateChartSize();

    setTimeout(() => {
      updateChartSize();
    }, 200);
  }, [isChartExpanded, isFiltersExpanded, error, queryHash, dateRanges.list.length, resultSet]);

  return (
    <QueryBuilderPanel>
      <QueryBuilderSidePanel />

      <Panel ref={ref} gridRows="min-content min-content minmax(0, 1fr)">
        {useMemo(
          () => (
            <>
              <QueryBuilderToolBar />
              <Divider />
            </>
          ),
          []
        )}

        <Panel gridRows="min-content min-content min-content min-content min-content minmax(0, 1fr)">
          {useMemo(
            () => (
              <>
                <QueryBuilderFilters onToggle={onToggle} />

                <Divider />
              </>
            ),
            []
          )}

          {useMemo(() => {
            return (
              <>
                <div ref={chartRef}>
                  <QueryBuilderChart onToggle={setIsChartExpanded} />
                </div>
                {!isChartExpanded || chartSize > CHART_THRESHOLD ? (
                  ResultsAndSQL
                ) : (
                  <Flow>
                    <Divider />
                    <Flex padding=".5x" placeContent="end">
                      <QueryBuilderExtras />
                    </Flex>
                  </Flow>
                )}
              </>
            );
          }, [isChartExpanded, chartSize, ResultsAndSQL])}
        </Panel>
      </Panel>
    </QueryBuilderPanel>
  );
});

export { QueryBuilderInternals };
