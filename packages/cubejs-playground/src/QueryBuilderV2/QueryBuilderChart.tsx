import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Button,
  Dialog,
  DialogTrigger,
  Divider,
  Header,
  Radio,
  Skeleton,
  Space,
  Title,
} from '@cube-dev/ui-kit';
import {
  AreaChartOutlined,
  BarChartOutlined,
  CodeOutlined,
  LineChartOutlined,
  LoadingOutlined,
  TableOutlined,
} from '@ant-design/icons';
import { ChartType } from '@cubejs-client/core';

import { useLocalStorage } from './hooks';
import { useQueryBuilderContext } from './context';
import { PivotAxes, PivotOptions } from './Pivot';
import { ChevronIcon } from './icons/ChevronIcon';
import { AccordionCard } from './components/AccordionCard';
import { OutdatedLabel } from './components/OutdatedLabel';
import { QueryBuilderChartResults } from './QueryBuilderChartResults';

const CHART_HEIGHT = 400;
const MAX_SERIES_LIMIT = 25;

interface QueryBuilderChartProps {
  maxHeight?: number;
  onToggle?: (isExpanded: boolean) => void;
}

const ALLOWED_CHART_TYPES = ['table', 'line', 'bar', 'area'];

export function QueryBuilderChart(props: QueryBuilderChartProps) {
  const [isVizardLoaded, setIsVizardLoaded] = useState(false);
  const [isExpanded, setIsExpanded] = useLocalStorage('QueryBuilder:Chart:expanded', false);
  const { maxHeight = CHART_HEIGHT, onToggle } = props;
  let {
    query,
    isLoading,
    chartType,
    setChartType,
    pivotConfig,
    updatePivotConfig,
    resultSet,
    apiToken,
    apiUrl,
    isResultOutdated,
    VizardComponent,
  } = useQueryBuilderContext();
  const containerRef = useRef<HTMLDivElement>(null);

  if (!ALLOWED_CHART_TYPES.includes(chartType || '')) {
    chartType = 'line';
  }

  useEffect(() => {
    const element = containerRef.current;

    if (!element) {
      return;
    }

    const onScroll = () => {
      if (chartType !== 'table') {
        element.scrollTop = 0;

        setTimeout(() => {
          element.scrollTop = 0;
        });
      }
    };

    element.addEventListener('scroll', onScroll);

    return () => {
      element.removeEventListener('scroll', onScroll);
    };
  }, [containerRef.current]);

  const chart = useMemo(
    () => (
      <QueryBuilderChartResults
        resultSet={resultSet}
        isLoading={isLoading}
        query={query}
        pivotConfig={pivotConfig}
        chartType={chartType}
        isExpanded={isExpanded}
        containerRef={containerRef}
      />
    ),
    [resultSet, chartType, isLoading, pivotConfig, isExpanded]
  );

  const onMove = useCallback(
    (arg) => {
      return updatePivotConfig.moveItem(arg);
    },
    [updatePivotConfig]
  );

  const onUpdate = useCallback(
    (arg) => {
      return updatePivotConfig.update(arg);
    },
    [updatePivotConfig]
  );

  const pivotConfigurator = useMemo(() => {
    return pivotConfig ? (
      <DialogTrigger type="popover">
        <Button size="small" rightIcon={<ChevronIcon direction="bottom" />}>
          Pivot
        </Button>
        <Dialog border overflow="hidden" width="40x max-content 80x">
          <PivotAxes pivotConfig={pivotConfig} onMove={onMove} />
          <Divider />
          <div style={{ padding: '8px' }}>
            <PivotOptions pivotConfig={pivotConfig} onUpdate={onUpdate} />
          </div>
        </Dialog>
      </DialogTrigger>
    ) : undefined;
  }, [pivotConfig, onMove, onUpdate]);

  return (
    <AccordionCard
      qa="QueryBuilderChart"
      isExpanded={isExpanded}
      title="Chart"
      subtitle={
        isLoading ? (
          isExpanded ? (
            <LoadingOutlined />
          ) : undefined
        ) : isResultOutdated ? (
          <OutdatedLabel />
        ) : undefined
      }
      extra={
        isExpanded ? (
          <Space>
            <Radio.ButtonGroup
              qa="ChartType"
              aria-label="Type"
              labelPosition="side"
              value={chartType}
              onChange={async (val) => {
                setChartType(val as ChartType);
              }}
            >
              <Radio.Button qa="LineChartType" value="line">
                <Space gap=".5x">
                  <LineChartOutlined style={{ fontSize: 'var(--icon-size)' }} />
                  <span>Line</span>
                </Space>
              </Radio.Button>
              <Radio.Button qa="BarChartType" value="bar">
                <Space gap=".5x">
                  <BarChartOutlined style={{ fontSize: 'var(--icon-size)' }} />
                  <span>Bar</span>
                </Space>
              </Radio.Button>
              <Radio.Button qa="AreaChartType" value="area">
                <Space gap=".5x">
                  <AreaChartOutlined style={{ fontSize: 'var(--icon-size)' }} />
                  <span>Area</span>
                </Space>
              </Radio.Button>
              <Radio.Button qa="TableChartType" value="table">
                <Space gap=".5x">
                  <TableOutlined style={{ fontSize: 'var(--icon-size)' }} />
                  <span>Table</span>
                </Space>
              </Radio.Button>
            </Radio.ButtonGroup>
            {pivotConfigurator}
            {VizardComponent ? (
              <DialogTrigger isDismissable type="fullscreen">
                <Button
                  type="primary"
                  size="small"
                  icon={<CodeOutlined />}
                  onPress={() => setIsVizardLoaded(true)}
                >
                  Code
                </Button>
                {/*<TooltipProvider title="Get a code example that visualize your data using a charting library of your choice.">*/}
                {/*</TooltipProvider>*/}
                <Dialog isDismissable>
                  <Header>
                    <Title>Chart Prototyping</Title>
                  </Header>
                  {isVizardLoaded ? (
                    <VizardComponent
                      apiToken={apiToken}
                      apiUrl={apiUrl}
                      query={query}
                      pivotConfig={pivotConfig}
                    />
                  ) : null}
                </Dialog>
              </DialogTrigger>
            ) : null}
          </Space>
        ) : (
          <div style={{ height: '32px' }} />
        )
      }
      onToggle={(open) => {
        setIsExpanded(open);
        onToggle?.(open);
      }}
    >
      <>
        {isLoading ? <Skeleton height={400} layout="chart" padding="0 1x 1x 1x" /> : undefined}
        {chart}
      </>
    </AccordionCard>
  );
}
