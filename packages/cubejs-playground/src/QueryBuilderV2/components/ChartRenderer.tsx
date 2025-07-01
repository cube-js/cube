import {
  ChartType,
  TimeDimensionGranularity,
  granularityFor,
  minGranularityForIntervals,
  isPredefinedGranularity,
} from '@cubejs-client/core';
import { UseCubeQueryResult } from '@cubejs-client/react';
import { Skeleton, Tag, tasty } from '@cube-dev/ui-kit';
import { ComponentType, memo, useCallback, useMemo } from 'react';
import { Col, Row, Statistic, Table } from 'antd';
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Line,
  LineChart,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import styled from 'styled-components';

import {
  CHART_COLORS,
  getChartColorByIndex,
  getChartSolidColorByIndex,
} from '../utils/chart-colors';
import { formatDateByGranularity, formatDateByPattern } from '../utils/index';

import { LocalError } from './LocalError';

function CustomDot(props: any) {
  const { cx, cy, fill } = props;

  return (
    <svg x={cx - 5} y={cy - 5} width={20} height={20} fill={fill}>
      <circle cx={5} cy={5} r={2.5} />
    </svg>
  );
}

const intlNumberFormatter = Intl.NumberFormat('en', { notation: 'compact' });
const numberFormatter = (item: any) =>
  typeof item === 'number' ? intlNumberFormatter.format(item) : item;

const StyledStatistic = styled(Statistic)`
  .ant-statistic-content {
    font-size: 40px;
  }
`;

const LegendTextElement = tasty({
  as: 'span',
  styles: {
    color: '#dark',
    preset: 't3',
  },
});

function isValidISOTimestamp(timestamp: string) {
  try {
    return new Date(timestamp + 'Z').toISOString() === timestamp + 'Z';
  } catch (e: any) {
    return false;
  }
}

function CartesianChart({
  dataTransformer,
  pivotConfig,
  dateFormat,
  resultSet,
  children,
  ChartComponent,
  height,
  domain,
  grid,
  syncId,
  yAxisFormatter = numberFormatter,
  tooltipFormatter = numberFormatter,
  tooltipCursor = false,
  extra,
}: any) {
  const legendFormatter = useCallback(
    (value) => <LegendTextElement>{value}</LegendTextElement>,
    []
  );

  const granularityField = Object.keys(resultSet?.loadResponse.results[0].data[0] || {}).find(
    (key) => {
      return (key as string).split('.').length === 3;
    }
  ) as string;
  let granularity = granularityField?.split('.')[2];

  if (!isPredefinedGranularity(granularity)) {
    const granularityInfo =
      resultSet?.loadResponse.results[0]?.annotation.timeDimensions[granularityField]?.granularity;
    if (granularityInfo) {
      granularity = minGranularityForIntervals(
        granularityInfo.interval,
        granularityInfo.offset || granularityFor(granularityInfo.origin)
      );
    }
  }

  const formatDate = useMemo(() => {
    if (dateFormat) {
      return (item: string) =>
        isValidISOTimestamp(item) ? formatDateByPattern(new Date(item), dateFormat) : item;
    }

    return granularity
      ? (item: string) =>
          isValidISOTimestamp(item)
            ? formatDateByGranularity(new Date(item), granularity as TimeDimensionGranularity)
            : item
      : (item: string) => item;
  }, [dateFormat]);

  const dateFormatter = useCallback(
    (item) => {
      try {
        return formatDate(item);
      } catch (e: any) {
        return item;
      }
    },
    [formatDate]
  );

  const chartPivot = useMemo(() => {
    let chartPivot = resultSet.chartPivot(pivotConfig);
    if (dataTransformer) {
      chartPivot = dataTransformer(chartPivot, { granularity });
    }

    return chartPivot.map((series: any) => {
      series.x = series.xValues
        .map((value: string) => {
          return formatDate(value);
        })
        .join(',');

      return series;
    });
  }, [resultSet, pivotConfig, dataTransformer, formatDate]);

  return (
    <ResponsiveContainer width="100%" height={height}>
      <ChartComponent margin={{ left: -10, top: 10 }} data={chartPivot} syncId={syncId}>
        <XAxis
          axisLine={false}
          tickLine={false}
          tickFormatter={dateFormatter}
          dataKey="x"
          minTickGap={20}
        />

        <YAxis
          domain={domain}
          axisLine={false}
          tickLine={false}
          tickCount={domain ? 5 : undefined}
          tickFormatter={yAxisFormatter}
          scale={domain ? 'linear' : undefined}
          width={80}
        />
        <CartesianGrid
          vertical={grid === 'vertical' || grid === 'both'}
          horizontal={grid === 'horizontal' || grid === 'both'}
          stroke="#f5f5f5"
        />
        {children}
        <Legend formatter={legendFormatter} iconType="square" iconSize={10} />
        <Tooltip
          labelStyle={{
            fontSize: 'var(--h6-font-size)',
            lineHeight: 'var(--h6-line-height)',
            fontWeight: 600,
            color: 'var(--dark-color)',
          }}
          itemStyle={{
            fontSize: 'var(--t4-font-size)',
            lineHeight: 'var(--t4-line-height)',
            fontWeight: 600,
            padding: 0,
          }}
          labelFormatter={dateFormatter}
          formatter={tooltipFormatter}
          cursor={tooltipCursor}
        />
        {extra ? extra() : null}
      </ChartComponent>
    </ResponsiveContainer>
  );
}

const TypeToChartComponent = {
  line: ({
    resultSet,
    height,
    fill,
    stroke,
    dot,
    grid,
    domain,
    nameTransform,
    yAxisFormatter,
    tooltipFormatter,
    pivotConfig,
    dateFormat,
    dataTransformer,
    syncId,
    tooltipCursor,
    extra,
  }: any) => {
    let seriesNames = resultSet.seriesNames(pivotConfig);

    if (nameTransform) {
      nameTransform(seriesNames);
    }

    return (
      <CartesianChart
        dataTransformer={dataTransformer}
        pivotConfig={pivotConfig}
        dateFormat={dateFormat}
        resultSet={resultSet}
        height={height}
        domain={domain}
        syncId={syncId}
        grid={grid ?? 'horizontal'}
        ChartComponent={LineChart}
        yAxisFormatter={yAxisFormatter}
        tooltipFormatter={tooltipFormatter}
        tooltipCursor={tooltipCursor}
        extra={extra}
      >
        {seriesNames.map((series: any, i: number) => (
          <Line
            key={series.key}
            type="monotone"
            dataKey={series.key}
            name={series.title}
            dot={dot ?? CustomDot}
            stroke={stroke?.[series.shortTitle] ?? stroke?.[i] ?? getChartColorByIndex(i)}
            fill={fill?.[series.shortTitle] ?? fill?.[i] ?? getChartColorByIndex(i)}
            animationDuration={150}
          />
        ))}
      </CartesianChart>
    );
  },

  bar: ({
    resultSet,
    domain,
    nameTransform,
    height,
    fill,
    grid,
    yAxisFormatter,
    tooltipFormatter,
    pivotConfig,
    dateFormat,
    dataTransformer,
    syncId,
    tooltipCursor,
  }: any) => {
    let seriesNames = resultSet.seriesNames(pivotConfig);

    if (nameTransform) {
      nameTransform(seriesNames);
    }

    return (
      <CartesianChart
        dataTransformer={dataTransformer}
        pivotConfig={pivotConfig}
        resultSet={resultSet}
        height={height}
        domain={domain}
        syncId={syncId}
        grid={grid ?? 'horizontal'}
        dateFormat={dateFormat}
        ChartComponent={BarChart}
        yAxisFormatter={yAxisFormatter}
        tooltipFormatter={tooltipFormatter}
        tooltipCursor={tooltipCursor}
      >
        {seriesNames.map((series: any, i: number) => (
          <Bar
            key={series.key}
            stackId="a"
            dataKey={series.key}
            name={series.title}
            fill={fill?.[series.shortTitle] ?? fill?.[i] ?? getChartSolidColorByIndex(i)}
            animationDuration={150}
          />
        ))}
      </CartesianChart>
    );
  },

  area: ({
    resultSet,
    domain,
    nameTransform,
    height,
    stroke,
    fill,
    grid,
    pivotConfig,
    yAxisFormatter,
    tooltipFormatter,
    dateFormat,
    syncId,
    tooltipCursor,
    dataTransformer,
  }: any) => {
    let seriesNames = resultSet.seriesNames(pivotConfig);

    if (nameTransform) {
      nameTransform(seriesNames);
    }

    return (
      <CartesianChart
        dataTransformer={dataTransformer}
        pivotConfig={pivotConfig}
        resultSet={resultSet}
        height={height}
        domain={domain}
        grid={grid ?? 'horizontal'}
        syncId={syncId}
        dateFormat={dateFormat}
        ChartComponent={AreaChart}
        yAxisFormatter={yAxisFormatter}
        tooltipFormatter={tooltipFormatter}
        tooltipCursor={tooltipCursor}
      >
        {seriesNames.map((series: any, i: number) => (
          <Area
            key={series.key}
            stackId="a"
            dataKey={series.key}
            name={series.title}
            fillOpacity={0.9}
            stroke={stroke?.[series.shortTitle] ?? stroke?.[i] ?? getChartSolidColorByIndex(i)}
            fill={fill?.[series.shortTitle] ?? fill?.[i] ?? getChartSolidColorByIndex(i)}
            strokeWidth={0}
            animationDuration={150}
            type="monotone"
          />
        ))}
      </CartesianChart>
    );
  },

  pie: ({ resultSet, nameTransform, pivotConfig, height, fill, stroke }: any) => {
    let seriesNames = resultSet.seriesNames(pivotConfig);

    if (nameTransform) {
      nameTransform(seriesNames);
    }

    return (
      <ResponsiveContainer width="100%" height={height}>
        <PieChart>
          <Pie
            isAnimationActive={false}
            data={resultSet.chartPivot()}
            nameKey="x"
            dataKey={seriesNames[0].key}
            fill="#8884d8"
          >
            {resultSet.chartPivot(pivotConfig).map((e: any, index: number) => {
              const i = index % (stroke?.length ?? CHART_COLORS.length);

              return (
                <Cell
                  key={index}
                  stroke={stroke?.[i] ?? getChartSolidColorByIndex(i)}
                  fill={fill?.[i] ?? getChartSolidColorByIndex(i)}
                />
              );
            })}
          </Pie>
          <Legend />
          <Tooltip />
        </PieChart>
      </ResponsiveContainer>
    );
  },

  table: ({ isLoading, resultSet, height, pivotConfig }: any) => {
    const columnData = resultSet?.tableColumns(pivotConfig);
    const dataSet = resultSet?.tablePivot(pivotConfig);
    const granularityMap: Record<string, string | undefined> = {};

    let headerSize = 1;

    Object.keys(dataSet[0] || {}).forEach((key) => {
      const size = (key as string).split(',').length;

      if (size > headerSize) {
        headerSize = size;
      }
    });

    columnData.forEach((field: any, i: number) => {
      if (field.key && typeof field.key === 'string') {
        granularityMap[field.key] = field.key.split('.')[2];
      } else {
        field.key = `key${i}`; // fallback index
      }
    });

    return (
      <Table
        loading={isLoading}
        tableLayout="auto"
        locale={{
          emptyText:
            'There is no data for the selected time interval and filters. Try updating the filters above',
        }}
        pagination={false}
        columns={columnData.map((c: any) => {
          const column = { ...c, dataIndex: c.key, title: c.shortTitle };
          const granularity = granularityMap[c.key];

          return {
            width: 90,
            ...column,
            render: granularity
              ? (text: any) => {
                  try {
                    return isValidISOTimestamp(text)
                      ? formatDateByGranularity(
                          new Date(text),
                          granularity as TimeDimensionGranularity
                        )
                      : text;
                  } catch (e: any) {
                    return text;
                  }
                }
              : (text: any) => {
                  switch (typeof text) {
                    case 'boolean':
                      return text ? 'true' : 'false';
                    case 'undefined':
                    case 'object':
                      return text === null ? <Tag>NULL</Tag> : <Tag>OBJECT</Tag>;
                    default:
                      if (c.type === 'boolean') {
                        return text && text !== '0' ? 'true' : 'false';
                      }

                      return text;
                  }
                },
          };
        })}
        dataSource={dataSet}
        scroll={{ y: height - headerSize * 60, x: 'max-content' }}
      />
    );
  },

  number: ({ resultSet }: any) => (
    <Row
      justify="center"
      align="middle"
      style={{
        height: '100%',
      }}
    >
      <Col>
        {resultSet.seriesNames().map((s: any) => (
          <StyledStatistic key={s.key} value={resultSet.totalRow()[s.key] || 'No data'} />
        ))}
      </Col>
    </Row>
  ),
} as const;

const TypeToMemoChartComponent = Object.keys(TypeToChartComponent)
  .map((key) => ({
    [key]: memo(TypeToChartComponent[key as keyof typeof TypeToChartComponent]),
  }))
  .reduce((a: any, b: any) => ({ ...a, ...b }));

const renderChart = (Component: ComponentType<any>) =>
  function (
    {
      resultSet,
      error,
      ...restParams
    }: UseCubeQueryResult<any, any> & {
      height: number;
      stroke?: string[];
      fill?: string[];
    },
    chartType: ChartType
  ) {
    if (error) {
      return <LocalError error={error} />;
    }

    if (chartType === 'table') {
      return <Component {...restParams} resultSet={resultSet} />;
    }

    return (
      (resultSet && <Component {...restParams} resultSet={resultSet} />) || (
        <Skeleton layout="chart" fill="#white" height={restParams.height} />
      )
    );
  };

export function PlaygroundChartRenderer({
  query,
  chartType,
  resultSet,
  component,
  chartHeight,
  ...rest
}: any) {
  const componentToRender = component || TypeToMemoChartComponent[chartType];

  if (componentToRender) {
    return renderChart(componentToRender)(
      {
        height: chartHeight,
        ...rest,
        grid: 'both',
        resultSet,
      },

      chartType
    );
  }

  return null;
}
