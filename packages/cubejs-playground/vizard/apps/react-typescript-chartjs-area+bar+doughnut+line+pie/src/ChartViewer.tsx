import 'chart.js/auto';
import { Line, Bar, Pie, Doughnut } from 'react-chartjs-2';
import { PivotConfig, ResultSet } from '@cubejs-client/core';
import { type ChartType } from './types';

interface ChartViewerProps {
  resultSet: ResultSet;
  pivotConfig: PivotConfig;
  chartType: ChartType;
}

export function ChartViewer(props: ChartViewerProps) {
  const { resultSet, pivotConfig, chartType } = props;

  const data = {
    labels: resultSet.chartPivot(pivotConfig).map((row) => row.x),
    datasets: resultSet.series(pivotConfig).map((item) => {
      return {
        fill: chartType === 'area',
        label: item.title,
        data: item.series.map(({ value }) => value)
      };
    }),
  };

  const ChartElement = {
    area: Line,
    bar: Bar,
    doughnut: Doughnut,
    line: Line,
    pie: Pie
  }[chartType as Exclude<ChartType, 'table'>];

  return <ChartElement data={data} />;
}
