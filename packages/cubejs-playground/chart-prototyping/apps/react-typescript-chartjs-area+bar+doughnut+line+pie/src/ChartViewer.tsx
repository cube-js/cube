import { PivotConfig, ResultSet } from '@cubejs-client/core';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Filler,
  Tooltip,
  Legend,
  BarElement,
  ArcElement,
} from 'chart.js';
import { Line, Bar, Pie, Doughnut } from 'react-chartjs-2';
import { CHART_COLORS, CHART_SOLID_COLORS } from './colors';
import { ChartType } from './types';

ChartJS.register(
  ArcElement,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Filler,
  Title,
  Tooltip,
  Legend
);

const CHART_OPTIONS = {
  responsive: true,
  maintainAspectRatio: true,
};

interface ChartViewerProps {
  resultSet: ResultSet;
  pivotConfig: PivotConfig;
  chartType: ChartType;
}

export function ChartViewer(props: ChartViewerProps) {
  const { resultSet, pivotConfig, chartType } = props;

  if (chartType === 'table') {
    return null;
  }

  const colors = chartType === 'line' ? CHART_COLORS : CHART_SOLID_COLORS;

  const data = {
    labels: resultSet.chartPivot(pivotConfig).map((row) => row.x),
    datasets: resultSet.series(pivotConfig).map((item, i) => {
      return {
        fill: chartType === 'area',
        label: item.title,
        data: item.series.map(({ value }) => value),
        backgroundColor: colors[i % colors.length],
      };
    }),
  };

  const ChartElement = {
    bar: Bar,
    line: Line,
    area: Line,
    pie: Pie,
    doughnut: Doughnut,
  }[chartType];

  if (!ChartElement) return;

  return <ChartElement options={CHART_OPTIONS} data={data} />;
}
