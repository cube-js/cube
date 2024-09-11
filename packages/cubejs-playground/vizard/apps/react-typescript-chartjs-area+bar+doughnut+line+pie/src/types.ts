import { PivotConfig, Query } from '@cubejs-client/core';

export type ChartType = 'area' | 'bar' | 'doughnut' | 'line' | 'pie' | 'table';

export type Config = {
  apiUrl: string;
  apiToken: string;
  useWebSockets?: boolean;
  useSubscription?: boolean;
  query: Query;
  pivotConfig: PivotConfig;
  chartType: ChartType;
};
