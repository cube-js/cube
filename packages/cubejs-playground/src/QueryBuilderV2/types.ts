import {
  ChartType,
  Cube,
  PivotConfig,
  PreAggregationType,
  Query,
} from '@cubejs-client/core';
import { VizState } from '@cubejs-client/react';
import { FC, ReactNode } from 'react';

import { useQueryBuilder } from './hooks/query-builder';

export type QueryBuilderTracking = {
  event: (name: string, props?: Record<string, any>) => void;
};

type SchemaChangeProps = {
  schemaVersion: number;
  refresh: () => Promise<void>;
};

export interface QueryBuilderSharedProps {
  apiUrl: string;
  apiToken: string | null;
  apiVersion?: string;
  isApiBlocked?: boolean;
  openSqlRunner?: (sqlQuery: string) => void;
  VizardComponent?: FC<any>;
  RequestStatusComponent?: FC<RequestStatusProps>;
}

export interface QueryBuilderContextProps
  extends ReturnType<typeof useQueryBuilder>,
    QueryBuilderSharedProps {
  selectedCube: Cube | null;
  selectCube: (cube: string | null) => void;
  connectionId?: number;
  tracking?: QueryBuilderTracking;
}

export interface QueryBuilderProps extends QueryBuilderSharedProps {
  schemaVersion?: number;
  defaultQuery?: Query;
  shouldRunDefaultQuery?: boolean;
  initialVizState?: VizState;
  onSchemaChange?: (props: SchemaChangeProps) => void;
  extra?: ReactNode | null;
  defaultChartType?: ChartType;
  defaultPivotConfig?: PivotConfig;
  tracking?: QueryBuilderTracking;
  onQueryChange?:
    | ((data: { query: Query; chartType?: ChartType }) => void)
    | undefined;
}

export type CubeStats = {
  missing?: boolean;
  instance?: Cube;
  measures: string[];
  dimensions: string[];
  timeDimensions: string[];
  filters: string[];
  segments: string[];
  dateRanges: string[];
  grouping: string[];
};

export interface RequestStatusProps {
  requestId: string;
  isAggregated: boolean;
  dbType?: string;
  error?: string;
  external: boolean | null;
  extDbType: string;
  preAggregationType?: PreAggregationType;
}
