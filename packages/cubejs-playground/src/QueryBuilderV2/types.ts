import {
  ChartType,
  Cube as OriginalCube,
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
  disableSidebarResizing?: boolean;
  tracking?: QueryBuilderTracking;
  RequestStatusComponent?: FC<RequestStatusProps>;
}

export interface QueryBuilderContextProps
  extends ReturnType<typeof useQueryBuilder>,
    QueryBuilderSharedProps {
  selectedCube: Cube | null;
  selectCube: (cube: string | null) => void;
  connectionId?: number;
}

export interface QueryBuilderProps extends QueryBuilderSharedProps {
  schemaVersion?: number;
  defaultQuery?: Query;
  shouldRunDefaultQuery?: boolean;
  initialVizState?: VizState;
  onSchemaChange?: (props: SchemaChangeProps) => void;
  extra?: ReactNode | null;
  memberViewType?: MemberViewType;
  defaultChartType?: ChartType;
  defaultPivotConfig?: PivotConfig;
  onQueryChange?: ((data: { query: Query; chartType?: ChartType }) => void) | undefined;
}

export type CubeStats = {
  isUsed: boolean;
  instance?: Cube;
  measures: string[];
  dimensions: string[];
  timeDimensions: string[];
  filters: string[];
  folders: Record<
    string,
    {
      dimensions: string[];
      measures: string[];
      segments: string[];
      grouping: string[];
    }
  >;
  hierarchies: Record<string, string[]>;
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

export interface QueryOptions {
  ungrouped?: boolean;
}

export type MissingMember = {
  name: string;
  category: 'measures' | 'dimensions' | 'segments' | 'timeDimensions';
  type?: 'string' | 'number' | 'time' | 'boolean';
  granularities?: string[];
  selected?: boolean;
};

export type WithUndefinedValues<T> = {
  [K in keyof T]: T[K] | undefined;
};

export type MemberType = 'measure' | 'dimension' | 'segment';

export type MemberViewType = 'name' | 'title';

export type TCubeFolder = {
  name: string;
  members: string[];
};

export type TCubeHierarchy = {
  name: string;
  title?: string;
  levels: string[];
  public?: boolean;
};

export type Cube = OriginalCube & {
  folders: TCubeFolder[];
  hierarchies: TCubeHierarchy[];
};
