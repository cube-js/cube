declare module "@cubejs-backend/api-gateway" {
  export interface QueryFilter {
    member: string;
    operator:
      | "equals"
      | "notEquals"
      | "contains"
      | "notContains"
      | "gt"
      | "gte"
      | "lt"
      | "lte"
      | "set"
      | "notSet"
      | "inDateRange"
      | "notInDateRange"
      | "beforeDate"
      | "afterDate";
    values?: string[];
  }

  export type QueryTimeDimensionGranularity =
    | "hour"
    | "day"
    | "week"
    | "month"
    | "year";

  export interface QueryTimeDimension {
    dimension: string;
    dateRange?: string[] | string;
    granularity?: QueryTimeDimensionGranularity;
  }

  export interface Query {
    measures: string[];
    dimensions?: string[];
    filters?: QueryFilter[];
    timeDimensions?: QueryTimeDimension[];
    segments?: string[];
    limit?: number;
    offset?: number;
    order?: "asc" | "desc";
    timezone?: string;
    renewQuery?: boolean;
    ungrouped?: boolean;
  }

  export interface NormalizedQuery extends Query {
    filters?: NormalizedQueryFilter[];
    rowLimit?: number;
  }

  export interface NormalizedQueryFilter extends QueryFilter {
    dimension?: string;
  }
}
