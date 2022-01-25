import type {
  BinaryOperator,
  Filter,
  TimeDimension,
  TimeDimensionGranularity,
  UnaryOperator,
} from '@cubejs-client/core';

declare module "@cubejs-client/dx" {

  type GetGeneratedValue<T> = T extends undefined ? string : T;

  export type IntrospectedMeasureName = GetGeneratedValue<import('./generated').IntrospectedMeasureName>;
  export type IntrospectedDimensionName = GetGeneratedValue<import('./generated').IntrospectedDimensionName>;
  export type IntrospectedTimeDimensionName = GetGeneratedValue<import('./generated').IntrospectedTimeDimensionName>;
  export type IntrospectedSegmentName = GetGeneratedValue<import('./generated').IntrospectedSegmentName>;
  export type IntrospectedMemberName = IntrospectedMeasureName | IntrospectedDimensionName;

  export type QueryOrder = 'asc' | 'desc';
  export type IntrospectedTQueryOrderObject = { [key in IntrospectedMemberName]?: QueryOrder };
  export type IntrospectedTQueryOrderArray = Array<[IntrospectedMemberName, QueryOrder]>;

  export interface IntrospectedQuery {
    measures?: IntrospectedMeasureName[];
    dimensions?: IntrospectedDimensionName[];
    filters?: Filter[];
    timeDimensions?: TimeDimension[];
    segments?: IntrospectedSegmentName[];
    limit?: number;
    offset?: number;
    order?: IntrospectedTQueryOrderObject | IntrospectedTQueryOrderArray;
    timezone?: string;
    renewQuery?: boolean;
    ungrouped?: boolean;
  }

  export interface IntrospectedTimeDimensionBase {
    dimension: IntrospectedTimeDimensionName;
    granularity?: TimeDimensionGranularity;
  }

  export interface IntrospectedBinaryFilter {
    dimension?: IntrospectedMemberName;
    member?: IntrospectedMemberName;
    operator: BinaryOperator;
    values: string[];
  }
  export interface IntrospectedUnaryFilter {
    dimension?: IntrospectedMemberName;
    member?: IntrospectedMemberName;
    operator: UnaryOperator;
    values?: never;
  }
}
