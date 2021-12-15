declare module "@cubejs-client/core" {

  export type IntrospectedMeasureName = import('./generated').IntrospectedMeasureName;
  export type IntrospectedDimensionName = import('./generated').IntrospectedDimensionName;
  export type IntrospectedTimeDimensionName = import('./generated').IntrospectedTimeDimensionName;
  export type IntrospectedSegmentName = import('./generated').IntrospectedSegmentName;
  export type IntrospectedMemberName = IntrospectedMeasureName | IntrospectedDimensionName;

  export type IntrospectedTQueryOrderObject = { [key in IntrospectedMemberName]?: QueryOrder };
  export type IntrospectedTQueryOrderArray = Array<[IntrospectedMemberName, QueryOrder]>;

  export interface Query {
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

  export interface TimeDimensionBase {
    dimension: IntrospectedTimeDimensionName;
  }

  export interface BinaryFilter {
    dimension?: IntrospectedMemberName;
    member?: IntrospectedMemberName;
  }

  export interface UnaryFilter {
    dimension?: IntrospectedMemberName;
    member?: IntrospectedMemberName;
  }

  export interface TFlatFilter {
    dimension?: IntrospectedMemberName;
    member?: IntrospectedMemberName;
  }
}
