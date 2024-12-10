import type { ConfigItem } from '../helpers/prepareAnnotation';
import type { NormalizedQuery } from './query';
import type { QueryType, ResultType } from './strings';

export type DBResponsePrimitive =
  null |
  boolean |
  number |
  string;

export type DBResponseValue =
  Date |
  DBResponsePrimitive |
  { value: DBResponsePrimitive };

export type TransformDataResponse = {
  members: string[],
  dataset: DBResponsePrimitive[][]
} | {
  [member: string]: DBResponsePrimitive
}[];

/**
 * SQL aliases to cube properties hash map.
 */
export type AliasToMemberMap = { [alias: string]: string };

export type TransformDataRequest = {
  aliasToMemberNameMap: { [alias: string]: string },
  annotation: { [member: string]: ConfigItem },
  data: { [sqlAlias: string]: unknown }[],
  query: NormalizedQuery,
  queryType: QueryType,
  resType?: ResultType
};
