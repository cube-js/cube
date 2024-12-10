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
