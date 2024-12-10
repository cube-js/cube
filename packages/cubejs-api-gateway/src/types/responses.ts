/**
 * Query 'or'-filters type definition.
 */
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
