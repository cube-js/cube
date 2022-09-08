// eslint-disable-next-line import/no-extraneous-dependencies
import { DeeplyReadonly, Query, QueryRecordType, ResultSet } from '@cubejs-client/core';
import { Schemas } from '../../src';

type DriverTestArg<QueryType extends DeeplyReadonly<Query | Query[]> = DeeplyReadonly<Query | Query[]>> = {
  name: string,
  query: QueryType,
  expectArray?: ((response: ResultSet<QueryRecordType<QueryType>>) => any)[]
  schemas: Schemas,
  skip?: boolean
};

export type DriverTest<QueryType extends DeeplyReadonly<Query | Query[]>> = {
  name: string,
  query: QueryType,
  expectArray?: ((response: ResultSet<QueryRecordType<QueryType>>) => any)[]
  schemas: Schemas,
  skip?: boolean;
};

export function driverTest<QueryType extends DeeplyReadonly<Query | Query[]> = DeeplyReadonly<Query | Query[]>>(
  { name, query, expectArray = [], skip, schemas }: DriverTestArg<QueryType>
) {
  return { name, query, expectArray, schemas, skip };
}
