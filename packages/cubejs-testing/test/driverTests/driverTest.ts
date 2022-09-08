// eslint-disable-next-line import/no-extraneous-dependencies
import { DeeplyReadonly, Query, QueryRecordType, ResultSet } from '@cubejs-client/core';

type DriverTestArg<QueryType extends DeeplyReadonly<Query | Query[]> = DeeplyReadonly<Query | Query[]>> = {
  name: string,
  query: QueryType,
  expectArray?: ((response: ResultSet<QueryRecordType<QueryType>>) => any)[]
  skip?: boolean
};

export type DriverTest<QueryType extends DeeplyReadonly<Query | Query[]>> = {
  name: string,
  query: QueryType,
  expectArray?: ((response: ResultSet<QueryRecordType<QueryType>>) => any)[]
  skip?: boolean;
};

export function driverTest<QueryType extends DeeplyReadonly<Query | Query[]> = DeeplyReadonly<Query | Query[]>>(
  { name, query, expectArray = [], skip }: DriverTestArg<QueryType>
) {
  return { name, query, expectArray, skip };
}
