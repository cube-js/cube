// eslint-disable-next-line import/no-extraneous-dependencies
import { DeeplyReadonly, Query, QueryRecordType, ResultSet } from '@cubejs-client/core';
import { Schemas } from '../../src';

type TestType = 'basic' | 'withError';

type DriverTestArg<QueryType extends DeeplyReadonly<Query | Query[]> = DeeplyReadonly<Query | Query[]>> = {
  name: string;
  query: QueryType;
  expectArray?: ((response: ResultSet<QueryRecordType<QueryType>>) => any)[];
  schemas: Schemas;
  skip?: boolean;
};

type DriverTestWithErrorArg<QueryType extends DeeplyReadonly<Query | Query[]> = DeeplyReadonly<Query | Query[]>> = {
  name: string;
  query: QueryType;
  expectArray?: ((e: Error) => any)[];
  schemas: Schemas;
  skip?: boolean;
};

export type DriverTest<QueryType extends DeeplyReadonly<Query | Query[]>> = {
  name: string,
  query: QueryType,
  expectArray?: ((response: ResultSet<QueryRecordType<QueryType>>) => any)[]
  schemas: Schemas,
  skip?: boolean;
  type: TestType;
};

export type DriverTestWithError<QueryType extends DeeplyReadonly<Query | Query[]>> = {
  name: string;
  query: QueryType;
  expectArray?: ((e: Error) => any)[];
  schemas: Schemas;
  skip?: boolean;
  type: TestType;
};

export function driverTest<QueryType extends DeeplyReadonly<Query | Query[]> = DeeplyReadonly<Query | Query[]>>(
  { name, query, expectArray = [], skip, schemas }: DriverTestArg<QueryType>
): DriverTest<QueryType> {
  return { name, query, expectArray, schemas, skip, type: 'basic' };
}

export function driverTestWithError<QueryType extends DeeplyReadonly<Query | Query[]> = DeeplyReadonly<Query | Query[]>>(
  { name, query, expectArray = [], skip, schemas }: DriverTestWithErrorArg<QueryType>
): DriverTestWithError<QueryType> {
  return { name, query, expectArray, schemas, skip, type: 'withError' };
}
