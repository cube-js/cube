// eslint-disable-next-line import/no-extraneous-dependencies
import { CubejsApi, Query, QueryRecordType, ResultSet } from '@cubejs-client/core';
import { uniqBy } from 'ramda';
import { Schemas } from '../../src';

export type TestType = 'basic' | 'withError' | 'testFn';

type DriverTestArg = {
  name: string;
  query: Query;
  expectArray?: ((response: ResultSet<QueryRecordType<Query>>) => any)[];
  schemas: Schemas;
  skip?: boolean;
};

type DriverTestWithErrorArg = {
  name: string;
  query: Query;
  expectArray?: ((e: Error) => any)[];
  schemas: Schemas;
  skip?: boolean;
};

export type DriverTestBasic = {
  name: string,
  query: Query,
  expectArray?: ((response: ResultSet<QueryRecordType<Query>>) => any)[]
  schemas: Schemas,
  skip?: boolean;
  type: 'basic';
};

export type DriverTestWithError = {
  name: string;
  query: Query;
  expectArray?: ((e: Error) => any)[];
  schemas: Schemas;
  skip?: boolean;
  type: 'withError';
};

type DriverTestFnArg = {
  name: string;
  schemas: Schemas,
  skip?: boolean;
  testFn: (client: CubejsApi) => Promise<void>;
};

export type DriverTestFn = DriverTestFnArg & {
  type: 'testFn';
};

export type DriverTest = DriverTestBasic | DriverTestWithError | DriverTestFn;

export function driverTest(
  { name, query, expectArray = [], skip, schemas }: DriverTestArg
): DriverTestBasic {
  return { name, query, expectArray, schemas, skip, type: 'basic' };
}

export function driverTestWithError(
  { name, query, expectArray = [], skip, schemas }: DriverTestWithErrorArg
): DriverTestWithError {
  return { name, query, expectArray, schemas, skip, type: 'withError' };
}

export function driverTestFn(
  { name, skip, schemas, testFn }: DriverTestFnArg
): DriverTestFn {
  return { name, testFn, schemas, skip, type: 'testFn' };
}

export function testSet(tests: DriverTest[]) {
  const uniqTests = uniqBy((t) => t.name, tests);
  return uniqTests;
}
