import { jest } from '@jest/globals';
import { Method, PatchedDriver } from '../types/PatchedDriver';

const methods: Method[] = [
  // 'streamQuery',
  'downloadQueryResults',
  // 'readOnly',
  // 'tablesSchema',
  'createSchemaIfNotExists',
  // 'getTablesQuery',
  'loadPreAggregationIntoTable',
  'dropTable',
  // 'param',
  // 'testConnectionTimeout',
  'downloadTable',
  'uploadTable',
  'uploadTableWithIndexes',
  'tableColumnTypes',
  'queryColumnTypes',
  'createTable',
  // 'setLogger',
  // 'release',
  // 'capabilities',
  // 'nowTimestamp',
  // 'wrapQueryWithLimit',
  // 'testConnection',
  'query'
];

export function patchDriver(driver: PatchedDriver) {
  driver.calls = [];
  methods.forEach((name: Method) => {
    if (driver[name]) {
      const origin = driver[name].bind(driver);
      // @ts-ignore
      jest.spyOn(driver, name).mockImplementation((...args) => {
        if (name === 'query') {
          if (
            true
            && `${args[0]}`.toLowerCase().indexOf('select floor') !== 0
            && `${args[0]}`.toLowerCase().indexOf('select schema_name') !== 0
            && `${args[0]}`.toLowerCase().indexOf('select table_name') !== 0
            && `${args[0]}`.toLowerCase().indexOf('select max') !== 0
            && `${args[0]}`.toLowerCase().indexOf('select min') !== 0
            && `${args[0]}`.toLowerCase().indexOf('create schema') !== 0
            && `${args[0]}`.toLowerCase().indexOf('create table') !== 0
            && `${args[0]}`.toLowerCase().indexOf('insert into') !== 0
            && `${args[0]}`.toLowerCase().indexOf('drop table') !== 0
          ) {
            driver.calls?.push(name);
          }
        } else {
          driver.calls?.push(name);
        }
        // @ts-ignore
        return origin(...args);
      });
    }
  });
}
