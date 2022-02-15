/**
 * @license Apache-2.0
 * @copyright Cube Dev, Inc.
 * @fileoverview transformValue function unit tests.
 */

/* globals describe,test,expect */
/* eslint-disable import/no-duplicates */
/* eslint-disable @typescript-eslint/no-duplicate-imports */

import moment from 'moment';
import transformValueDef from '../../src/helpers/transformValue';
import { transformValue } from '../../src/helpers/transformValue';

describe('transformValue helper', () => {
  test('export looks as expected', () => {
    expect(transformValueDef).toBeDefined();
    expect(transformValue).toBeDefined();
    expect(transformValue).toEqual(transformValueDef);
  });
  // TODO: enable it after function refactoring.
  test.skip('object with the null value', () => {
    expect(transformValue({ value: null }, 'null')).toBeNull();
  });
  // TODO: enable it after function refactoring.
  test.skip('object with the boolean value', () => {
    expect(transformValue({ value: true }, 'boolean')).toBeTruthy();
    expect(transformValue({ value: false }, 'boolean')).toBeFalsy();
  });
  // TODO: enable it after function refactoring.
  test.skip('object with the the number value', () => {
    expect(transformValue({ value: 0 }, 'number')).toBe(0);
    expect(transformValue({ value: 10 }, 'number')).toBe(10);
    expect(transformValue({ value: -1 }, 'number')).toBe(-1);
  });
  test('object with the string value', () => {
    expect(transformValue({ value: 'a' }, 'string')).toBe('a');
    expect(transformValue({ value: 'abc' }, 'string')).toBe('abc');
  });
  test('object with the time value', () => {
    const date = Date();
    expect(transformValue(date, 'time')).toBe(
      moment.utc(date).format(moment.HTML5_FMT.DATETIME_LOCAL_MS)
    );
  });
  test('object with the Date value', () => {
    const date = new Date();
    expect(transformValue(date, 'date')).toBe(
      moment(date).format(moment.HTML5_FMT.DATETIME_LOCAL_MS)
    );
  });
});
