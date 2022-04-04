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
