/**
 * @license Apache-2.0
 * @copyright Cube Dev, Inc.
 * @fileoverview transformValue function unit tests.
 */

/* globals describe,test,expect */
import moment from 'moment';

import { transformValue } from '../../src/helpers/transformData';

describe('transformValue helper', () => {
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
