/* globals describe,test,expect */

import 'jest';
import dayjs from 'dayjs';
import { internalDayjs } from '../src/time';

describe('Dayjs Instance Isolation', () => {
  test('internalDayjs should not affect global dayjs instance week start', () => {
    const initialWeekStart = dayjs().startOf('week').format('dddd');
    
    const cubeDayjs = internalDayjs();
    expect(cubeDayjs.startOf('week').format('dddd')).toBe('Monday');
    
    const afterWeekStart = dayjs().startOf('week').format('dddd');
    expect(afterWeekStart).toBe(initialWeekStart);
  });

  test('internalDayjs week calculation should use Monday as week start', () => {
    const testDate = '2024-01-10';

    const globalWeekStartBefore = dayjs(testDate).startOf('week');
    const internalWeekStart = internalDayjs(testDate).startOf('week');
    expect(internalWeekStart.format('YYYY-MM-DD')).toBe('2024-01-08');
    expect(internalWeekStart.format('dddd')).toBe('Monday');
    
    const globalWeekStartAfter = dayjs(testDate).startOf('week');
    expect(globalWeekStartAfter.format('YYYY-MM-DD')).toBe(globalWeekStartBefore.format('YYYY-MM-DD'));
    expect(globalWeekStartAfter.format('dddd')).toBe(globalWeekStartBefore.format('dddd'));
  });

  test('multiple calls to internalDayjs should not affect global instance', () => {
    const initialWeekStart = dayjs().startOf('week').format('dddd');
    
    internalDayjs('2024-01-01');
    internalDayjs('2024-02-01');
    internalDayjs('2024-03-01');
    
    expect(dayjs().startOf('week').format('dddd')).toBe(initialWeekStart);
  });

  test('internalDayjs should consistently use weekStart: 1', () => {
    const dates = [
      '2024-01-10', // Wednesday
      '2024-02-15', // Thursday
      '2024-03-20', // Wednesday
      '2024-04-25', // Thursday
    ];
    
    dates.forEach((date) => {
      const weekStart = internalDayjs(date).startOf('week');
      expect(weekStart.format('dddd')).toBe('Monday');
    });
  });
});
