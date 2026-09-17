import { ChartType } from '../src/query-builder/chart-type';
import { StateSubject } from '../src/query-builder/common';
import { PivotConfig } from '../src/query-builder/pivot-config';

describe('StateSubject', () => {
  test('get returns the current value and set emits', () => {
    const state = new StateSubject(1);
    const emitted: number[] = [];
    state.subject.subscribe((value) => emitted.push(value));

    expect(state.get()).toBe(1);

    state.set(2);

    expect(state.get()).toBe(2);
    expect(emitted).toEqual([1, 2]);
  });
});

describe('ChartType', () => {
  test('holds the chart type', () => {
    const chartType = new ChartType('line');

    expect(chartType.get()).toBe('line');

    chartType.set('bar');

    expect(chartType.get()).toBe('bar');
  });
});

describe('PivotConfig', () => {
  const config = () => new PivotConfig({
    x: ['Orders.createdAt.day'],
    y: ['measures'],
  });

  test('setFillMissingDates keeps the rest of the config', () => {
    const pivotConfig = config();

    pivotConfig.setFillMissingDates(true);

    expect(pivotConfig.get()).toEqual({
      x: ['Orders.createdAt.day'],
      y: ['measures'],
      fillMissingDates: true,
    });
  });

  test('moveItem moves an item between axes', () => {
    const pivotConfig = config();

    pivotConfig.moveItem(0, 0, 'x', 'y');

    expect(pivotConfig.get()).toEqual({
      x: [],
      y: ['Orders.createdAt.day', 'measures'],
    });
  });
});
