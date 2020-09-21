import { StateSubject } from './common';

export type TChartType = 'line' | 'bar' | 'number' | 'table' | 'pie';

export class ChartType extends StateSubject<TChartType> {
  constructor(value) {
    super(value);
  }
}
