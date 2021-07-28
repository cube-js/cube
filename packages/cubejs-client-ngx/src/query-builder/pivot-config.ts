import {
  movePivotItem,
  PivotConfig as TPivotConfig,
  TSourceAxis,
} from '@cubejs-client/core';
import { StateSubject } from './common';

export class PivotConfig extends StateSubject<TPivotConfig> {
  constructor(pivotConfig: TPivotConfig) {
    super(pivotConfig);
  }
  
  moveItem(
    sourceIndex: number,
    destinationIndex: number,
    sourceAxis: TSourceAxis,
    destinationAxis: TSourceAxis
  ) {
    this.subject.next(
      movePivotItem(
        this.get(),
        sourceIndex,
        destinationIndex,
        sourceAxis,
        destinationAxis
      )
    );
  }
  
  setFillMissingDates(fillMissingDates: boolean) {
    this.subject.next({
      ...this.get(),
      fillMissingDates
    });
  }
}
