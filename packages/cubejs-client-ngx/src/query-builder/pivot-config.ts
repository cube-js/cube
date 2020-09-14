import { PivotConfig as TPivotConfig } from '@cubejs-client/core' ;
import { StateSubject } from './common';

type TSourceAxis = 'x' | 'y';

export class PivotConfig extends StateSubject<TPivotConfig> {
  constructor(pivotConfig: TPivotConfig) {
    super(pivotConfig);
  }

  moveItem(sourceIndex: number, destinationIndex: number, sourceAxis: TSourceAxis, destinationAxis: TSourceAxis) {
    const pivotConfig = this.subject.value;
    
    // todo: move to core
    const nextPivotConfig: TPivotConfig = {
      ...pivotConfig,
      x: [...pivotConfig.x],
      y: [...pivotConfig.y],
    };
    const id = pivotConfig[sourceAxis][sourceIndex];
    const lastIndex = nextPivotConfig[destinationAxis].length - 1;
    
    if (id === 'measures') {
      destinationIndex = lastIndex + 1;
    } else if (destinationIndex >= lastIndex && nextPivotConfig[destinationAxis][lastIndex] === 'measures') {
      destinationIndex = lastIndex - 1;
    }

    nextPivotConfig[sourceAxis].splice(sourceIndex, 1);
    nextPivotConfig[destinationAxis].splice(destinationIndex, 0, id);
    
    this.subject.next(nextPivotConfig);
  }
  
  get() {
    return this.subject.value;
  }
  
  set(pivotConfig: TPivotConfig) {
    this.subject.next(pivotConfig);
  }
}
