import { PivotConfig } from '@cubejs-client/core';

export function movePivotItem(
  pivotConfig: PivotConfig,
  sourceIndex: number,
  destinationIndex: number,
  sourceAxis: 'x' | 'y',
  destinationAxis: 'x' | 'y'
) {
  const nextPivotConfig = {
    ...pivotConfig,
    x: [...(pivotConfig.x || [])],
    y: [...(pivotConfig.y || [])],
  };
  const id = pivotConfig?.[sourceAxis]?.[sourceIndex];
  const lastIndex = nextPivotConfig[destinationAxis].length - 1;

  if (id === undefined) {
    return pivotConfig;
  }

  if (id === 'measures') {
    destinationIndex = lastIndex + 1;
  } else if (
    destinationIndex >= lastIndex &&
    nextPivotConfig[destinationAxis][lastIndex] === 'measures'
  ) {
    destinationIndex = lastIndex - 1;
  }

  nextPivotConfig[sourceAxis].splice(sourceIndex, 1);
  nextPivotConfig[destinationAxis].splice(destinationIndex, 0, id);

  return nextPivotConfig;
}
