import { DragDropContext } from 'react-beautiful-dnd';
import { Grid } from '@cube-dev/ui-kit';

import { QueryBuilderContextProps } from '../types';

import { PivotDroppableArea } from './DroppableArea';

export function PivotAxes({
  pivotConfig,
  onMove,
}: {
  pivotConfig: QueryBuilderContextProps['pivotConfig'];
  onMove: QueryBuilderContextProps['updatePivotConfig']['moveItem'];
}) {
  return (
    <DragDropContext
      onDragEnd={({ source, destination }) => {
        if (!destination) {
          return;
        }
        onMove({
          sourceIndex: source.index,
          destinationIndex: destination.index,
          sourceAxis: source.droppableId as 'x' | 'y',
          destinationAxis: destination.droppableId as 'x' | 'y',
        });
      }}
    >
      <Grid columns="minmax(160px, 1fr) minmax(160px, 1fr)">
        <div>
          <PivotDroppableArea pivotConfig={pivotConfig} axis="x" />
        </div>
        <div>
          <PivotDroppableArea pivotConfig={pivotConfig} axis="y" />
        </div>
      </Grid>
    </DragDropContext>
  );
}
