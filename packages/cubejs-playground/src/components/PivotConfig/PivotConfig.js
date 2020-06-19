import React from 'react';
import { DragDropContext } from 'react-beautiful-dnd';
import { Divider, Checkbox } from 'antd';
import DroppableArea from './DroppableArea';

export default function PivotConfig({ pivotConfig, onMove, onToggle }) {
  return (
    <DragDropContext
      onDragEnd={({ source, destination }) => {
        if (!destination) {
          return;
        }

        onMove({
          sourceIndex: source.index,
          destinationIndex: destination.index,
          sourceAxis: source.droppableId,
          destinationAxis: destination.droppableId,
        });
      }}
    >
      <DroppableArea pivotConfig={pivotConfig} axis="x" />

      <Divider style={{ margin: '12px 0' }} />

      <DroppableArea pivotConfig={pivotConfig} axis="y" />

      <Checkbox checked={pivotConfig.fillMissingDates} style={{ marginTop: 12 }} onChange={onToggle}>
        Fill missing dates
      </Checkbox>
    </DragDropContext>
  );
}
