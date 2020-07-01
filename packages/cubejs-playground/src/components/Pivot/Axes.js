import React from 'react';
import { DragDropContext } from 'react-beautiful-dnd';
import { Row, Col, Divider } from 'antd';
import DroppableArea from './DroppableArea';

export default function Axes({ pivotConfig, onMove }) {
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
      <Row>
        <Col span={11}>
          <DroppableArea pivotConfig={pivotConfig} axis="x" />
        </Col>

        <Col span={2}>
          <Divider style={{ height: '100%' }} type="vertical" />
        </Col>

        <Col span={11}>
          <DroppableArea pivotConfig={pivotConfig} axis="y" />
        </Col>
      </Row>
    </DragDropContext>
  );
}
