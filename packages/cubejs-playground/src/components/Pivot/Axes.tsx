import { DragDropContext } from 'react-beautiful-dnd';
import { Row, Col } from 'antd';
import DroppableArea from './DroppableArea';

export default function Axes({ pivotConfig, onMove }) {
  return (
    <DragDropContext
      onDragEnd={({ source, destination, ...props }) => {
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
        <Col span={12} style={{ minWidth: 160 }}>
          <DroppableArea pivotConfig={pivotConfig} axis="x" />
        </Col>

        <Col span={12} style={{ minWidth: 160 }}>
          <DroppableArea pivotConfig={pivotConfig} axis="y" />
        </Col>
      </Row>
    </DragDropContext>
  );
}
