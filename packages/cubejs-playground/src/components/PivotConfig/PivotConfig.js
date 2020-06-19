import React from 'react';
import { Droppable, DragDropContext } from 'react-beautiful-dnd';
import { Row, Col, Divider } from 'antd';
import { DragOutlined } from '@ant-design/icons';
import Item from './Item'

export default function PivotConfig({ pivotConfig, onChange }) {
  console.log('!PivotConfig!', pivotConfig)
  return (
    <>
      <DragDropContext
        onDragEnd={({ source, destination }) => {
          if (!destination) {
            return;
          }

          onChange({
            sourceIndex: source.index,
            destinationIndex: destination.index,
            sourceAxis: source.droppableId,
            destinationAxis: destination.droppableId,
          });
        }}
      >
        <Droppable droppableId="x" direction="horizontal">
          {(provided) => (
            <div ref={provided.innerRef} {...provided.droppableProps}>
              <Row gutter={8}>
                {pivotConfig.x.map((id, index) => {
                  return (
                    <Item key={id} id={id} index={index}>
                      <Col><DragOutlined /> {id}</Col>
                    </Item>
                  );
                })}

                {provided.placeholder}
              </Row>
            </div>
          )}
        </Droppable>

        <Row>
          <Col span={24}>
            <Divider style={{ margin: '12px 0' }} />
          </Col>
        </Row>

        <Droppable droppableId="y" direction="horizontal">
          {(provided) => (
            <div ref={provided.innerRef} {...provided.droppableProps}>
              <Row gutter={8}>
                {pivotConfig.y.map((id, index) => {
                  return (
                    <Item key={id} id={id} index={index}>
                      <Col><DragOutlined /> {id}</Col>
                    </Item>
                  );
                })}

                {provided.placeholder}
              </Row>
            </div>
          )}
        </Droppable>
      </DragDropContext>
    </>
  );
}
