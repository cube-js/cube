import React from 'react';
import { Typography } from 'antd';
import { Droppable } from 'react-beautiful-dnd';
import Item from './Item';
export default function DroppableArea({ pivotConfig, axis }) {
  return (
    <>
      <Typography.Text
        strong
        style={{
          display: 'flex',
          justifyContent: 'center',
        }}
      >
        {axis}
      </Typography.Text>
      <Droppable droppableId={axis}>
        {(provided) => (
          <div
            ref={provided.innerRef}
            {...provided.droppableProps}
            style={{
              height: '100%',
            }}
          >
            {pivotConfig[axis].map((id, index) => (
              <Item key={id} id={id} index={index} />
            ))}

            {provided.placeholder}
          </div>
        )}
      </Droppable>
    </>
  );
}
