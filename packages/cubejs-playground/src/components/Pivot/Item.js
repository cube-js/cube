import React from 'react';
import { Draggable } from 'react-beautiful-dnd';
import { DragOutlined } from '@ant-design/icons';

export default function Item({ id, index }) {
  return (
    <Draggable draggableId={id} index={index}>
      {({ draggableProps, dragHandleProps, innerRef }) => (
        <div
          ref={innerRef}
          {...draggableProps}
          {...dragHandleProps}
          style={{
            ...draggableProps.style,
          }}
        >
          <DragOutlined style={{ marginRight: 8 }} />

          <span>{id}</span>
        </div>
      )}
    </Draggable>
  );
}
