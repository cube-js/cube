import React from 'react';
import { Draggable } from 'react-beautiful-dnd';

export default function Item({ id, index, children }) {
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
          {children}
        </div>
      )}
    </Draggable>
  );
}
