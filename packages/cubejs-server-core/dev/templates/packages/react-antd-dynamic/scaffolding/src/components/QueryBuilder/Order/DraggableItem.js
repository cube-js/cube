import React from 'react';
import { Button } from 'antd';
import { Draggable } from 'react-beautiful-dnd';
import { DragOutlined } from '@ant-design/icons';

const orderOptions = ['asc', 'desc', 'none'];

export default function DraggableItem({ id, index, order = 'none', children, onOrderChange }) {
  const getNextOrder = () => {
    const index = orderOptions.indexOf(order) + 1;
    return orderOptions[index > 2 ? 0 : index];
  };

  return (
    <Draggable draggableId={id} index={index}>
      {({ draggableProps, dragHandleProps, innerRef }) => (
        <div
          ref={innerRef}
          {...draggableProps}
          {...dragHandleProps}
          style={{
            marginBottom: 8,
            ...draggableProps.style
          }}
        >
          <DragOutlined style={{ marginRight: 8 }} />

          <span>{children}</span>

          <Button
            type={order !== 'none' ? 'primary' : null}
            size="small"
            style={{ width: 80, float: 'right' }}
            onClick={() => onOrderChange(id, getNextOrder())}
          >
            {order.toUpperCase()}
          </Button>
        </div>
      )}
    </Draggable>
  );
}
