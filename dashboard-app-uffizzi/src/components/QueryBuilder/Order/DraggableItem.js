import React from 'react';
import { Button, Typography } from 'antd';
import { Draggable } from 'react-beautiful-dnd';
import { DragOutlined } from '@ant-design/icons';
const orderOptions = ['asc', 'desc', 'none'];
export default function DraggableItem({
  id,
  index,
  order = 'none',
  children,
  onOrderChange,
}) {
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
            display: 'flex',
            flexWrap: 'nowrap',
            justifyContent: 'space-between',
            alignItems: 'center',
            marginBottom: 8,
            ...draggableProps.style,
          }}
        >
          <DragOutlined />

          <Typography.Text
            ellipsis
            style={{
              margin: '0 auto 0 8px',
            }}
          >
            {children}
          </Typography.Text>

          <Button
            type={order !== 'none' ? 'primary' : null}
            size="small"
            style={{
              minWidth: 70,
              marginLeft: 8,
            }}
            onClick={() => onOrderChange(id, getNextOrder())}
          >
            {order.toUpperCase()}
          </Button>
        </div>
      )}
    </Draggable>
  );
}
