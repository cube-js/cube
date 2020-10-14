import React from 'react';
import { Draggable } from 'react-beautiful-dnd';
import { DragOutlined } from '@ant-design/icons';
import { Typography } from 'antd';
import vars from '../../variables';

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
            border: `1px dashed ${vars.dark05Color}`,
            borderRadius: 4,
            padding: '5px 12px',
            lineHeight: '22px',
          }}
        >
          <Typography.Text ellipsis style={{ maxWidth: '100%' }}>
            <DragOutlined style={{ marginRight: 8 }} />
            {id}
          </Typography.Text>
        </div>
      )}
    </Draggable>
  );
}
