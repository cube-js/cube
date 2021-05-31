import { Draggable } from 'react-beautiful-dnd';
import { DragOutlined } from '@ant-design/icons';
import { Typography } from 'antd';

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
            border: `1px dashed var(--dark-05-color)`,
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
