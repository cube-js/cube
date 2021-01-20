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
          padding: '8px 16px',
          background: 'var(--light-5)',
          borderBottom: '1px solid var(--light-color)',
        }}
      >
        {axis}
      </Typography.Text>
      <div style={{
        padding: '8px',
      }}>
        <Droppable droppableId={axis}>
          {(provided) => (
            <div
              ref={provided.innerRef}
              {...provided.droppableProps}
              style={{
                display: 'grid',
                gridTemplateColumns: 'minmax(0, 1fr)',
                gap: '8px',
                height: '100%',
                minHeight: '32px',
              }}
            >
              {pivotConfig[axis].map((id, index) => (
                <Item key={id} id={id} index={index} />
              ))}

              {provided.placeholder}
            </div>
          )}
        </Droppable>
      </div>
    </>
  );
}
