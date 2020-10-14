import React from 'react';
import { Typography } from 'antd';
import { Droppable } from 'react-beautiful-dnd';
import Item from './Item';
import vars from '../../variables';

export default function DroppableArea({ pivotConfig, axis }) {
  return (
    <>
      <Typography.Text
        strong
        style={{
          display: 'flex',
          justifyContent: 'center',
          padding: '8px 16px',
          background: vars.light5Color,
          borderBottom: `1px solid ${vars.lightColor}`,
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
