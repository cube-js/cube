import React from 'react';
import { DragDropContext, Droppable } from 'react-beautiful-dnd';
import DraggableItem from './DraggableItem';

function reorder(list, startIndex, endIndex) {
  const result = [...list];
  const [removed] = result.splice(startIndex, 1);
  result.splice(endIndex, 0, removed);

  return result;
}

export default function OrderGroup({ orderMembers, onChange }) {
  return (
    <DragDropContext
      onDragEnd={({ source, destination }) => {
        if (source !== null && destination !== null && source.index !== destination.index) {
          onChange(reorder(orderMembers, source.index, destination.index));
        }
      }}
    >
      <Droppable droppableId="droppable">
        {(provided) => (
          <div
            ref={provided.innerRef}
            {...provided.droppableProps}
            style={{
              paddingTop: 8,
              width: 260
            }}
          >
            {orderMembers.map(({ id, title, order }, index) => (
              <DraggableItem
                key={id}
                id={id}
                index={index}
                order={order}
                onOrderChange={(order) => {
                  onChange(
                    orderMembers.map((member) => ({
                      ...member,
                      order: member.id === id ? order : member.order
                    }))
                  );
                }}
              >
                {title}
              </DraggableItem>
            ))}

            {provided.placeholder}
          </div>
        )}
      </Droppable>
    </DragDropContext>
  );
}
