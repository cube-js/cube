import React, { useState, useEffect, useCallback } from 'react';
import { HTML5Backend } from 'react-dnd-html5-backend';
import { DndProvider, useDrop } from 'react-dnd';
import DraggableItem from './DraggableItem';
import useDeepCompareMemoize from '../../hooks/deep-compare-memoize';

export const TYPE = 'orderItem';

function indexById(members) {
  return Object.fromEntries(members.map(({ id }, index) => [id, index]));
}

function Order({ orderMembers, onChange }) {
  const [orderMembersOrder, setOrderMembersOrder] = useState(indexById(orderMembers));

  const sortedMembers = orderMembers.sort((a, b) => orderMembersOrder[a.id] - orderMembersOrder[b.id]);

  const [{ didDrop }, drop] = useDrop({
    accept: TYPE,
    collect(monitor) {
      return {
        didDrop: monitor.didDrop(),
        dropItem: monitor.getItem()
      };
    }
  });

  useEffect(() => {
    setOrderMembersOrder(indexById(orderMembers))
  }, useDeepCompareMemoize([orderMembers]));

  useEffect(() => {
    if (didDrop) {
      onChange(sortedMembers)
    }
  }, [didDrop]);

  const moveTag = useCallback(
    (dragIndex, hoverIndex) => {
      setOrderMembersOrder({
          ...orderMembersOrder,
          [dragIndex]: orderMembersOrder[hoverIndex],
          [hoverIndex]: orderMembersOrder[dragIndex]
        }
      )
    },
    [orderMembers, orderMembersOrder]
  );

  return (
    <div ref={drop}>
      {sortedMembers.map(({ id, title, order }, index) => {
        return (
          <DraggableItem
            key={id}
            id={id}
            index={index}
            order={order}
            moveTag={moveTag}
            onOrderChange={(order) => {
              onChange(sortedMembers.map((member) => {
                return {
                  ...member,
                  order: member.id === id ? order : member.order,
                };
              }))
            }}
          >
            {title}
          </DraggableItem>
        );
      })}
    </div>
  );
}

export default function OrderGroup(props) {
  return (
    <DndProvider backend={HTML5Backend}>
      <Order {...props} />
    </DndProvider>
  );
}
