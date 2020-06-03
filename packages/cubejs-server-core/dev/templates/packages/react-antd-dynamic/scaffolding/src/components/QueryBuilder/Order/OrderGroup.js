import React, { useState, useRef, useEffect, useCallback } from 'react';
import equals from 'fast-deep-equal';
import { HTML5Backend } from 'react-dnd-html5-backend';
import { DndProvider, useDrop } from 'react-dnd';
import DraggableItem from './DraggableItem';

export const TYPE = 'orderItem';

function Order({ members, onChange }) {
  const [sortItems, setSortItems] = useState([]);

  const [{ didDrop }, drop] = useDrop({
    accept: TYPE,
    collect(monitor) {
      return {
        didDrop: monitor.didDrop(),
      };
    },
  });

  useEffect(() => {
    const memberIds = members.map((member) => member.id);
    const addedMembers = [];

    const nextSortItems = sortItems
      .map((currentSortItem, index) => {
        if (memberIds.includes(currentSortItem.id)) {
          addedMembers.push(currentSortItem.id);

          return {
            ...currentSortItem,
            index,
          };
        }
      })
      .filter(Boolean);

    members.forEach(({ id, title, order }) => {
      if (!addedMembers.includes(id)) {
        nextSortItems.push({
          id,
          title,
          order: order || 'none',
          index: nextSortItems.length - 1,
        });
      }
    });

    setSortItems(nextSortItems);
  }, useDeepCompareMemoize([members]));

  useEffect(() => {
    if (didDrop) {
      handleOnChange(sortItems);
    }
  }, [didDrop]);

  function handleOnChange(sortItems) {
    onChange(
      sortItems
        .filter((tag) => tag.order !== 'none')
        .map((tag) => ({ [tag.id]: tag.order }))
        .reduce((a, b) => ({ ...a, ...b }), {})
    );
  }

  const moveTag = useCallback(
    (dragIndex, hoverIndex) => {
      const dragTag = sortItems[dragIndex];
      const nextSortItems = [...sortItems.filter((_, index) => index !== dragIndex)];
      nextSortItems.splice(hoverIndex, 0, dragTag);

      setSortItems(nextSortItems.map((tag, index) => ({ ...tag, index })));
    },
    [sortItems]
  );

  return (
    <div ref={drop}>
      {sortItems.map(({ id, title, order }, index) => {
        return (
          <DraggableItem
            key={id}
            id={id}
            index={index}
            order={order}
            moveTag={moveTag}
            onOrderChange={(order) => {
              const nextSortItems = sortItems.map((currentSortItem) => {
                return {
                  ...currentSortItem,
                  order: currentSortItem.id === id ? order : currentSortItem.order,
                };
              });

              setSortItems(nextSortItems);
              handleOnChange(nextSortItems);
            }}
          >
            {title}
          </DraggableItem>
        );
      })}
    </div>
  );
}

function useDeepCompareMemoize(value) {
  const ref = useRef([]);

  if (!equals(value, ref.current)) {
    ref.current = value;
  }

  return ref.current;
}

export default function OrderGroup(props) {
  return (
    <DndProvider backend={HTML5Backend}>
      <Order {...props} />
    </DndProvider>
  );
}
