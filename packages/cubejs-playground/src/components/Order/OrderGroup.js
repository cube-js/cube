import React, { useState, useEffect, useCallback } from 'react';
import { HTML5Backend } from 'react-dnd-html5-backend';
import { DndProvider, useDrop } from 'react-dnd';
import DraggableItem from './DraggableItem';
import useDeepCompareMemoize from '../../hooks/deep-compare-memoize';

export const TYPE = 'orderItem';

function indexById(members) {
  return members.map(({ id }, index) => ({ [id]: index })).reduce((a, b) => ({ ...a, ...b }));
}

function Order({ orderMembers, onChange, testOnOrderChange }) {
  const [members, setMembers] = useState(orderMembers);
  const [orderMembersOrder, setOrderMembersOrder] = useState(indexById(orderMembers));

  const [{ didDrop, dropItem }, drop] = useDrop({
    accept: TYPE,
    collect(monitor, props) {
      // console.log('collect', props, monitor.getItem())
      return {
        didDrop: monitor.didDrop(),
        dropItem: monitor.getItem()
      };
    }
  });

  useEffect(() => {
    let index = Math.max(...Object.values(orderMembersOrder));
    const nextOrderMembersOrder = { ...orderMembersOrder };

    orderMembers.forEach(({ id }) => {
      if (nextOrderMembersOrder[id] === undefined) {
        nextOrderMembersOrder[id] = ++index;
      }
    })

    setOrderMembersOrder(nextOrderMembersOrder);
  }, useDeepCompareMemoize([orderMembers]));

  // useEffect(() => {
  //   const memberIds = orderMembers.map((member) => member.id);
  //   const addedMembers = [];
  //
  //   const nextMembers = members
  //     .map((currentSortItem, index) => {
  //       if (memberIds.includes(currentSortItem.id)) {
  //         addedMembers.push(currentSortItem.id);
  //
  //         return {
  //           ...currentSortItem,
  //           index,
  //         };
  //       }
  //     })
  //     .filter(Boolean);
  //
  //   orderMembers.forEach(({ id, title, order }) => {
  //     if (!addedMembers.includes(id)) {
  //       nextMembers.push({
  //         id,
  //         title,
  //         order: order || 'none',
  //         index: nextMembers.length - 1,
  //       });
  //     }
  //   });
  //
  //   setMembers(nextMembers);
  // }, useDeepCompareMemoize([orderMembers]));

  useEffect(() => {
    if (didDrop) {
      const { id, order, index } = dropItem;
      if (order !== 'none') {
        testOnOrderChange(id, order, index);
      }
      // handleOnChange(members);
    }
  }, [didDrop]);

  function handleOnChange(members) {
    onChange(
      members
        .filter((member) => member.order !== 'none')
        .map((member) => ({ [member.id]: member.order }))
        .reduce((a, b) => ({ ...a, ...b }), {})
    );
  }

  const moveTag = useCallback(
    (dragIndex, hoverIndex) => {
      // onMove(orderMembers[dragIndex].id, hoverIndex);
      // const dragItem = members[dragIndex];
      // const nextMembers = [...members.filter((_, index) => index !== dragIndex)];
      // nextMembers.splice(hoverIndex, 0, dragItem);

      console.log({
        dragIndex,
        hoverIndex
      })
      console.log('before', orderMembersOrder)

      setOrderMembersOrder({
          ...orderMembersOrder,
          [dragIndex]: orderMembersOrder[hoverIndex],
          [hoverIndex]: orderMembersOrder[dragIndex]
        }
      )

      // setMembers(nextMembers.map((member, index) => ({ ...member, index })));

      console.log('after', {
        ...orderMembersOrder,
        [dragIndex]: orderMembersOrder[hoverIndex],
        [hoverIndex]: orderMembersOrder[dragIndex]
      })
    },
    [orderMembers, orderMembersOrder]
  );

  const sortedMembers = orderMembers.sort((a, b) => orderMembersOrder[a.id] - orderMembersOrder[b.id]);

  console.log('...', orderMembersOrder)

  return (
    <div ref={drop}>
      {sortedMembers.map(({ id, title, order }, index) => {
        return (
          <DraggableItem
            key={id}
            id={id}
            index={orderMembersOrder[id]}
            order={order}
            moveTag={moveTag}
            onOrderChange={(order) => {
              testOnOrderChange(id, order);
              // const nextMembers = members.map((currentSortItem) => {
              //   return {
              //     ...currentSortItem,
              //     order: currentSortItem.id === id ? order : currentSortItem.order,
              //   };
              // });
              //
              // setMembers(nextMembers);
              // handleOnChange(nextMembers);
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
