import React, { useRef, useEffect } from 'react';
import { Button } from 'antd';
import styled from 'styled-components';
import { useDrag, useDrop } from 'react-dnd';

import { TYPE } from './OrderGroup';

const orderOptions = ['asc', 'desc', 'none'];

const SortItem = styled.div`
  display: flex;
  flex-wrap: nowrap;

  & + div {
    margin-top: 8px;
  }
  
  & > button {
    width: 70px;  
  }
`;

const MemberName = styled.div`
  display: inline-block;
  border: 1px dotted #d9d9d9;
  border-right: none;
  border-radius: 3px;
  padding: 4px 12px;
  cursor: move;
  flex-grow: 1;
  margin-right: -3px;
`;

export default function DraggableItem({ id, index, order = 'none', moveTag, children, onOrderChange }) {
  const ref = useRef(null);

  const [, drop] = useDrop({
    accept: TYPE,
    hover(item) {
      if (!ref.current) {
        return;
      }
      const dragIndex = item.index;
      const hoverIndex = index;

      if (dragIndex === hoverIndex) {
        return;
      }
      // moveTag(dragIndex, hoverIndex);
      // console.log('hover', item.id, id, { dragIndex, hoverIndex, item })
      moveTag(item.id, id);

      item.index = hoverIndex;
    },
  });

  const [{ isDragging }, drag] = useDrag({
    item: {
      id,
      type: TYPE,
      index,
      order
    },
    collect: (monitor) => ({
      isDragging: monitor.isDragging(),
    }),
  });

  useEffect(() => {
    drag(drop(ref));
  }, [ref]);

  function getNextOrder() {
    const index = orderOptions.indexOf(order) + 1;
    return orderOptions[index > 2 ? 0 : index];
  }

  return (
    <SortItem ref={ref} style={{ opacity: isDragging ? 0 : 1 }}>
      <MemberName>{children}</MemberName>
      <Button
        type={order !== 'none' ? 'primary' : undefined}
        onClick={() => onOrderChange(getNextOrder())}
      >
        {order.toUpperCase()}
      </Button>
    </SortItem>
  );
}
