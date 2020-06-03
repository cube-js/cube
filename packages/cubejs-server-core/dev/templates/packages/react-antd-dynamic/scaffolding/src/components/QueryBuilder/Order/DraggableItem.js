import React, { useRef, useEffect } from 'react';
import { Button } from 'antd';
import { SortDescendingOutlined, SortAscendingOutlined } from '@ant-design/icons';
import { useDrag, useDrop } from 'react-dnd';

import { TYPE } from './OrderGroup';

const orderOptions = ['asc', 'desc', 'none'];

export default function DraggableItem({ id, children, index, order = 'none', moveTag, onOrderChange }) {
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
      moveTag(dragIndex, hoverIndex);

      item.index = hoverIndex;
    },
  });

  const [{ isDragging }, drag] = useDrag({
    item: {
      id,
      type: TYPE,
      index,
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
    <div className="sort-item" ref={ref} style={{ opacity: isDragging ? 0 : 1 }}>
      <div className="member-name">{children}</div>
      <Button
        type={order !== 'none' ? 'primary' : undefined}
        icon={order === 'desc' ? <SortDescendingOutlined /> : <SortAscendingOutlined />}
        onClick={() => onOrderChange(getNextOrder())}
      />
    </div>
  );
}
