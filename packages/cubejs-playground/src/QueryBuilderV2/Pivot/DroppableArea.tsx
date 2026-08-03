import { memo } from 'react';
import { Flow, tasty } from '@cube-dev/ui-kit';
import { Droppable } from 'react-beautiful-dnd';

import { QueryBuilderContextProps } from '../types';

import { PivotItem } from './Item';

const HeaderElement = tasty({
  styles: {
    display: 'grid',
    preset: 'h6',
    color: '#dark',
    placeContent: 'center',
    fill: '#light',
    padding: '8px 16px',
    border: 'bottom',
  },
});

const Header = memo(({ axis }: { axis: string }) => {
  return <HeaderElement>{axis.toUpperCase()} axis</HeaderElement>;
});

export function PivotDroppableArea({
  pivotConfig,
  axis,
}: {
  pivotConfig: QueryBuilderContextProps['pivotConfig'];
  axis: string;
}) {
  return (
    <>
      <Header axis={axis} />

      <div
        data-testid={`pivot-popover-${axis}`}
        style={{
          padding: '8px',
        }}
      >
        <Droppable droppableId={axis}>
          {(provided) => (
            <Flow ref={provided.innerRef} {...provided.droppableProps} gap="1ow">
              {/* @ts-ignore */}
              {pivotConfig[axis].map((id, index) => {
                let type: 'timeDimension' | 'dimension' | 'measure' = id.includes('.')
                  ? id.split('.').length === 3
                    ? 'timeDimension'
                    : 'dimension'
                  : 'measure';

                return <PivotItem key={id} type={type} id={id} index={index} />;
              })}

              {provided.placeholder}
            </Flow>
          )}
        </Droppable>
      </div>
    </>
  );
}
