import { Draggable } from 'react-beautiful-dnd';
import { DragOutlined } from '@ant-design/icons';
import { tasty } from '@cube-dev/ui-kit';

import { MemberLabelText } from '../components/MemberLabelText';
import { MemberBadge } from '../components/Badge';

const PivotItemElement = tasty({
  styles: {
    display: 'grid',
    flow: 'row',
    gridColumns: 'min-content 1fr',
    gap: '1x',
    placeContent: 'center start',
    placeItems: 'center stretch',
    radius: true,
    padding: '.5x 1x',
    preset: 't3m',

    color: {
      '': '#dark',
      '[data-type="dimension"]': '#dimension-text',
      '[data-type="measure"]': '#measure-text',
      '[data-type="time-dimension"]': '#time-dimension-text',
    },
    fill: {
      '': '#dark.15',
      '[data-type="dimension"]': '#dimension-hover',
      '[data-type="measure"]': '#measure-hover',
      '[data-type="timeDimension"]': '#time-dimension-hover',
    },
  },
});

export function PivotItem({
  id,
  index,
  type,
}: {
  id: string;
  index: number;
  type: 'timeDimension' | 'dimension' | 'measure';
}) {
  return (
    <Draggable draggableId={id} index={index}>
      {({ draggableProps, dragHandleProps, innerRef }) => {
        const arr = id.split('.');

        return (
          // <TooltipProvider activeWrap title={id} tooltipStyles={{ width: 'auto' }}>
          <PivotItemElement
            ref={innerRef}
            data-type={type}
            {...draggableProps}
            {...dragHandleProps}
            style={draggableProps.style}
          >
            <DragOutlined />
            <MemberLabelText data-member={type}>
              {arr.length > 1 ? (
                <>
                  <span data-element="Name">
                    <span data-element="CubeName">{arr[0]}</span>
                    <span data-element="Divider">.</span>
                    <span data-element="MemberName">{arr[1]}</span>
                  </span>
                  {arr[2] ? (
                    <span data-element="Grouping">
                      <MemberBadge isSpecial type={type}>
                        {arr[2]}
                      </MemberBadge>
                    </span>
                  ) : null}
                </>
              ) : (
                <span data-element="MemberName">{id}</span>
              )}
            </MemberLabelText>
          </PivotItemElement>
          // </TooltipProvider>
        );
      }}
    </Draggable>
  );
}
