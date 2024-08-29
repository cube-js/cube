import { Key } from 'react';
import { tasty, MenuTrigger, Menu, CalendarIcon } from '@cube-dev/ui-kit';
import { FilterOutlined, MoreOutlined } from '@ant-design/icons';
import { TCubeDimension, TCubeMeasure, TCubeSegment } from '@cubejs-client/core';

import { ListMemberButton } from './ListMemberButton';

const OptionButtonElement = tasty(ListMemberButton, {
  'aria-label': 'Options',
  icon: <MoreOutlined />,
  styles: {
    color: {
      '': '#dark-04',
      hovered: '#dark',
    },
    fill: {
      '': '#clear',
      hovered: '#white.3',
    },
    gridColumns: 'auto',
    placeContent: 'center',
    radius: '1r right',
    margin: '-.75x -1.5x -.75x 0',

    ButtonIcon: { fontSize: '16px' },
  },
});

interface ListMemberOptionButtonProps {
  member: TCubeMeasure | TCubeDimension | TCubeSegment;
  type: string;
  onAddFilter?: (name: string) => void;
  onAddDateRange?: (name: string) => void;
}

export function ListMemberOptionButton(props: ListMemberOptionButtonProps) {
  const { member, type, onAddFilter, onAddDateRange } = props;

  function onAction(action: Key) {
    switch (action) {
      case 'filter':
        onAddFilter?.(member.name as string);
        break;
      case 'date-range':
        onAddDateRange?.(member.name as string);
        break;
    }
  }

  const disabledKeys = [];

  if (!onAddFilter) {
    disabledKeys.push('filter');
  }

  if (!onAddDateRange) {
    disabledKeys.push('date-range');
  }

  return (
    <MenuTrigger>
      <OptionButtonElement data-member={type} />
      <Menu disabledKeys={disabledKeys} onAction={onAction}>
        <Menu.Item key="filter" icon={<FilterOutlined />}>
          Add filter
        </Menu.Item>
        <Menu.Item key="date-range" icon={<CalendarIcon />}>
          Add date range
        </Menu.Item>
      </Menu>
    </MenuTrigger>
  );
}
