import { CubeButtonProps, tasty, TooltipProvider } from '@cube-dev/ui-kit';
import { FilterFilled, FilterOutlined } from '@ant-design/icons';
import { memo } from 'react';

import { ListMemberButton } from './ListMemberButton';

const OptionButtonElement = tasty(ListMemberButton, {
  'aria-label': 'Options',
  styles: {
    width: '3.5x',
    color: {
      '': '#measure-text-color',
      'filtered & [data-member="timeDimension"]': '#time-dimension-text',
      'filtered & [data-member="dimension"]': '#dimension-text',
      'filtered & [data-member="measure"]': '#measure-text',
    },
    opacity: {
      '': '.75',
      hovered: '1',
      filtered: '1',
    },
    fill: {
      '': '#clear',
      hovered: '#white.3',
    },
    gridColumns: 'auto',
    placeContent: 'center',
    placeItems: 'center',
    radius: {
      '': 0,
      angular: '1r right',
    },
    margin: {
      '': '-.75x 0 -.75x 0',
      angular: '-.75x -.75x -.75x 0',
    },
    padding: 0,

    ButtonIcon: { fontSize: '16px' },
  },
});

interface ListMemberOptionButtonProps {
  type: string;
  isAngular?: boolean;
  isFiltered?: boolean;
  color?: CubeButtonProps['color'];
  onPress?: () => void;
}

export const FilterByMemberButton = memo((props: ListMemberOptionButtonProps) => {
  const { type, isFiltered, isAngular, color, onPress } = props;

  return (
    <TooltipProvider
      title={isFiltered ? 'There is a filter for this member' : 'Add a filter for this member'}
      delay={1000}
    >
      <OptionButtonElement
        icon={isFiltered ? <FilterFilled /> : <FilterOutlined />}
        color={color}
        mods={{ filtered: isFiltered, angular: isAngular }}
        data-member={type}
        onPress={onPress}
      />
    </TooltipProvider>
  );
});
