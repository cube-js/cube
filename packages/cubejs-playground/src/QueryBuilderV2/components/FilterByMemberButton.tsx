import { tasty, TooltipProvider } from '@cube-dev/ui-kit';
import { FilterFilled, FilterOutlined } from '@ant-design/icons';

import { ListMemberButton } from './ListMemberButton';

const OptionButtonElement = tasty(ListMemberButton, {
  'aria-label': 'Options',
  styles: {
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
    radius: '1r right',
    margin: '-.75x -1.5x -.75x 0',

    ButtonIcon: { fontSize: '16px' },
  },
});

interface ListMemberOptionButtonProps {
  type: string;
  isFiltered?: boolean;
  onPress?: () => void;
}

export function FilterByMemberButton(props: ListMemberOptionButtonProps) {
  const { type, isFiltered, onPress } = props;

  return (
    <TooltipProvider title="Add a filter for this member" delay={1000}>
      <OptionButtonElement
        icon={isFiltered ? <FilterFilled /> : <FilterOutlined />}
        mods={{ filtered: isFiltered }}
        data-member={type}
        onPress={onPress}
      />
    </TooltipProvider>
  );
}
