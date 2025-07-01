import { Action, CloseIcon, tasty } from '@cube-dev/ui-kit';
import { TCubeMemberType } from '@cubejs-client/core';

import { getTypeIcon } from '../utils';
import { MemberViewType } from '../types';
import { useShownMemberName } from '../hooks';

import { MemberLabelText } from './MemberLabelText';

const FilterMemberElement = tasty({
  styles: {
    padding: {
      '': '.75x 1x',
      '[data-size="small"]': '.25x .5x',
    },
    display: 'flex',
    justifyContent: 'space-between',
    position: 'relative',
    radius: true,
    fill: {
      '': '#border',
      '[data-member="measure"]': '#measure-active',
      '[data-member="dimension"]': '#dimension-active',
      '[data-member="timeDimension"]': '#time-dimension-active',
      '[data-member="segment"]': '#segment-active',
    },
  },
});

interface FilterLabelProps {
  type: TCubeMemberType | 'filter';
  member?: 'measure' | 'dimension' | 'timeDimension' | 'segment';
  isCompact?: boolean;
  isMissing?: boolean;
  name: string;
  memberName?: string;
  memberTitle?: string;
  cubeName?: string;
  cubeTitle?: string;
  memberViewType?: MemberViewType;
  size?: 'small' | 'normal';
  hideIcon?: boolean;
  onRemove?: () => Promise<void>;
}

export function FilterLabel(props: FilterLabelProps) {
  const {
    type,
    isMissing,
    member,
    name,
    cubeName = props.name.split('.')[0],
    cubeTitle,
    memberName = props.name.split('.')[1],
    memberTitle,
    memberViewType,
    isCompact,
    size = 'normal',
    hideIcon = false,
    onRemove,
  } = props;

  const { shownMemberName, shownCubeName } = useShownMemberName({
    memberName,
    memberTitle,
    cubeName,
    cubeTitle,
    type: memberViewType,
  });

  return (
    <FilterMemberElement data-member={isMissing ? 'missing' : member} data-size={size}>
      <MemberLabelText data-member={isMissing ? 'missing' : member} data-size={size}>
        {!hideIcon ? getTypeIcon(type) : null}
        <span data-element="MemberPath">
          {!isCompact ? (
            <>
              <span data-element="CubeName">{shownCubeName}</span>
              <span data-element="Divider">{memberViewType === 'name' ? '.' : <>&nbsp;</>}</span>
            </>
          ) : undefined}
          <span data-element="MemberName">{shownMemberName}</span>
        </span>
      </MemberLabelText>

      {onRemove ? (
        <Action preset="t4" onPress={onRemove}>
          <CloseIcon />
        </Action>
      ) : null}
    </FilterMemberElement>
  );
}
