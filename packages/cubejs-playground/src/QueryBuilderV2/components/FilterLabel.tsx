import { tasty } from '@cube-dev/ui-kit';
import { TCubeMemberType } from '@cubejs-client/core';

import { getTypeIcon } from '../utils';

import { MemberLabelText } from './MemberLabelText';

const FilterMemberElement = tasty({
  styles: {
    padding: '.75x 1x',
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
}

export function FilterLabel(props: FilterLabelProps) {
  const { type, isMissing, member, name, isCompact } = props;

  return (
    <FilterMemberElement data-member={isMissing ? 'missing' : member}>
      <MemberLabelText data-member={isMissing ? 'missing' : member}>
        {getTypeIcon(type)}
        <span>
          {!isCompact ? (
            <>
              <span data-element="CubeName">{name.split('.')[0]}</span>
              <span data-element="Divider">.</span>
            </>
          ) : undefined}
          <span data-element="MemberName">{name.split('.')[1]}</span>
        </span>
      </MemberLabelText>
    </FilterMemberElement>
  );
}
