import { Badge, tasty } from '@cube-dev/ui-kit';
import { memo, ReactNode } from 'react';
import { QuestionCircleOutlined } from '@ant-design/icons';

const MemberBadgeElement = tasty(Badge, {
  styles: {
    display: 'inline-grid',
    flow: 'column',
    gap: '.5x',
    placeContent: 'center',
    color: {
      '': '#dark',
      '![data-member]': '#danger-text',
      'special | missing': '#white',
    },
    fill: {
      '': '#danger-text.15',
      '[data-member="dimension"]': '#dimension-active',
      '[data-member="measure"]': '#measure-active',
      '[data-member="timeDimension"]': '#time-dimension-active',
      '[data-member="segment"]': '#segment-active',
      '[data-member="filter"]': '#filter-active',
      '[data-member="dimension"] & special': '#dimension-text',
      '[data-member="measure"] & special': '#measure-text',
      '[data-member="timeDimension"] & special': '#time-dimension-text',
      '[data-member="segment"] & special': '#segment-text',
      '[data-member="filter"] & special': '#filter-text',
      missing: '#danger',
    },
    preset: 't4m',
    width: 'max-content',
    textOverflow: 'ellipsis',
    overflow: 'hidden',
    lineHeight: '16px',
  },
});

export const MemberBadge = memo(
  ({
    type,
    isSpecial,
    isMissing,
    children,
  }: {
    type?: 'measure' | 'dimension' | 'segment' | 'filter' | 'timeDimension';
    isSpecial?: boolean;
    isMissing?: boolean;
    children: ReactNode | number;
  }) => {
    return (
      <MemberBadgeElement
        data-member={type}
        mods={{ special: isSpecial || !Number.isNaN(Number(children)), missing: isMissing }}
        radius="1r"
      >
        {!type && <QuestionCircleOutlined style={{ fontSize: '13px' }} />}
        {children}
      </MemberBadgeElement>
    );
  }
);
