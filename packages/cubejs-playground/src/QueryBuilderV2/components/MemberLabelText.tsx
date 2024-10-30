import { tasty } from '@cube-dev/ui-kit';

export const MemberLabelText = tasty({
  styles: {
    display: 'grid',
    flow: 'column',
    gap: '.5x',
    preset: 't3m',
    color: {
      '': '#dark',
      '[data-member="missing"]': '#danger-text',
      '[data-member="measure"]': '#measure-text',
      '[data-member="dimension"]': '#dimension-text',
      '[data-member="timeDimension"]': '#time-dimension-text',
      '[data-member="segment"]': '#segment-text',
      '[data-member="filter"]': '#filter-text',
    },
    whiteSpace: 'nowrap',
    placeItems: 'center start',
    placeContent: 'center start',

    Name: {
      display: 'grid',
      flow: 'column',
      overflow: 'hidden',
      textOverflow: 'ellipsis',
    },

    CubeName: {
      color: '#dark',
    },

    MemberName: {
      overflow: 'hidden',
      textOverflow: 'ellipsis',
    },

    Divider: {
      padding: '0 1bw',
      color: '#dark.6',
    },

    '& svg': { placeSelf: 'center' },
  },
});
