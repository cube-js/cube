import { tasty } from '@cube-dev/ui-kit';

export const MemberLabelText = tasty({
  qa: 'MemberLabel',
  'aria-label': 'Member label',
  styles: {
    display: 'grid',
    flow: 'column',
    gap: '.75x',
    preset: {
      '': 't3m',
      '[data-size="small"]': 't4',
    },
    color: {
      '': '#dark',
      '[data-member="measure"]': '#measure-text',
      '[data-member="dimension"]': '#dimension-text',
      '[data-member="timeDimension"]': '#time-dimension-text',
      '[data-member="segment"]': '#segment-text',
      '[data-member="filter"]': '#filter-text',
      '[data-member="missing"] | missing': '#danger-text',
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

    MemberPath: {
      overflow: 'hidden',
      textOverflow: 'ellipsis',
      // Fixes issue with `overflow: elipsis` not applying correctly
      maxWidth: '100%',
    },

    Divider: {
      padding: '0 1bw',
      color: '#dark.6',
    },

    '& svg': { placeSelf: 'center' },
  },
});
