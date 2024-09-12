import { tasty } from '@cube-dev/ui-kit';

import { ListButton } from './ListButton';

export const ListMemberButton = tasty(ListButton, {
  styles: {
    placeContent: 'center start',
    color: '#text',
    fill: {
      '': '#clear',
      hovered: '#hover',
      selected: '#active',
      'selected & hovered': '#active.8',
    },

    '--text-color': {
      '': '#dark',
      '[data-member="measure"]': '#measure-text',
      '[data-member="dimension"]': '#dimension-text',
      '[data-member="timeDimension"]': '#time-dimension-text',
      '[data-member="segment"]': '#segment-text',
    },

    '--hover-color': {
      '': '#dark-05',
      '[data-member="measure"]': '#measure-hover',
      '[data-member="dimension"]': '#dimension-hover',
      '[data-member="timeDimension"]': '#time-dimension-hover',
      '[data-member="segment"]': '#segment-hover',
    },

    '--active-color': {
      '': '#dark',
      '[data-member="measure"]': '#measure-active',
      '[data-member="dimension"]': '#dimension-active',
      '[data-member="timeDimension"]': '#time-dimension-active',
      '[data-member="segment"]': '#segment-active',
    },
  },
});
