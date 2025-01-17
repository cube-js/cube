import { Button, tasty } from '@cube-dev/ui-kit';

export const ListButton = tasty(Button, {
  type: 'clear',
  size: 'small',
  styles: {
    color: '#dark',
    opacity: {
      '': '1',
      disabled: '.5',
    },
    border: {
      '': '#clear',
      '[data-type="outline"]': '#purple.5',
      disabled: '#purple',
    },
    placeContent: 'space-between',
    gridTemplateColumns: 'auto 1fr auto',
    textAlign: 'left',
  },
});
