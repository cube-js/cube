import { Flow, tasty } from '@cube-dev/ui-kit';

export const ScrollableArea = tasty(Flow, {
  styles: {
    overflow: 'auto',
    styledScrollbar: true,
  },
});
