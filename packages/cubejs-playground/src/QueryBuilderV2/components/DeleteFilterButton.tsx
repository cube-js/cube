import { Button, CloseIcon, tasty } from '@cube-dev/ui-kit';

export const DeleteFilterButton = tasty(Button, {
  'aria-label': 'Delete this filter',
  size: 'small',
  type: 'secondary',
  theme: 'danger',
  icon: <CloseIcon />,
});
