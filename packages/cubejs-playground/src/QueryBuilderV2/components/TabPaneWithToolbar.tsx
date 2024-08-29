import { tasty } from '@cube-dev/ui-kit';
import { ReactNode } from 'react';

const ContainerElement = tasty({
  styles: {
    position: 'relative',
    display: 'grid',
    gridTemplateRows: 'min-content 1fr',
    placeContent: 'stretch',

    Toolbar: {
      display: 'flex',
      flow: 'row',
      placeContent: 'space-between',
      gap: '1x',
      top: '0',
      padding: '1x',
      fill: '#white',
      border: 'bottom',
    },

    Actions: {
      display: 'flex',
      flow: 'row',
      gap: '1x',
    },

    ExtraActions: {
      display: 'flex',
      flow: 'row',
      gap: '1x',
    },

    TabPane: {
      display: 'grid',
      position: 'relative',
    },
  },
});

interface TabPaneWithToolbarProps {
  actions?: ReactNode;
  extraActions?: ReactNode;
  children?: ReactNode;
}

export function TabPaneWithToolbar(props: TabPaneWithToolbarProps) {
  const { children, actions, extraActions } = props;

  return (
    <ContainerElement>
      {actions || extraActions ? (
        <div data-element="Toolbar">
          <div data-element="Actions">{actions}</div>
          <div data-element="ExtraActions">{extraActions}</div>
        </div>
      ) : (
        <div />
      )}
      <div data-element="TabPane">{children}</div>
    </ContainerElement>
  );
}
