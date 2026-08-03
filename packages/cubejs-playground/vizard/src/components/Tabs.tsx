import { ReactNode, createContext, useContext, useCallback } from 'react';
import { Action, tasty } from '@cube-dev/ui-kit';
import { CloseIcon } from '../icons/CloseIcon';

interface TabsContextValue {
  type?: 'default' | 'card';
  activeKey?: string;
  onChange: (key: string) => void;
  onDelete?: (key: string) => void;
}

const TabsContext = createContext<TabsContextValue | undefined>(undefined);

const TabsElement = tasty({
  styles: {
    display: 'grid',
    gridAutoFlow: 'column',
    gap: '1x',
    placeContent: 'start',
    overflow: 'auto',
    padding: '1x 1x 0 1x',
    shadow: 'inset 0 -1bw 0 #border',

    '--border-color': 'rgb(234,234,238)',
    '--light-purple-color': 'rgb(243,243,255)',
  },
});

const TabContainer = tasty({
  styles: {
    position: 'relative',
    display: 'grid',
  },
});

const TabElement = tasty(Action, {
  styles: {
    preset: 't3m',
    display: 'grid',
    flow: 'column',
    gap: '1x',
    placeContent: 'space-between',
    placeItems: 'center start',
    padding: {
      '': '1x 1.5x',
      deletable: '1x 4.5x 1x 1.5x',
    },
    cursor: 'pointer',
    border: {
      '': 'none',
      card: true,
    },
    fill: {
      '': '#white',
      card: '#light',
      'hovered && card': '#light-purple',
      active: '#white',
    },
    color: {
      '': '#dark-02',
      active: '#purple-text',
      '!card & hovered': '#purple',
    },
    borderBottom: {
      '': 'none',
      card: 'none',
    },
    shadow: {
      '': 'inset 0 -1bw 0 #border',
      active: 'inset 0 -1ow 0 #purple',
      'card & active': 'inset 0 -1bw 0 #white',
    },
    radius: 'top',
    backgroundClip: 'border-box',
    width: 'max 100%',
    transition: 'theme, borderBottom',
    whiteSpace: 'nowrap',
  },
});

const TabCloseButton = tasty(Action, {
  'aria-label': 'Delete tab',
  styles: {
    position: 'absolute',
    top: '1x',
    right: '1x',
    display: 'grid',
    placeItems: 'center',
    padding: '.5x',
    cursor: 'pointer',
    fontSize: '12px',
    fill: {
      '': '#clean',
      hovered: '#purple.04',
    },
    color: {
      '': '#dark-03',
      'hovered | pressed': '#purple',
    },
    radius: true,
    transition: 'theme',
  },
  children: <CloseIcon />,
});

interface TabsProps extends TabsContextValue {
  label?: string;
  children?: ReactNode;
}

interface TabProps {
  id: string;
  title: string;
  children?: ReactNode;
}

export function Tabs(props: TabsProps) {
  const { label, activeKey, type, onChange, onDelete, children } = props;

  const isCardType = type === 'card';

  return (
    <TabsContext.Provider value={{ activeKey, onChange, onDelete, type }}>
      <TabsElement aria-label={label ?? 'Tabs'} mods={{ card: isCardType }}>
        {children}
      </TabsElement>
    </TabsContext.Provider>
  );
}

export function Tab(props: TabProps) {
  const { title, id } = props;
  const { activeKey, type, onChange, onDelete } = useContext(TabsContext) || {};

  const isActive = id === activeKey;

  const onDeleteCallback = useCallback(() => {
    onDelete?.(id);
  }, [onDelete, id]);
  const onChangeCallback = useCallback(() => {
    onChange?.(id);
  }, [id]);

  const isCardType = type === 'card';
  const isDeletable = onDelete && isCardType;

  return (
    <TabContainer>
      <TabElement
        onPress={onChangeCallback}
        mods={{
          active: isActive,
          card: isCardType,
          deletable: isDeletable,
        }}
      >
        {title}
      </TabElement>
      {onDelete && <TabCloseButton onPress={onDeleteCallback} />}
    </TabContainer>
  );
}

Tabs.Tab = Tab;
