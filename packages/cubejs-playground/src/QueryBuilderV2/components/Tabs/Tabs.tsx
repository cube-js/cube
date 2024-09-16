import { ReactNode, createContext, useContext, useCallback, useState, useEffect } from 'react';
import { Action, tasty, CloseIcon, Styles } from '@cube-dev/ui-kit';

interface TabsContextValue {
  type?: 'default' | 'card';
  size?: 'normal' | 'large';
  activeKey?: string;
  extra?: ReactNode;
  setContent: (content?: ReactNode) => void;
  onChange: (key: string) => void;
  onDelete?: (key: string) => void;
}

const TabsContext = createContext<TabsContextValue | undefined>(undefined);

const TabsElement = tasty({
  styles: {
    display: 'grid',
    gridColumns: 'max-content max-content',
    placeContent: 'stretch space-between',
    placeItems: 'end',
    overflow: 'auto',
    shadow: 'inset 0 -1bw 0 #border',
    width: '100%',
    padding: '0 2x',

    Container: {
      display: 'grid',
      gridAutoFlow: 'column',
      gap: '0',
      placeContent: 'start',
    },

    Extra: {
      placeSelf: 'center',
    },

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
    preset: {
      '': 't3m',
      '[data-size="large"]': 't2m',
    },
    display: 'grid',
    flow: 'column',
    gap: '1x',
    placeContent: 'space-between',
    placeItems: 'center start',
    padding: '1.25x @delete-padding 1.25x 1.5x',
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
      hovered: '#purple',
      active: '#purple-text',
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

    '@delete-padding': {
      '': '1.5x',
      deletable: '4.5x',
    },
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

interface TabsProps extends Omit<TabsContextValue, 'setContent'> {
  label?: string;
  children?: ReactNode;
  styles?: Styles;
  size?: TabsContextValue['size'];
}

interface TabProps {
  id: string;
  title: ReactNode;
  children?: ReactNode;
  isDisabled?: boolean;
  qa?: string;
  styles?: Styles;
  size?: TabsContextValue['size'];
  extra?: ReactNode;
}

export function Tabs(props: TabsProps) {
  const [content, setContent] = useState<ReactNode>(null);
  const { label, activeKey, size, type, onChange, onDelete, children, styles, extra } = props;

  const isCardType = type === 'card';

  return (
    <TabsContext.Provider value={{ activeKey, onChange, onDelete, type, size, setContent }}>
      <TabsElement
        qa="Tabs"
        aria-label={label ?? 'Tabs'}
        data-size={size ?? 'normal'}
        mods={{ card: isCardType }}
        styles={styles}
      >
        <div data-element="Container">{children}</div>
        {extra ? <div data-element="Extra">{extra}</div> : null}
      </TabsElement>
      {content}
    </TabsContext.Provider>
  );
}

export function Tab(props: TabProps) {
  const { title, id, isDisabled, qa, styles, children } = props;
  const { activeKey, size, type, onChange, onDelete, setContent } = useContext(TabsContext) || {};

  const isActive = id === activeKey;

  const onDeleteCallback = useCallback(() => {
    onDelete?.(id);
  }, [onDelete, id]);
  const onChangeCallback = useCallback(() => {
    onChange?.(id);
  }, [id]);

  const isCardType = type === 'card';
  const isDeletable = onDelete && isCardType;

  useEffect(() => {
    if (isActive) {
      setContent?.(children || null);
    }
  }, [activeKey, children]);

  return (
    <TabContainer>
      <TabElement
        qa={`Tab-${id}` ?? qa}
        isDisabled={isDisabled}
        styles={styles}
        mods={{
          active: isActive,
          card: isCardType,
          deletable: isDeletable,
        }}
        data-size={size}
        onPress={onChangeCallback}
      >
        {title}
      </TabElement>
      {onDelete && <TabCloseButton onPress={onDeleteCallback} />}
    </TabContainer>
  );
}

Tabs.Tab = Tab;
