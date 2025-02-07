import { FocusableRefValue } from '@react-types/shared';
import {
  ReactNode,
  createContext,
  useContext,
  useState,
  useMemo,
  useLayoutEffect,
  useRef,
  useEffect,
} from 'react';
import { Action, tasty, CloseIcon, Styles } from '@cube-dev/ui-kit';

import { useEvent } from '../../hooks';

interface TabData {
  content: ReactNode;
  prerender: boolean;
  keepMounted: boolean;
}

interface TabsContextValue {
  type?: 'default' | 'card';
  size?: 'normal' | 'large';
  activeKey?: string;
  extra?: ReactNode;
  setTabContent: (id: string, content: TabData | null) => void;
  prerender?: boolean;
  keepMounted?: boolean;
  onChange: (key: string) => void;
  onDelete?: (key: string) => void;
}

interface TabsProps extends Omit<TabsContextValue, 'setTabContent'> {
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
  qaVal?: string;
  styles?: Styles;
  size?: TabsContextValue['size'];
  extra?: ReactNode;
  prerender?: boolean;
  keepMounted?: boolean;
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
    scrollbarWidth: 'none',

    Container: {
      display: 'grid',
      gridAutoFlow: 'column',
      gap: {
        '': 0,
        card: '1bw',
      },
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
    position: 'relative',
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
      'disabled & !active': '#dark-04',
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
    outline: false,

    '@delete-padding': {
      '': '1.5x',
      deletable: '4.5x',
    },

    '&::before': {
      content: '""',
      display: 'block',
      position: 'absolute',
      inset: '0 0 -1ow 0',
      pointerEvents: 'none',
      radius: 'top',
      shadow: {
        '': 'inset 0 0 0 #purple',
        focused: 'inset 0 0 0 1ow #purple-03',
      },
      transition: 'theme',
      zIndex: 1,
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

export function Tabs(props: TabsProps) {
  const [contentMap, setContentMap] = useState<Map<string, TabData>>(new Map());
  const {
    label,
    activeKey,
    size,
    type,
    onChange,
    onDelete,
    children,
    styles,
    extra,
    prerender,
    keepMounted,
  } = props;

  const isCardType = type === 'card';

  // Update the content map whenever the activeKey changes
  const setTabContent = useEvent((id: string, content: TabData | null) => {
    setContentMap((prev) => {
      const newMap = new Map(prev);
      if (content) {
        newMap.set(id, content);
      } else {
        newMap.delete(id);
      }

      return newMap;
    });
  });

  const mods = useMemo(() => ({ card: isCardType, deletable: !!onDelete }), [isCardType, onDelete]);

  return (
    <TabsContext.Provider
      value={{ activeKey, onChange, onDelete, type, size, setTabContent, prerender, keepMounted }}
    >
      <TabsElement
        qa="Tabs"
        aria-label={label ?? 'Tabs'}
        data-size={size ?? 'normal'}
        mods={mods}
        styles={styles}
      >
        <div data-element="Container">{children}</div>
        {extra ? <div data-element="Extra">{extra}</div> : null}
      </TabsElement>
      {[...contentMap.entries()].map(([id, { content, prerender, keepMounted }]) =>
        prerender || id === activeKey || keepMounted ? (
          <div
            key={id}
            data-qa="TabPanel"
            data-qaval={id}
            style={{
              display: id === activeKey ? 'contents' : 'none',
            }}
          >
            {content}
          </div>
        ) : null
      )}
    </TabsContext.Provider>
  );
}

export function Tab(props: TabProps) {
  let { title, id, isDisabled, prerender, keepMounted, qa, qaVal, styles, children } = props;

  const ref = useRef<FocusableRefValue>(null);

  const { activeKey, size, type, onChange, onDelete, setTabContent, ...contextProps } =
    useContext(TabsContext) || ({} as TabsContextValue);

  prerender = prerender ?? contextProps.prerender;
  keepMounted = keepMounted ?? contextProps.keepMounted;

  const isActive = id === activeKey;

  const onDeleteCallback = useEvent(() => {
    onDelete?.(id);
  });
  const onChangeCallback = useEvent(() => {
    onChange?.(id);
  });

  const isCardType = type === 'card';
  const isDeletable = !!onDelete;

  useLayoutEffect(() => {
    if (prerender || isActive) {
      setTabContent?.(id, {
        content: children,
        prerender: prerender ?? false,
        keepMounted: keepMounted ?? false,
      });
    } else if (!keepMounted) {
      setTabContent?.(id, null);
    }
  }, [children, isActive, keepMounted, prerender, setTabContent]);

  useLayoutEffect(() => {
    return () => {
      setTabContent?.(id, null);
    };
  }, []);

  const mods = useMemo(
    () => ({ card: isCardType, active: isActive, deletable: isDeletable, disabled: isDisabled }),
    [isCardType, isActive, isDeletable, isDisabled]
  );

  useEffect(() => {
    if (ref.current && isActive) {
      ref.current.UNSAFE_getDOMNode()?.scrollIntoView?.();
    }
  }, [isActive]);

  return (
    <TabContainer mods={mods}>
      <TabElement
        ref={ref}
        qa={qa ?? `Tab-${id}`}
        qaVal={qaVal}
        isDisabled={isDisabled}
        styles={styles}
        mods={mods}
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
