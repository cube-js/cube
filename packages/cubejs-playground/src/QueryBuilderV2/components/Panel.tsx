import {
  BASE_STYLES,
  BaseProps,
  BaseStyleProps,
  BLOCK_STYLES,
  BlockStyleProps,
  COLOR_STYLES,
  ColorStyleProps,
  OUTER_STYLES,
  OuterStyleProps,
  Styles,
  tasty,
} from '@cube-dev/ui-kit';
import { ForwardedRef, forwardRef, ReactNode, useMemo } from 'react';

const PanelElement = tasty({
  as: 'section',
  qa: 'Panel',
  styles: {
    position: {
      '': 'relative',
      'stretched | floating': 'absolute',
    },
    inset: {
      '': 'initial',
      stretched: true,
    },
    display: 'block',
    overflow: 'hidden',
    radius: {
      '': '0',
      card: '1r',
    },
    border: {
      '': '0',
      card: '1bw',
    },
    flexGrow: 1,
  },
});

const PanelInnerElement = tasty({
  styles: {
    position: 'absolute',
    display: 'grid',
    top: 0,
    left: 0,
    right: 0,
    bottom: 0,
    overflow: 'auto',
    styledScrollbar: true,
    gridColumns: 'minmax(100%, 100%)',
    gridRows: {
      '': 'initial',
      stretched: 'minmax(0, 1fr)',
    },
    radius: {
      '': '0',
      card: '(1r - 1bw)',
    },
    flow: 'row',
    placeContent: 'start stretch',
  },
  styleProps: [...OUTER_STYLES, ...BASE_STYLES, ...COLOR_STYLES],
});

interface CubePanelProps
  extends OuterStyleProps,
    BlockStyleProps,
    BaseStyleProps,
    ColorStyleProps,
    BaseProps {
  isStretched?: boolean;
  isCard?: boolean;
  isFloating?: boolean;
  styles?: Styles;
  innerStyles?: Styles;
  placeContent?: Styles['placeContent'];
  placeItems?: Styles['placeItems'];
  gridColumns?: Styles['gridTemplateColumns'];
  gridRows?: Styles['gridTemplateRows'];
  flow?: Styles['flow'];
  gap?: Styles['gap'];
  children?: ReactNode;
}

const STYLES = [
  'placeContent',
  'placeItems',
  'gridColumns',
  'gridRows',
  'flow',
  'gap',
  'padding',
  'overflow',
  'fill',
  'color',
  'preset',
] as const;

function Panel(props: CubePanelProps, ref: ForwardedRef<HTMLDivElement>) {
  let { qa, mods, isStretched, isFloating, isCard, styles, innerStyles, children } = props;

  STYLES.forEach((style) => {
    if (props[style]) {
      innerStyles = { ...innerStyles, [style]: props[style] };
    }
  });

  [...OUTER_STYLES, ...BASE_STYLES, ...BLOCK_STYLES, ...COLOR_STYLES].forEach((style) => {
    if (props[style]) {
      styles = { ...styles, [style]: props[style] };
    }
  });

  const appliedMods = useMemo(
    () => ({
      floating: isFloating,
      stretched: isStretched,
      card: isCard,
      ...mods,
    }),
    [isStretched, isCard, mods]
  );

  return (
    <PanelElement ref={ref} qa={qa} mods={appliedMods} styles={styles}>
      <PanelInnerElement mods={appliedMods} styles={innerStyles}>
        {children}
      </PanelInnerElement>
    </PanelElement>
  );
}

const _Panel = forwardRef(Panel);

export { _Panel as Panel };
