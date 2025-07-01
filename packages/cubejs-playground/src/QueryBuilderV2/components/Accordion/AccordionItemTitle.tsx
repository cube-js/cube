import { memo, PropsWithChildren, ReactText, useState } from 'react';
import { useFocus, useFocusVisible, useFocusWithin, useHover, usePress } from 'react-aria';
import { mergeProps, Styles, tasty, Text } from '@cube-dev/ui-kit';

import { Arrow } from '../Arrow';

import { AccordionItemProps, AccordionProps } from './types';

export type AccordionItemTitleProps = {
  qa?: string;
  title: AccordionItemProps['title'];
  subtitle: AccordionItemProps['subtitle'];
  extra: AccordionItemProps['extra'];
  showExtra: AccordionItemProps['showExtra'];
  size: AccordionProps['size'];
  styles?: Styles;
  onExpand: () => void;
  isExpanded: boolean;
  contentID: string;
  titleID: string;
};

const StyledAccordionItemTitleWrap = tasty({
  styles: {
    display: 'grid',
    gridColumns: {
      '': '1fr auto',
      subtitle: 'auto 1fr auto',
    },
    placeItems: 'center start',
    gap: '0',
    width: '100%',
    borderRadius: { '': 0, focused: '0.5x' },
    outline: { '': '#purple-04.0', focused: '#purple-04' },
  },
});

const StyledAccordionItemTitle = memo(
  tasty({
    styles: {
      display: 'grid',
      width: '100%',
      gridTemplateAreas: '"icon . title ."',
      gridTemplateColumns: '2x 1x auto 1fr',
      alignItems: 'center',
      padding: {
        '': '1.75x 1x 1.75x 0',
        '[data-size="small"]': '0.5x 1x 0.5x 0',
      },
      cursor: 'pointer',
      userSelect: 'none',
    },
  })
);
const TitleSection = tasty({
  styles: { gridArea: 'title', width: 'max 100%', overflow: 'hidden' },
});
const ExtraSection = tasty({
  styles: {
    display: 'flex',
    alignItems: 'center',

    opacity: { '': 0, show: 1 },
    transition: 'opacity 0.2s ease-out',
  },
});
const ExpandArrowSection = tasty({
  styles: {
    display: 'grid',
    placeContent: 'center',
    gridArea: 'icon',
    width: '2x',
    height: '2x',
    fontSize: '2x',
    transform: { '': 'rotate(0)', expanded: 'rotate(-90deg)' },
    transition: 'transform 0.2s ease-out',
    transformOrigin: 'center',
  },
});

export function AccordionItemTitle(props: AccordionItemTitleProps) {
  const {
    qa,
    title,
    subtitle,
    extra,
    onExpand,
    isExpanded,
    contentID,
    titleID,
    size,
    showExtra,
    styles,
  } = props;

  const [isFocusWithin, setIsFocusWithin] = useState(false);
  const [isFocused, setIsFocused] = useState(false);

  const { hoverProps, isHovered } = useHover({});
  const { isFocusVisible } = useFocusVisible({});
  const { focusProps } = useFocus({ onFocusChange: setIsFocused });
  const { focusWithinProps } = useFocusWithin({
    onFocusWithinChange: setIsFocusWithin,
  });
  const { pressProps } = usePress({ onPress: onExpand });

  const shouldShowFocus = isFocusVisible && isFocused;
  const hasUserHovered = isHovered || shouldShowFocus || (isFocusWithin && isFocusVisible);

  return (
    <StyledAccordionItemTitleWrap
      qa={qa}
      data-size={size}
      aria-labelledby={titleID}
      mods={{ subtitle: !!subtitle }}
      styles={styles}
      {...hoverProps}
    >
      <StyledAccordionItemTitle
        {...mergeProps(focusProps, pressProps, focusWithinProps)}
        mods={{ focused: shouldShowFocus }}
        aria-expanded={isExpanded}
        aria-controls={contentID}
        role="button"
        tabIndex="0"
        data-size={size}
      >
        <AccordionItemIcon isExpanded={isExpanded} />
        <AccordionItemContent title={title} id={titleID} />
      </StyledAccordionItemTitle>
      {subtitle ? <div data-element="Subtitle">{subtitle}</div> : null}

      <AccordionItemExtra showExtra={showExtra} isHovered={hasUserHovered}>
        {extra}
      </AccordionItemExtra>
    </StyledAccordionItemTitleWrap>
  );
}

const AccordionItemIcon = memo(function StyledAccordionItemIcon(props: { isExpanded: boolean }) {
  const { isExpanded } = props;

  return (
    <ExpandArrowSection mods={{ expanded: isExpanded }}>
      <Arrow direction="right" />
    </ExpandArrowSection>
  );
});

const AccordionItemContent = memo(function AccordionItemContent(props: {
  id: string;
  title: ReactText;
}) {
  const { id, title } = props;

  return (
    <TitleSection id={id}>
      <Text key="text" ellipsis preset="h6">
        {title}
      </Text>
    </TitleSection>
  );
});

function AccordionItemExtra(
  props: PropsWithChildren<{
    showExtra: AccordionItemProps['showExtra'];
    isHovered: boolean;
  }>
) {
  const { children, showExtra, isHovered } = props;

  if (!children) {
    return null;
  }

  const show = shouldShowExtra(showExtra, isHovered);

  return <ExtraSection mods={{ show }}>{children}</ExtraSection>;
}

function shouldShowExtra(showExtra: AccordionItemProps['showExtra'], isHovered: boolean) {
  if (typeof showExtra === 'boolean') {
    return showExtra;
  }

  return isHovered;
}
