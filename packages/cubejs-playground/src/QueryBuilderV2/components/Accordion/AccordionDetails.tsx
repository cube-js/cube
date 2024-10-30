import { memo, ReactNode, useLayoutEffect, useRef, useState } from 'react';
import { CSSTransition } from 'react-transition-group';
import { Flex, Styles, tasty } from '@cube-dev/ui-kit';
import styled from 'styled-components';

import { AccordionItemProps, AccordionProps } from './types';

type AccordionDetailsProps = {
  children: AccordionItemProps['children'];
  isLazy?: boolean;
  size?: AccordionProps['size'];
  styles?: Styles;
  isExpanded?: boolean;
  isSeparated?: boolean;
};

const ACCORDION_CONTENT_HEIGHT_VARIABLE = '--accordion-content-height';
const ANIMATION_TIMEOUT = 180;

const AccordionDetailsContentInnerElement = tasty(Flex, {
  styles: {
    padding: {
      '': '1x 0 3x 3x',
      '[data-size="small"]': '0 0 0 3x',
      '[data-size="small"] & separated': '0 0 1x 3x',
    },
    gap: 0,
    flow: 'column',
  },
});

const AccordionDetailsContent = memo(styled.div<{ $expanded: boolean }>`
  ${ACCORDION_CONTENT_HEIGHT_VARIABLE}: 0;

  height: ${({ $expanded }) => ($expanded ? 'auto' : '0')};
  opacity: ${({ $expanded }) => ($expanded ? 1 : 0)};

  transition-property: height, opacity;
  transition-duration: ${ANIMATION_TIMEOUT}ms;
  transition-timing-function: cubic-bezier(0.42, 0.7, 0.82, 1);

  &.cube-accordion-transition {
    &-enter {
      opacity: 0;
      height: 0;
      contain: size layout style paint;
      will-change: height, opacity;
    }
    &-enter-active {
      opacity: 1;
      height: var(${ACCORDION_CONTENT_HEIGHT_VARIABLE});
      contain: size layout style paint;
      will-change: height, opacity;
    }
    &-exit {
      opacity: 1;
      height: var(${ACCORDION_CONTENT_HEIGHT_VARIABLE});
      contain: size layout style paint;
      will-change: height, opacity;
    }
    &-exit-active {
      opacity: 0;
      height: 0;
      contain: size layout style paint;
      will-change: height, opacity;
    }
  }
`);
export const AccordionDetails = memo(function AccordionDetails(
  props: AccordionDetailsProps
): JSX.Element {
  const { children, isLazy, size, isSeparated, isExpanded = false, styles } = props;

  const [innerExpandingState, setInnerExpandingState] = useState<
    'expanded' | 'collapsed' | 'collapsing' | 'expanding'
  >(isExpanded ? 'expanded' : 'collapsed');

  const accordionContentRef = useRef<HTMLDivElement>(null);
  const isLazyChildren = isLazy || typeof children === 'function';

  const content = (() => {
    if (isLazyChildren) {
      if (isExpanded || innerExpandingState === 'collapsing') {
        return renderChildren(children);
      }

      if (innerExpandingState === 'collapsed') {
        return null;
      }
    }

    return renderChildren(children);
  })();

  useLayoutEffect(() => {
    accordionContentRef.current?.style.setProperty(
      ACCORDION_CONTENT_HEIGHT_VARIABLE,
      `${accordionContentRef.current.scrollHeight}px`
    );
  }, [isExpanded]);

  return (
    <CSSTransition
      in={isExpanded}
      timeout={ANIMATION_TIMEOUT}
      classNames="cube-accordion-transition"
      onEnter={() => setInnerExpandingState('expanding')}
      onEntered={() => setInnerExpandingState('expanded')}
      onExiting={() => setInnerExpandingState('collapsing')}
      onExited={() => setInnerExpandingState('collapsed')}
    >
      <AccordionDetailsContent ref={accordionContentRef} $expanded={isExpanded}>
        <AccordionDetailsContentInnerElement
          data-size={size}
          mods={{ separated: isSeparated }}
          styles={styles}
        >
          {content}
        </AccordionDetailsContentInnerElement>
      </AccordionDetailsContent>
    </CSSTransition>
  );
});

function renderChildren(children: AccordionDetailsProps['children']): ReactNode {
  return typeof children === 'function' ? children() : children;
}
