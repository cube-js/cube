import { useEffect, useMemo, useRef, useState } from 'react';
import { mergeStyles, tasty } from '@cube-dev/ui-kit';

import { useEvent, useIsFirstRender, useUniqID } from '../../hooks';

import { useAccordionNestedContext } from './AccordionNestedContext';
import { AccordionDetails } from './AccordionDetails';
import { AccordionItemTitle } from './AccordionItemTitle';
import { AccordionItemProps } from './types';
import { useAccordionContext } from './AccordionProvider';

const StyledAccordionItemContent = tasty({
  styles: {
    border: {
      '': false,
      separated: '1bw solid #dark-05 bottom',
    },
    overflow: 'hidden',
  },
});

export function AccordionItem(props: AccordionItemProps) {
  let {
    qa,
    isExpanded,
    isDefaultExpanded,
    onToggle,
    onExpand,
    onCollapse,
    extra,
    showExtra = true,
    title,
    subtitle,
    titleStyles,
    contentStyles,
    children,
  } = props;

  const contentRef = useRef<HTMLElement>(null);
  const accordionTreeContext = useAccordionNestedContext();

  const isFirstRender = useIsFirstRender();
  const {
    isLazy,
    size,
    isSeparated = true,
    titleStyles: sharedTitleStyles,
    contentStyles: sharedContentStyles,
  } = useAccordionContext();
  const isControllable = isExpanded !== undefined;
  let [expanded, setExpanded] = useState(isDefaultExpanded ?? false);

  expanded = (isControllable ? isExpanded : expanded) ?? false;

  titleStyles = useMemo(
    () => mergeStyles(titleStyles, sharedTitleStyles),
    [titleStyles, sharedTitleStyles]
  );
  contentStyles = useMemo(
    () => mergeStyles(contentStyles, sharedContentStyles),
    [contentStyles, sharedContentStyles]
  );

  const onExpandHandler = useEvent(() => {
    if (!isControllable) {
      setExpanded(!expanded);
    }

    onToggle?.(!expanded);
  });

  const contentID = useUniqID();
  const titleID = useUniqID();

  useEffect(() => {
    if (isFirstRender) {
      return;
    }

    if (!expanded) {
      onCollapse?.();
    } else {
      onExpand?.();
    }
  }, [expanded]);

  useEffect(() => {
    const registeredItem = {
      title,
      subtitle,
      expand: () => setExpanded(true),
      collapse: () => setExpanded(false),
    };

    if (accordionTreeContext) {
      accordionTreeContext.items.add(registeredItem);
    }

    return () => {
      if (accordionTreeContext) {
        accordionTreeContext.items.delete(registeredItem);
      }
    };
  }, []);

  return (
    <>
      <AccordionItemTitle
        qa={qa}
        titleID={titleID}
        contentID={contentID}
        isExpanded={expanded}
        title={title}
        subtitle={subtitle}
        extra={extra}
        showExtra={showExtra}
        size={size}
        styles={titleStyles}
        onExpand={onExpandHandler}
      />

      <StyledAccordionItemContent
        ref={contentRef}
        id={contentID}
        role="region"
        aria-labelledby={titleID}
        mods={{ expanded, separated: isSeparated }}
      >
        <AccordionDetails
          isExpanded={expanded}
          isLazy={isLazy}
          size={size}
          styles={contentStyles}
          isSeparated={isSeparated}
        >
          {children}
        </AccordionDetails>
      </StyledAccordionItemContent>
    </>
  );
}
