import { tasty } from '@cube-dev/ui-kit';

import { AccordionProvider } from './AccordionProvider';
import { AccordionProps } from './types';
import { AccordionItem } from './AccordionItem';

const StyledAccordion = tasty({
  styles: { display: 'grid', width: '100%', flow: 'row' },
});

export function Accordion(props: AccordionProps) {
  const { children, isLazy, size, isSeparated, titleStyles, contentStyles } = props;

  return (
    <AccordionProvider
      isLazy={isLazy}
      size={size}
      isSeparated={isSeparated}
      titleStyles={titleStyles}
      contentStyles={contentStyles}
    >
      <StyledAccordion>{children}</StyledAccordion>
    </AccordionProvider>
  );
}

Accordion.Item = AccordionItem;
