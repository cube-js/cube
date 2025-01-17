import { Block, Card } from '@cube-dev/ui-kit';

import { Accordion, AccordionItemProps } from '../Accordion';

export interface AccordionCardProps extends AccordionItemProps {
  noPadding?: boolean;
  qa?: string;
}

const TITLE_STYLES = {
  padding: '0 1x',
  height: '6x',
};

const CONTENT_STYLES = {
  padding: 0,
};

export function AccordionCard(props: AccordionCardProps) {
  const { qa, children, noPadding, ...restProps } = props;

  return (
    <Card padding="0" border={false}>
      <Accordion
        isSeparated={false}
        size="small"
        titleStyles={TITLE_STYLES}
        contentStyles={CONTENT_STYLES}
      >
        <Accordion.Item key="card" qa={qa} {...restProps}>
          <Block>{typeof children === 'function' ? children() : children}</Block>
        </Accordion.Item>
      </Accordion>
    </Card>
  );
}
