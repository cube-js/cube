import { Block, Flex, Text, Title } from '@cube-dev/ui-kit';
import { ReactNode } from 'react';

/* @TODO: optimize styling */

interface MemberSectionProps {
  name: string;
  hasFilter?: boolean;
  totalShownItems: number;
  totalItems: number;
  children: ReactNode;
}

export function MemberSection({
  name,
  hasFilter,
  totalShownItems,
  totalItems,
  children,
}: MemberSectionProps) {
  return (
    <Flex gap="1x" flow="column">
      <Title level={6} preset="c2" fontWeight={600} color={`#${name}-text`}>
        {name}s{' '}
        {hasFilter ? (
          <Text color="#dark">
            ({totalShownItems}/{totalItems ?? 0})
          </Text>
        ) : undefined}
      </Title>
      <Flex gap="1bw" flow="column">
        {!totalItems ? (
          <Block padding="1.5x left" color="#minor">
            No {name}s
          </Block>
        ) : hasFilter && !totalShownItems ? (
          <Block padding="1.5x left" color="#minor">
            Nothing found
          </Block>
        ) : undefined}
        {children}
      </Flex>
    </Flex>
  );
}
