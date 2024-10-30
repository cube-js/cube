import { CalendarEditIcon, CalendarIcon, Text, TooltipProvider } from '@cube-dev/ui-kit';
import { useRef } from 'react';

import { useHasOverflow } from '../hooks/index';
import { titleize } from '../utils/index';

import { ListMemberButton } from './ListMemberButton';

export interface GranularityListMemberProps {
  name: string;
  title?: string;
  isCustom?: boolean;
  isSelected: boolean;
  onToggle: () => void;
}

export function GranularityListMember(props: GranularityListMemberProps) {
  const { name, title, isCustom, isSelected, onToggle } = props;
  const textRef = useRef<HTMLDivElement>(null);

  const hasOverflow = useHasOverflow(textRef);
  const isAutoTitle = titleize(name) === title;

  const button = (
    <ListMemberButton
      icon={isCustom ? <CalendarEditIcon /> : <CalendarIcon />}
      data-member="timeDimension"
      isSelected={isSelected}
      onPress={onToggle}
    >
      <Text ref={textRef} ellipsis>
        {name}
      </Text>
    </ListMemberButton>
  );

  if (hasOverflow || (!isAutoTitle && isCustom)) {
    return (
      <TooltipProvider
        title={
          <>
            <Text preset="t4">{name}</Text>
            <br />
            <Text preset="t3">{title}</Text>
          </>
        }
        delay={1000}
        placement="right"
      >
        {button}
      </TooltipProvider>
    );
  } else {
    return button;
  }
}
