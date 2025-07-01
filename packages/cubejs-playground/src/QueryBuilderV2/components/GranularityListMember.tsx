import { CalendarEditIcon, CalendarIcon, Text } from '@cube-dev/ui-kit';
import { useRef } from 'react';

import { MemberViewType } from '../types';

import { InstanceTooltipProvider } from './InstanceTooltipProvider';
import { ListMemberButton } from './ListMemberButton';

export interface GranularityListMemberProps {
  name: string;
  title?: string;
  isCustom?: boolean;
  isSelected: boolean;
  isMissing?: boolean;
  memberViewType?: MemberViewType;
  onToggle: () => void;
}

export function GranularityListMember(props: GranularityListMemberProps) {
  const { name, title, isCustom, isSelected, isMissing, memberViewType = 'name', onToggle } = props;
  const textRef = useRef<HTMLDivElement>(null);

  return (
    <InstanceTooltipProvider name={name} title={title} overflowRef={isCustom ? textRef : undefined}>
      <ListMemberButton
        icon={isCustom ? <CalendarEditIcon /> : <CalendarIcon />}
        data-member="timeDimension"
        isSelected={isSelected}
        mods={{ missing: isMissing }}
        onPress={onToggle}
      >
        <Text ref={textRef} ellipsis>
          {(memberViewType === 'name' ? name : title) ?? name}
        </Text>
      </ListMemberButton>
    </InstanceTooltipProvider>
  );
}
