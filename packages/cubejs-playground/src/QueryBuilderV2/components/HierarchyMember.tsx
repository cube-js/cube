import { Text, tasty, HierarchyIcon, Space } from '@cube-dev/ui-kit';
import { Cube } from '@cubejs-client/core';
import { ReactElement, useRef } from 'react';

import { MemberViewType, TCubeHierarchy } from '../types';
import { useShownMemberName } from '../hooks';
import { ChevronIcon } from '../icons/ChevronIcon';

import { InstanceTooltipProvider } from './InstanceTooltipProvider';
import { ListMemberButton } from './ListMemberButton';
import { FilteredLabel } from './FilteredLabel';

export interface FolderProps {
  cube: Cube;
  isOpen?: boolean;
  onToggle: (isOpen: boolean, name: string) => void;
  filterString?: string;
  member: TCubeHierarchy;
  memberViewType?: MemberViewType;
  children?: ReactElement[];
  count?: number;
}

const HierarchyElement = tasty({
  styles: {
    display: 'flex',
    flow: 'column',
    gap: '1bw',

    Contents: {
      display: 'flex',
      position: 'relative',
      margin: '4x left',
      flow: 'column',
      gap: '1bw',
    },

    HierarchyLine: {
      position: 'absolute',
      inset: '0 auto 0 (1bw - 2x)',
      fill: '#dimension-active',
      width: '.25x',
      radius: true,
    },

    Extra: {
      display: 'grid',
    },
  },
});

export function HierarchyMember(props: FolderProps) {
  const { isOpen, onToggle, cube, memberViewType, member, filterString, children } = props;

  const textRef = useRef<HTMLDivElement>(null);
  const name = member.name.replace(`${cube.name}.`, '').trim();
  const title = 'title' in member ? member.title : undefined;

  const { shownMemberName } = useShownMemberName({
    cubeName: cube.name,
    cubeTitle: 'title' in cube ? cube.title : undefined,
    memberName: name,
    memberTitle: title,
    type: memberViewType,
  });

  return (
    <HierarchyElement mods={{ open: isOpen }}>
      <InstanceTooltipProvider
        name={name}
        fullName={member.name}
        type="hierarchy"
        title={title}
        overflowRef={textRef}
      >
        <ListMemberButton
          qa="MemberButton"
          qaVal={member.name}
          icon={<HierarchyIcon />}
          data-member="dimension"
          onPress={() => onToggle?.(!isOpen, member.name)}
        >
          <Space gap=".75x">
            <Text ref={textRef} ellipsis>
              {filterString ? (
                <FilteredLabel text={shownMemberName} filter={filterString} />
              ) : (
                shownMemberName
              )}
            </Text>
            <ChevronIcon
              direction={isOpen ? 'top' : 'bottom'}
              color="var(--dimension-text-color)"
            />
          </Space>
        </ListMemberButton>
      </InstanceTooltipProvider>
      {children && children.length ? (
        <div data-element="Contents">
          {children}
          <div data-element="HierarchyLine" />
        </div>
      ) : null}
    </HierarchyElement>
  );
}
