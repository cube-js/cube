import { useRef } from 'react';
import {
  Menu,
  MenuTrigger,
  Space,
  tasty,
  Text,
  CloseIcon,
  TooltipProvider,
} from '@cube-dev/ui-kit';
import { TCubeMeasure, TCubeDimension, TCubeSegment, Cube, MemberType } from '@cubejs-client/core';
import { PlusOutlined } from '@ant-design/icons';

import { getTypeIcon } from '../utils';
import { PrimaryKeyIcon } from '../icons/PrimaryKeyIcon';
import { NonPublicIcon } from '../icons/NonPublicIcon';
import { ItemInfoIcon } from '../icons/ItemInfoIcon';
import { useHasOverflow } from '../hooks/has-overflow';

import { ListMemberButton } from './ListMemberButton';
import { FilterByMemberButton } from './FilterByMemberButton';
import { FilteredLabel } from './FilteredLabel';

interface ListMemberProps {
  cube: Cube;
  member: TCubeMeasure | TCubeDimension | TCubeSegment;
  category: MemberType;
  filterString?: string;
  isSelected: boolean;
  isFiltered?: boolean;
  onToggle?: (name: string) => void;
  onAddFilter?: (name: string) => void;
  onRemoveFilter?: (name: string) => void;
}

const ListMemberWrapper = tasty({
  styles: {
    display: 'grid',
    position: 'relative',
  },
});

export function ListMember(props: ListMemberProps) {
  const textRef = useRef<HTMLDivElement>(null);
  const {
    cube,
    filterString,
    category,
    member,
    isSelected,
    isFiltered,
    onAddFilter,
    onRemoveFilter,
    onToggle,
  } = props;
  const type = 'type' in member ? member.type : 'string';
  const name = member.name.replace(`${cube.name}.`, '').trim();
  const title = member.title;
  // @ts-ignore
  const description = member.description;

  const hasOverflow = useHasOverflow(textRef);
  const button = (
    <ListMemberWrapper>
      <ListMemberButton
        qa="MemberButton"
        qaVal={member.name}
        icon={getTypeIcon(category === 'segments' ? 'filter' : type)}
        data-member={category.replace(/s$/, '')}
        mods={{ selected: isSelected }}
        onPress={() => onToggle?.(`${cube.name}.${member.name}`)}
      >
        <Text ref={textRef} ellipsis>
          {filterString ? <FilteredLabel text={name} filter={filterString} /> : name}
        </Text>
        <Space gap=".5x">
          <Space gap="1x" color="#dark.6">
            {description ? <ItemInfoIcon title={title} description={description} /> : undefined}
            {/* @ts-ignore */}
            {member.primaryKey ? <PrimaryKeyIcon color={'dark-02'} /> : undefined}
            {/* @ts-ignore */}
            {member.public === false ? <NonPublicIcon /> : undefined}
          </Space>
          {onAddFilter ? (
            isFiltered ? (
              <MenuTrigger>
                <FilterByMemberButton type={category.replace(/s$/, '')} isFiltered={true} />
                <Menu
                  onAction={(key) => {
                    switch (key) {
                      case 'add':
                        onAddFilter?.(member.name);
                        break;
                      case 'remove':
                        onRemoveFilter?.(member.name);
                        break;
                      default:
                        return;
                    }
                  }}
                >
                  <Menu.Item key="add" icon={<PlusOutlined style={{ fontSize: 16 }} />}>
                    Add an additional filter with this member
                  </Menu.Item>
                  <Menu.Item key="remove" icon={<CloseIcon color="danger-text" />}>
                    <Text color="#danger-text">Remove all filters associated with this member</Text>
                  </Menu.Item>
                </Menu>
              </MenuTrigger>
            ) : (
              <FilterByMemberButton
                type={category.replace(/s$/, '')}
                isFiltered={isFiltered || false}
                onPress={() =>
                  !isFiltered ? onAddFilter?.(member.name) : onRemoveFilter?.(member.name)
                }
              />
            )
          ) : undefined}
        </Space>
      </ListMemberButton>
    </ListMemberWrapper>
  );

  return hasOverflow ? (
    <TooltipProvider
      title={
        <>
          <b>{name}</b>
        </>
      }
      delay={1000}
      placement="right"
    >
      {button}
    </TooltipProvider>
  ) : (
    button
  );
}
