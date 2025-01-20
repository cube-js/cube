import { useMemo, useRef } from 'react';
import { Menu, MenuTrigger, Space, Text, CloseIcon, PlusIcon, CubeIcon } from '@cube-dev/ui-kit';
import { TCubeMeasure, TCubeDimension, TCubeSegment, Cube, MemberType } from '@cubejs-client/core';

import { getTypeIcon } from '../utils';
import { PrimaryKeyIcon } from '../icons/PrimaryKeyIcon';
import { NonPublicIcon } from '../icons/NonPublicIcon';
import { ItemInfoIcon } from '../icons/ItemInfoIcon';
import { MemberViewType } from '../types';
import { useEvent, useShownMemberName } from '../hooks';

import { ListMemberButton } from './ListMemberButton';
import { FilterByMemberButton } from './FilterByMemberButton';
import { FilteredLabel } from './FilteredLabel';
import { InstanceTooltipProvider } from './InstanceTooltipProvider';

interface ListMemberProps {
  cube: Cube | { name: string };
  member:
    | TCubeMeasure
    | TCubeDimension
    | TCubeSegment
    | { name: string; type?: 'string' | 'number' };
  isMissing?: boolean;
  category: MemberType;
  filterString?: string;
  isSelected: boolean;
  isFiltered?: boolean;
  isImported?: boolean;
  memberViewType?: MemberViewType;
  onToggle?: (name: string) => void;
  onAddFilter?: (name: string) => void;
  onRemoveFilter?: (name: string) => void;
}

export function ListMember(props: ListMemberProps) {
  const textRef = useRef<HTMLDivElement>(null);
  const {
    cube,
    filterString,
    category,
    member,
    memberViewType,
    isMissing,
    isSelected,
    isFiltered,
    isImported,
    onAddFilter,
    onRemoveFilter,
    onToggle,
  } = props;
  const type = 'type' in member ? member.type : undefined;
  const name = member.name.replace(`${cube.name}.`, '').trim();
  const title = 'shortTitle' in member ? member.shortTitle : undefined;
  // @ts-ignore
  const description = member.description;
  const { shownMemberName } = useShownMemberName({
    cubeName: cube.name,
    cubeTitle: 'title' in cube ? cube.title : undefined,
    memberName: name,
    memberTitle: title,
    type: memberViewType,
  });

  const onFilterPress = useEvent(() =>
    !isFiltered ? onAddFilter?.(member.name) : onRemoveFilter?.(member.name)
  );

  const filterMenu = useMemo(() => {
    const dangerProps = isFiltered
      ? {
          color: '#danger-text',
        }
      : {};

    return (
      <MenuTrigger>
        <FilterByMemberButton
          isAngular
          type={category.replace(/s$/, '')}
          isFiltered={true}
          {...(isMissing ? { color: '#danger-text' } : {})}
        />
        <Menu
          disabledKeys={isMissing ? ['add'] : []}
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
          <Menu.Item key="add" icon={<PlusIcon />}>
            Filter by This Member
          </Menu.Item>
          <Menu.Item key="remove" icon={<CloseIcon {...dangerProps} />}>
            <Text {...dangerProps}>Remove All</Text>
          </Menu.Item>
        </Menu>
      </MenuTrigger>
    );
  }, [category, isMissing, member.name, onAddFilter, onRemoveFilter, isFiltered]);

  return (
    <InstanceTooltipProvider
      name={name}
      fullName={member.name}
      type={category.replace(/s$/, '') as 'measure' | 'dimension' | 'segment'}
      title={title}
      overflowRef={textRef}
    >
      <ListMemberButton
        qa="MemberButton"
        qaVal={member.name}
        icon={getTypeIcon(category === 'segments' ? 'filter' : type)}
        data-member={category.replace(/s$/, '')}
        mods={{ selected: isSelected, missing: isMissing }}
        onPress={() => onToggle?.(member.name)}
      >
        <Text ref={textRef} ellipsis>
          {filterString ? (
            <FilteredLabel text={shownMemberName} filter={filterString} />
          ) : (
            shownMemberName
          )}
        </Text>
        <Space gap=".5x">
          {description ||
          isImported ||
          ('primaryKey' in member && member.primaryKey) ||
          ('public' in member && member.public === false) ? (
            <Space gap="1x" color="#dark.6">
              {description || isImported ? (
                <ItemInfoIcon
                  description={
                    isImported ? (
                      <>
                        {description ? (
                          <>
                            {description}
                            <br />
                            <br />
                          </>
                        ) : null}
                        <Text preset="t4">This member is imported from another cube:</Text>
                        <br />
                        <CubeIcon /> <b>{member.name.split('.')[0]}</b>
                      </>
                    ) : (
                      description
                    )
                  }
                />
              ) : undefined}
              {'primaryKey' in member && member.primaryKey ? (
                <PrimaryKeyIcon color={'dark-02'} />
              ) : undefined}
              {'public' in member && member.public === false ? <NonPublicIcon /> : undefined}
            </Space>
          ) : null}
          {onAddFilter || onRemoveFilter ? (
            isFiltered && !isMissing ? (
              filterMenu
            ) : (
              <FilterByMemberButton
                isAngular
                isFiltered={isFiltered || false}
                type={category.replace(/s$/, '')}
                onPress={onFilterPress}
              />
            )
          ) : undefined}
        </Space>
      </ListMemberButton>
    </InstanceTooltipProvider>
  );
}
