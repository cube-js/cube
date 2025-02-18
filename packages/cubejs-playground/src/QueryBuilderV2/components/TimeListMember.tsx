import { useMemo, useRef } from 'react';
import {
  CloseIcon,
  Flex,
  Menu,
  MenuTrigger,
  PlusIcon,
  Space,
  tasty,
  Text,
  TimeIcon,
} from '@cube-dev/ui-kit';
import { Cube, TCubeDimension, TimeDimensionGranularity } from '@cubejs-client/core';

import { ChevronIcon } from '../icons/ChevronIcon';
import { NonPublicIcon } from '../icons/NonPublicIcon';
import { ItemInfoIcon } from '../icons/ItemInfoIcon';
import { titleize } from '../utils/index';
import { PREDEFINED_GRANULARITIES } from '../values';
import { useEvent, useShownMemberName } from '../hooks';
import { MemberViewType } from '../types';

import { GranularityListMember } from './GranularityListMember';
import { ListMemberButton } from './ListMemberButton';
import { FilterByMemberButton } from './FilterByMemberButton';
import { FilteredLabel } from './FilteredLabel';
import { InstanceTooltipProvider } from './InstanceTooltipProvider';

const GranularitiesWrapper = tasty(Flex, {
  styles: {
    position: 'relative',
    flow: 'column',
    gap: '1bw',
    margin: '4x left',

    TimeListLine: {
      position: 'absolute',
      inset: '0 auto 0 (1bw - 2x)',
      fill: '#dimension-active',
      width: '.25x',
      radius: true,
    },
  },
});

interface TimeListMemberProps {
  cube: Cube | { name: string };
  isOpen?: boolean;
  member: TCubeDimension | { name: string; type: 'time' };
  filterString?: string;
  isCompact?: boolean;
  isSelected: (granularity?: TimeDimensionGranularity) => boolean;
  isFiltered: boolean;
  isDateRangeFiltered: boolean;
  isMissing?: boolean;
  selectedGranularities?: string[];
  memberViewType?: MemberViewType;
  onDimensionToggle: (component: string) => void;
  onGranularityToggle: (name: string, granularity: TimeDimensionGranularity) => void;
  onAddDataRange?: (name: string) => void;
  onRemoveDataRange?: (name: string) => void;
  onToggle: (isOpen: boolean, name: string) => void;
  onAddFilter?: (name: string) => void;
  onRemoveFilter?: (name: string) => void;
}

export function TimeListMember(props: TimeListMemberProps) {
  const textRef = useRef<HTMLDivElement>(null);

  const {
    isOpen,
    cube,
    member,
    filterString,
    isSelected,
    isFiltered,
    isDateRangeFiltered,
    isMissing,
    selectedGranularities = [],
    memberViewType,
    onDimensionToggle,
    onGranularityToggle,
    onAddDataRange,
    onRemoveDataRange,
    onAddFilter,
    onRemoveFilter,
    onToggle,
  } = props;

  const name = member.name.replace(`${cube.name}.`, '').trim();
  const title = 'shortTitle' in member ? member.shortTitle : undefined;
  // @ts-ignore
  const description = member.description;
  const isTimestampSelected = isSelected();
  const definedGranularities =
    (member.type === 'time' ? ('granularities' in member ? member?.granularities : []) : []) ?? [];
  const definedGranularityNames = definedGranularities.map((g) => g.name);
  const nonPredefinedGranularityNames = [...definedGranularityNames];
  const { shownMemberName } = useShownMemberName({
    cubeName: cube.name,
    cubeTitle: 'title' in cube ? cube.title : undefined,
    memberName: name,
    memberTitle: title,
    type: memberViewType,
  });

  selectedGranularities.forEach((granularity) => {
    if (
      !nonPredefinedGranularityNames.includes(granularity) &&
      !PREDEFINED_GRANULARITIES.includes(granularity)
    ) {
      nonPredefinedGranularityNames.push(granularity);
    }
  });

  const missingGranularities = selectedGranularities.filter(
    (granularity) =>
      !definedGranularityNames.includes(granularity) &&
      !PREDEFINED_GRANULARITIES.includes(granularity)
  );

  const definedGranularitiesTitleMap = useMemo(() => {
    return (
      member.type === 'time' &&
      definedGranularities?.reduce(
        (map, granularity) => {
          map[granularity.name] = granularity.title;

          return map;
        },
        {} as Record<string, string>
      )
    );
  }, [member.type === 'time' ? definedGranularities : null]);

  const allGranularityNames = nonPredefinedGranularityNames.concat(PREDEFINED_GRANULARITIES);
  const isGranularitySelectedMap: Record<string, boolean> = {};

  allGranularityNames.forEach((granularity) => {
    isGranularitySelectedMap[granularity] = isSelected(granularity);
  });

  const selectedGranularity = allGranularityNames.find((granularity) => isSelected(granularity));

  const granularityItems = (items: string[], isCustom?: boolean) => {
    return items.map((granularity: string) => {
      if (!isOpen && !isGranularitySelectedMap[granularity]) {
        return null;
      }

      const title = definedGranularitiesTitleMap
        ? definedGranularitiesTitleMap[granularity]
        : titleize(granularity);

      return (
        <GranularityListMember
          key={`${name}.${granularity}`}
          name={granularity}
          title={title}
          memberViewType={memberViewType}
          isMissing={missingGranularities.includes(granularity)}
          isCustom={isCustom}
          isSelected={isGranularitySelectedMap[granularity]}
          onToggle={() => {
            onGranularityToggle(member.name, granularity);
          }}
        />
      );
    });
  };

  const onPress = useEvent(() => {
    onToggle(!isOpen, member.name);
  });

  const filterMenu = useMemo(() => {
    const dangerProps = isFiltered
      ? {
          color: '#danger-text',
        }
      : {};
    const disabledMenuKeys: string[] = [];

    if (!isFiltered) {
      disabledMenuKeys.push('remove');
    }

    if (isDateRangeFiltered) {
      disabledMenuKeys.push('add-date-range');
    }

    return (
      <MenuTrigger>
        <FilterByMemberButton
          isAngular
          type="timeDimension"
          isFiltered={isFiltered || false}
          {...(isMissing ? { color: '#danger-text' } : {})}
        />
        <Menu
          disabledKeys={disabledMenuKeys}
          onAction={(key) => {
            switch (key) {
              case 'add-date-range':
                onAddDataRange?.(member.name);
                break;
              case 'add-filter':
                onAddFilter?.(member.name);
                break;
              case 'remove':
                onRemoveFilter?.(member.name);
                onRemoveDataRange?.(member.name);
                break;
              default:
                return;
            }
          }}
        >
          <Menu.Item key="add-date-range" icon={<PlusIcon />}>
            Filter by Date Range
          </Menu.Item>
          <Menu.Item key="add-filter" icon={<PlusIcon />}>
            Filter by This Member
          </Menu.Item>
          <Menu.Item key="remove" icon={<CloseIcon {...dangerProps} />}>
            <Text {...dangerProps}>Remove all</Text>
          </Menu.Item>
        </Menu>
      </MenuTrigger>
    );
  }, [
    member?.name,
    isDateRangeFiltered,
    isFiltered,
    isMissing,
    onAddDataRange,
    onRemoveDataRange,
    onAddFilter,
    onRemoveFilter,
  ]);

  return (
    <>
      <InstanceTooltipProvider
        name={name}
        fullName={member.name}
        type="dimension"
        title={title}
        overflowRef={textRef}
      >
        <ListMemberButton
          icon={<TimeIcon />}
          data-member="dimension"
          isSelected={isTimestampSelected && !isOpen}
          mods={{ missing: isMissing }}
          gridColumns="auto minmax(0, 1fr) auto"
          onPress={onPress}
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

          <Space gap=".5x">
            {('public' in member && member.public === false) || description ? (
              <Space gap="1x">
                {description ? <ItemInfoIcon description={description} /> : undefined}
                {'public' in member && member.public === false ? <NonPublicIcon /> : undefined}
              </Space>
            ) : null}
            {member && filterMenu}
          </Space>
        </ListMemberButton>
      </InstanceTooltipProvider>
      {isOpen || selectedGranularity ? (
        <GranularitiesWrapper>
          {isOpen ? (
            <ListMemberButton
              icon={<TimeIcon />}
              data-member="dimension"
              isSelected={isTimestampSelected}
              onPress={() => {
                onDimensionToggle(member.name);
              }}
            >
              <Text ellipsis>value</Text>
            </ListMemberButton>
          ) : null}
          {granularityItems(nonPredefinedGranularityNames, true)}
          {granularityItems(PREDEFINED_GRANULARITIES)}
          <div data-element="TimeListLine" />
        </GranularitiesWrapper>
      ) : null}
    </>
  );
}
