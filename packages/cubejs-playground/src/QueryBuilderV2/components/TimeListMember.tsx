import { useMemo, useRef, useState } from 'react';
import { Flex, Space, Text, TimeIcon, TooltipProvider } from '@cube-dev/ui-kit';
import { Cube, TCubeDimension, TimeDimensionGranularity } from '@cubejs-client/core';

import { ArrowIcon } from '../icons/ArrowIcon';
import { NonPublicIcon } from '../icons/NonPublicIcon';
import { ItemInfoIcon } from '../icons/ItemInfoIcon';
import { useHasOverflow } from '../hooks/has-overflow';
import { titleize } from '../utils/index';

import { GranularityListMember } from './GranularityListMember';
import { ListMemberButton } from './ListMemberButton';
import { FilterByMemberButton } from './FilterByMemberButton';
import { FilteredLabel } from './FilteredLabel';

interface ListMemberProps {
  cube: Cube;
  member: TCubeDimension;
  filterString?: string;
  isCompact?: boolean;
  isSelected: (granularity?: TimeDimensionGranularity) => boolean;
  isFiltered: boolean;
  onDimensionToggle: (component: string) => void;
  onGranularityToggle: (name: string, granularity: TimeDimensionGranularity) => void;
  onToggleDataRange?: (name: string) => void;
}

const PREDEFINED_GRANULARITIES: TimeDimensionGranularity[] = [
  'second',
  'minute',
  'hour',
  'day',
  'week',
  'month',
  'quarter',
  'year',
];

export function TimeListMember(props: ListMemberProps) {
  const textRef = useRef<HTMLDivElement>(null);

  let [open, setOpen] = useState(false);

  const {
    cube,
    member,
    filterString,
    isCompact,
    isSelected,
    isFiltered,
    onDimensionToggle,
    onGranularityToggle,
    onToggleDataRange,
  } = props;

  // const title = member.title.replace(cube.title, '').trim();
  const name = member.name.replace(`${cube.name}.`, '').trim();
  const title = member.title;
  // @ts-ignore
  const description = member.description;
  const isTimestampSelected = isSelected();

  const customGranularities =
    member.type === 'time' && member.granularities ? member.granularities.map((g) => g.name) : [];
  const customGranularitiesTitleMap = useMemo(() => {
    return (
      member.type === 'time' &&
      member.granularities?.reduce(
        (map, granularity) => {
          map[granularity.name] = granularity.title;

          return map;
        },
        {} as Record<string, string>
      )
    );
  }, [member.type === 'time' ? member.granularities : null]);
  const memberGranularities = customGranularities.concat(PREDEFINED_GRANULARITIES);
  const isGranularitySelectedMap: Record<string, boolean> = {};
  memberGranularities.forEach((granularity) => {
    isGranularitySelectedMap[granularity] = isSelected(granularity);
  });
  const selectedGranularity = memberGranularities.find((granularity) => isSelected(granularity));

  open = isCompact ? false : open;

  const hasOverflow = useHasOverflow(textRef);
  const isAutoTitle = titleize(member.name) === title;

  const button = (
    <ListMemberButton
      icon={
        isCompact ? (
          <TimeIcon />
        ) : (
          <ArrowIcon direction={open ? 'top' : 'right'} color="var(--dimension-text-color)" />
        )
      }
      data-member="dimension"
      isSelected={isTimestampSelected && (isCompact || !open)}
      onPress={() => {
        if (!isCompact) {
          setOpen(!open);
        } else {
          if (isTimestampSelected) {
            onDimensionToggle(member.name);
          } else if (selectedGranularity) {
            onGranularityToggle(member.name, selectedGranularity);
          }
        }
      }}
    >
      <Text ref={textRef} ellipsis>
        {filterString ? <FilteredLabel text={name} filter={filterString} /> : name}
      </Text>

      <Space gap=".5x">
        <Space gap="1x">
          {description ? <ItemInfoIcon title={title} description={description} /> : undefined}
          {/* @ts-ignore */}
          {member.public === false ? <NonPublicIcon /> : undefined}
        </Space>
        <FilterByMemberButton
          type="timeDimension"
          isFiltered={isFiltered || false}
          onPress={() => onToggleDataRange?.(member.name)}
        />
      </Space>
    </ListMemberButton>
  );

  const granularityItems = (items: string[], isCustom?: boolean) => {
    return items.map((granularity: string) => {
      if ((!open || isCompact) && !isGranularitySelectedMap[granularity]) {
        return null;
      }

      const title = customGranularitiesTitleMap
        ? customGranularitiesTitleMap[granularity]
        : titleize(granularity);

      return (
        <GranularityListMember
          key={`${name}.${granularity}`}
          name={granularity}
          title={title}
          isCustom={isCustom}
          isSelected={isGranularitySelectedMap[granularity]}
          onToggle={() => {
            onGranularityToggle(member.name, granularity);
            setOpen(false);
          }}
        />
      );
    });
  };

  return (
    <>
      {hasOverflow || !isAutoTitle ? (
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
      )}
      {open || isCompact || selectedGranularity ? (
        <Flex flow="column" gap="1bw" padding="4.5x left">
          {open && !isCompact ? (
            <ListMemberButton
              icon={<TimeIcon />}
              data-member="dimension"
              isSelected={isTimestampSelected}
              onPress={() => {
                onDimensionToggle(member.name);
                setOpen(false);
              }}
            >
              <Text ellipsis>value</Text>
            </ListMemberButton>
          ) : null}
          {granularityItems(customGranularities, true)}
          {granularityItems(PREDEFINED_GRANULARITIES)}
        </Flex>
      ) : null}
    </>
  );
}
