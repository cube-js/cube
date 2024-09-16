import { useRef, useState } from 'react';
import {
  Action,
  Flex,
  Space,
  Text,
  TimeIcon,
  CalendarIcon,
  TooltipProvider,
} from '@cube-dev/ui-kit';
import { Cube, TCubeDimension, TimeDimensionGranularity } from '@cubejs-client/core';

import { ArrowIcon } from '../icons/ArrowIcon';
import { NonPublicIcon } from '../icons/NonPublicIcon';
import { ItemInfoIcon } from '../icons/ItemInfoIcon';
import { useHasOverflow } from '../hooks/has-overflow';

import { ListMemberButton } from './ListMemberButton';
import { FilterByMemberButton } from './FilterByMemberButton';
import { MemberBadge } from './Badge';
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

const GRANULARITIES: TimeDimensionGranularity[] = [
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
  const isGranularitySelectedList = GRANULARITIES.map((granularity) => isSelected(granularity));
  const selectedGranularity = GRANULARITIES.find((granularity) => isSelected(granularity));
  // const isGranularitySelected = !!isGranularitySelectedList.find((gran) => gran);

  open = isCompact ? false : open;

  const hasOverflow = useHasOverflow(textRef);
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
      {(isCompact || !open) && selectedGranularity ? (
        <TooltipProvider
          delay={1000}
          title="Click the granularity label to remove it from the query"
        >
          <Action
            onPress={() => {
              onGranularityToggle(member.name, selectedGranularity);
            }}
          >
            <MemberBadge isSpecial type="timeDimension">
              {selectedGranularity}
            </MemberBadge>
          </Action>
        </TooltipProvider>
      ) : null}
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

  return (
    <>
      {hasOverflow ? (
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
      {open || isCompact ? (
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
          {GRANULARITIES.map((granularity, i) => {
            return open && !isCompact ? (
              <ListMemberButton
                key={`${name}.${granularity}`}
                icon={<CalendarIcon />}
                data-member="timeDimension"
                isSelected={isGranularitySelectedList[i]}
                onPress={() => {
                  onGranularityToggle(member.name, granularity);
                  setOpen(false);
                }}
              >
                <Text ellipsis>{granularity}</Text>
              </ListMemberButton>
            ) : null;
          })}
        </Flex>
      ) : null}
    </>
  );
}
