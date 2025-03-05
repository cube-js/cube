import { useEffect, useRef, useState } from 'react';
import { Button, ClearIcon, Flex, Flow, Space, tasty } from '@cube-dev/ui-kit';
import { TCubeDimension, TCubeMeasure } from '@cubejs-client/core';

import { useQueryBuilderContext } from './context';
import { useEvent } from './hooks';
import { AccordionCard } from './components/AccordionCard';
import { DateRangeFilter } from './components/DateRangeFilter';
import { MemberBadge } from './components/Badge';
import { FilterMember } from './components/FilterMember';
import { SegmentFilter } from './components/SegmentFilter';
import { LogicalFilter } from './components/LogicalFilter';
import { AddFilterInput } from './components/AddFilterInput';

const BadgeContainer = tasty(Space, {
  styles: {
    gap: '.5x',
    transition: 'opacity',
    opacity: {
      '': 1,
      hidden: 0,
    },
  },
});

export function QueryBuilderFilters({ onToggle }: { onToggle?: (isExpanded: boolean) => void }) {
  const filtersRef = useRef<HTMLElement>(null);
  const {
    segments: segmentsUpdater,
    dateRanges,
    members,
    filters: filtersUpdater,
    query,
    joinableCubes,
    usedCubes,
    cubes,
    memberViewType,
    usedMembersInFilters,
  } = useQueryBuilderContext();

  const isCompact = usedCubes.length === 1;
  const isAddingCompact = joinableCubes.length === 1;
  const timeDimensions = query.timeDimensions || [];
  const filters = query.filters || [];
  const segments = query.segments || [];
  const timeCounter = dateRanges.list.length;
  const segmentsCounter = segments.length;
  const measureCounter = usedMembersInFilters.filter(
    (memberName) => members.measures[memberName]
  ).length;
  const dimensionCounter = usedMembersInFilters.filter(
    (memberName) => members.dimensions[memberName]
  ).length;

  const isFiltered = filters.length > 0 || segments.length > 0 || dateRanges.list.length > 0;

  const [isExpanded, setIsExpanded] = useState(isFiltered);

  useEffect(() => {
    setIsExpanded(isFiltered);
  }, [isFiltered]);

  function getMemberType(member: TCubeMeasure | TCubeDimension) {
    if (!member?.name) {
      return undefined;
    }

    if (members.measures[member.name]) {
      return 'measure';
    }
    if (members.dimensions[member.name]) {
      return 'dimension';
    }

    return undefined;
  }

  useEffect(() => {
    (
      filtersRef?.current?.querySelector('button[data-is-invalid]') as HTMLButtonElement | undefined
    )?.click();
  }, [dateRanges.list.length]);

  const onClearAction = useEvent(() => {
    dateRanges.clear();
    filtersUpdater.clear();
    segmentsUpdater?.clear();
  });

  return (
    <AccordionCard
      noPadding
      isExpanded={isExpanded}
      title="Filters"
      subtitle={
        timeCounter || dimensionCounter || measureCounter || segmentsCounter ? (
          <BadgeContainer mods={{ hidden: isExpanded }}>
            {timeCounter ? (
              <MemberBadge type="timeDimension">{timeCounter}</MemberBadge>
            ) : undefined}
            {dimensionCounter ? (
              <MemberBadge type="dimension">{dimensionCounter}</MemberBadge>
            ) : undefined}
            {measureCounter ? (
              <MemberBadge type="measure">{measureCounter}</MemberBadge>
            ) : undefined}
            {segmentsCounter ? (
              <MemberBadge type="segment">{segmentsCounter}</MemberBadge>
            ) : undefined}
          </BadgeContainer>
        ) : undefined
      }
      extra={
        timeCounter || dimensionCounter || measureCounter || segmentsCounter ? (
          <Button icon={<ClearIcon />} size="small" theme="danger" onPress={onClearAction}>
            Remove All
          </Button>
        ) : null
      }
      contentStyles={{ border: 'top' }}
      onToggle={(isExpanded) => {
        setIsExpanded(isExpanded);
        onToggle?.(isExpanded);
      }}
    >
      <Flow ref={filtersRef}>
        <Flex flow="column" gap=".75x" padding="1x">
          {dateRanges.list.map((dimensionName, i) => {
            const timeDimension = timeDimensions.find(
              (timeDimension) => timeDimension.dimension === dimensionName
            );

            const dimension = members.dimensions[dimensionName];
            const cubeName = dimensionName.split('.')[0];
            const cube = cubes.find((cube) => cube.name === cubeName);
            const memberName = dimensionName.split('.')[1];
            const member = members.measures[dimensionName] || members.dimensions[dimensionName];

            return (
              <DateRangeFilter
                key={i}
                isMissing={!dimension}
                isCompact={isCompact}
                name={dimensionName}
                member={timeDimension || { dimension: dimensionName }}
                memberName={memberName}
                memberTitle={member?.shortTitle}
                cubeName={cubeName}
                cubeTitle={cube?.title}
                memberViewType={memberViewType}
                onRemove={() => {
                  dateRanges.remove(dimensionName);
                }}
                onChange={(dateRange) => {
                  dateRanges.set(dimensionName, dateRange);
                }}
              />
            );
          })}
          {filters.map((filter, index) => {
            if ('and' in filter) {
              return (
                <LogicalFilter
                  key={index}
                  type="and"
                  values={filter.and}
                  isCompact={isCompact}
                  isAddingCompact={isAddingCompact}
                  onRemove={() => {
                    filtersUpdater.remove(index);
                  }}
                  onChange={(filter) => {
                    filtersUpdater.update(index, filter);
                  }}
                  onUnwrap={() => {
                    if (filter.and.length === 1) {
                      filtersUpdater.update(index, filter.and[0]);

                      return;
                    }

                    filtersUpdater.remove(index);
                    filter.and.forEach((filter) => {
                      filtersUpdater.add(filter);
                    });
                  }}
                />
              );
            }

            if ('or' in filter) {
              return (
                <LogicalFilter
                  key={index}
                  type="or"
                  values={filter.or}
                  isCompact={isCompact}
                  isAddingCompact={isAddingCompact}
                  onRemove={() => {
                    filtersUpdater.remove(index);
                  }}
                  onChange={(filter) => {
                    filtersUpdater.update(index, filter);
                  }}
                  onUnwrap={() => {
                    if (filter.or.length === 1) {
                      filtersUpdater.update(index, filter.or[0]);

                      return;
                    }

                    filtersUpdater.remove(index);
                    filter.or.forEach((filter) => {
                      filtersUpdater.add(filter);
                    });
                  }}
                />
              );
            }

            if (!('member' in filter) || !filter.member) {
              return null;
            }

            const memberFullName = filter.member;
            const cubeName = memberFullName.split('.')[0];
            const cube = cubes.find((cube) => cube.name === cubeName);
            const memberName = memberFullName.split('.')[1];
            const member = members.measures[memberFullName] || members.dimensions[memberFullName];

            return (
              <FilterMember
                key={index}
                isMissing={!member}
                isCompact={isCompact}
                filter={filter}
                memberName={memberName}
                memberTitle={member?.shortTitle}
                cubeName={cubeName}
                cubeTitle={cube?.title}
                memberViewType={memberViewType}
                memberType={getMemberType(member)}
                type={member?.type}
                onRemove={() => {
                  filtersUpdater.remove(index);
                }}
                onChange={(updatedFilter) => {
                  filtersUpdater.update(index, updatedFilter);
                }}
              />
            );
          })}
          {segments.map((segment, i) => {
            const member = members.segments[segment];
            const cubeName = segment.split('.')[0];
            const cube = cubes.find((cube) => cube.name === cubeName);
            const memberName = segment.split('.')[1];

            return (
              <SegmentFilter
                key={member?.name || i}
                isMissing={!member}
                isCompact={isCompact}
                member={member}
                memberName={memberName}
                memberTitle={member?.shortTitle}
                cubeName={cubeName}
                cubeTitle={cube?.title}
                memberViewType={memberViewType}
                name={segment}
                onRemove={() => {
                  segmentsUpdater.remove(segment);
                }}
              />
            );
          })}
          <AddFilterInput
            hasLabel
            isCompact={isAddingCompact}
            onAdd={(filter) => {
              filtersUpdater.add(filter);
            }}
            onSegmentAdd={(name) => {
              segmentsUpdater.add(name);
            }}
            onDateRangeAdd={(name) => {
              dateRanges.set(name);
            }}
          />
        </Flex>
      </Flow>
    </AccordionCard>
  );
}
