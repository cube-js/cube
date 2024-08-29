import { useEffect, useRef, useState } from 'react';
import { Block, Button, Divider, Flow, Menu, MenuTrigger, Space, tasty } from '@cube-dev/ui-kit';
import { PlusOutlined } from '@ant-design/icons';
import { TCubeDimension, TCubeMeasure } from '@cubejs-client/core';

import { useQueryBuilderContext } from './context';
import { getTypeIcon } from './utils';
import { useListMode } from './hooks/list-mode';
import { AccordionCard } from './components/AccordionCard';
import { ScrollableArea } from './components/ScrollableArea';
import { DateRangeFilter } from './components/DateRangeFilter';
import { MemberBadge } from './components/Badge';
import { MemberFilter } from './components/MemberFilter';
import { SegmentFilter } from './components/SegmentFilter';

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
  const [listMode] = useListMode();
  const filtersRef = useRef<HTMLElement>(null);
  const {
    selectedCube,
    segments: segmentsUpdater,
    dateRanges,
    members,
    filters: filtersUpdater,
    query,
    queryStats,
  } = useQueryBuilderContext();

  const isCompact =
    Object.keys(queryStats).length === 1 &&
    ((selectedCube && selectedCube === queryStats[selectedCube?.name]?.instance) || !selectedCube);
  const timeDimensions = query.timeDimensions || [];
  const filters = query.filters || [];
  const segments = query.segments || [];
  const timeCounter = dateRanges.list.length;
  const segmentsCounter = segments.length;

  const measureCounter = filters.filter((filter) => {
    if (!('member' in filter) || !filter.member) {
      return false;
    }

    return !!members.measures[filter.member];
  }).length;

  const dimensionCounter = filters.filter((filter) => {
    if (!('member' in filter) || !filter.member) {
      return false;
    }

    return !!members.dimensions[filter.member];
  }).length;

  const availableTimeDimensions =
    selectedCube?.dimensions.filter((member) => {
      return member.type === 'time' && !dateRanges.list.includes(member.name);
    }) || [];

  const isFiltered = filters.length > 0 || segments.length > 0 || dateRanges.list.length > 0;

  const [isExpanded, setIsExpanded] = useState(isFiltered);

  useEffect(() => {
    setIsExpanded(isFiltered);
  }, [isFiltered]);

  const availableMeasuresAndDimensions = [
    ...(selectedCube?.dimensions || []),
    ...(selectedCube?.measures || []),
    // ...(selectedCube?.timeDimensions || []),
  ];

  const availableSegments =
    selectedCube?.segments.filter((member) => {
      return !segments.includes(member.name);
    }) || [];

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

  function addDateRange(name: string) {
    dateRanges.set(name);
  }

  function addSegment(name: string) {
    segmentsUpdater?.add(name);
  }

  function addFilter(name: string) {
    filtersUpdater.add({ member: name, operator: 'set' });
  }

  useEffect(() => {
    (
      filtersRef?.current?.querySelector('button[data-is-invalid]') as HTMLButtonElement | undefined
    )?.click();
  }, [dateRanges.list.length]);

  useEffect(() => {
    const invalidTime = filtersRef?.current?.querySelector('button[data-is-invalid]') as
      | HTMLButtonElement
      | undefined;

    if (invalidTime) {
      return;
    }

    (
      [...(filtersRef?.current?.querySelectorAll('button') ?? [])].slice(-1)[0] as
        | HTMLButtonElement
        | undefined
    )?.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }, [query?.filters?.length, dateRanges.list.length, segments?.length]);

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
      contentStyles={{ border: 'top' }}
      onToggle={(isExpanded) => {
        setIsExpanded(isExpanded);
        onToggle?.(isExpanded);
      }}
    >
      <Flow ref={filtersRef}>
        <ScrollableArea gap="1x" padding="1x" height="max 18x">
          {!isFiltered ? <Block preset="t3m">No filters set</Block> : null}
          {dateRanges.list.map((dimensionName, i) => {
            const timeDimension = timeDimensions.find(
              (timeDimension) => timeDimension.dimension === dimensionName
            );

            const dimension = members.dimensions[dimensionName];

            return (
              <DateRangeFilter
                key={i}
                isCompact={isCompact}
                isMissing={!dimension}
                member={timeDimension || { dimension: dimensionName }}
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
            if (!('member' in filter) || !filter.member) {
              return null;
            }

            const member = members.measures[filter.member] || members.dimensions[filter.member];

            return (
              <MemberFilter
                key={index}
                isCompact={isCompact}
                member={filter}
                memberType={getMemberType(member)}
                type={member?.type}
                isMissing={!member}
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

            return (
              <SegmentFilter
                key={member?.name || i}
                isCompact={isCompact}
                member={member}
                name={segment}
                onRemove={() => {
                  segmentsUpdater?.remove(segment);
                }}
              />
            );
          })}
        </ScrollableArea>
        {listMode === 'dev' ? (
          <>
            {isFiltered ? <Divider /> : undefined}
            <Space padding="1x 1x 1x 1x">
              <MenuTrigger>
                <Button
                  isDisabled={!selectedCube}
                  icon={<PlusOutlined />}
                  type="clear"
                  size="small"
                >
                  Filter
                </Button>
                <Menu height="max 44x" onAction={(name) => addFilter(name as string)}>
                  {availableMeasuresAndDimensions.map((dimension) => {
                    return (
                      <Menu.Item key={dimension.name} textValue={dimension.name}>
                        <Space
                          gap="1x"
                          color={`#${
                            members.dimensions[dimension.name] ? 'dimension' : 'measure'
                          }-text`}
                        >
                          {getTypeIcon(dimension.type)}
                          {dimension.name.split('.')[1]}
                        </Space>
                      </Menu.Item>
                    );
                  })}
                </Menu>
              </MenuTrigger>
              <MenuTrigger>
                <Button
                  isDisabled={!selectedCube || !availableTimeDimensions.length}
                  icon={<PlusOutlined />}
                  type="clear"
                  size="small"
                >
                  Date Range
                </Button>
                <Menu height="max 44x" onAction={(name) => addDateRange(name as string)}>
                  {availableTimeDimensions.map((dimension) => {
                    return (
                      <Menu.Item key={dimension.name} textValue={dimension.name}>
                        <Space color="#time-dimension-text">
                          {getTypeIcon('time')}
                          {dimension.name.split('.')[1]}
                        </Space>
                      </Menu.Item>
                    );
                  })}
                </Menu>
              </MenuTrigger>
              <MenuTrigger>
                <Button
                  isDisabled={!selectedCube || !availableSegments.length}
                  icon={<PlusOutlined />}
                  type="clear"
                  size="small"
                >
                  Segment
                </Button>
                <Menu onAction={(name) => addSegment(name as string)}>
                  {availableSegments.map((segment) => {
                    return <Menu.Item key={segment.name}>{segment.name.split('.')[1]}</Menu.Item>;
                  })}
                </Menu>
              </MenuTrigger>
              {!selectedCube && <Block preset="t3m">Select a cube or a view to add filters</Block>}
            </Space>
          </>
        ) : null}
      </Flow>
    </AccordionCard>
  );
}
