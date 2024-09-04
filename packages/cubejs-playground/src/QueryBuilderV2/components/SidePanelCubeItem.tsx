import { useEffect, useMemo, useRef, useState } from 'react';
import {
  Block,
  Button,
  Space,
  tasty,
  Text,
  CubeIcon,
  ViewIcon,
  TooltipProvider,
} from '@cube-dev/ui-kit';
import { QuestionCircleOutlined } from '@ant-design/icons';

import { useQueryBuilderContext } from '../context';
import { ArrowIcon } from '../icons/ArrowIcon';
import { NonPublicIcon } from '../icons/NonPublicIcon';
import { ItemInfoIcon } from '../icons/ItemInfoIcon';
import { useHasOverflow, useFilteredMembers } from '../hooks';

import { ListMember } from './ListMember';
import { TimeListMember } from './TimeListMember';
import { FilteredLabel } from './FilteredLabel';

const CubeWrapper = tasty({
  styles: {
    position: 'sticky',
    top: 0,
    fill: '#white',
    display: 'grid',
    zIndex: 1,
  },
});

const ArrowIconWrapper = tasty({
  styles: {
    display: 'grid',
    placeContent: 'center',
    width: '16px',
  },
});

const CubeButton = tasty(Button, {
  qa: 'CubeButton',
  type: 'secondary',
  size: 'small',
  styles: {
    color: {
      '': '#dark-03',
      joinable: '#dark',
      missing: '#danger-text',
    },
    radius: 0,
    border: '#clear',
    fill: {
      '': '#purple.08',
      hovered: '#purple.16',
      pressed: '#purple.12',
      '!joinable': '#dark.04',
    },
    padding: '(.75x - 1bw) (1.5x - 1bw) (.75x - 1bw) (1.5x - 1bw)',
    cursor: {
      joinable: 'pointer',
      disabled: 'default',
    },
    // shadow: {
    //   '': '0 1ow 3ow #dark.0',
    //   open: '0 1ow 1ow #dark.1',
    // },
    placeContent: 'space-between',
    gridTemplateColumns: 'auto 1fr auto',
    textAlign: 'left',
    zIndex: 2,
    transition: 'fill .08s, color .3s',

    CubeIcon: {
      color: {
        '': '#dark-03',
        joinable: '#purple-text',
        missing: '#danger-text',
      },
    },
  },
});

interface CubeListItemProps {
  isOpen: boolean;
  name: string;
  showStats?: boolean;
  filterString?: string;
  isFiltered?: boolean;
  isPrivate?: boolean;
  isSelected?: boolean;
  isNonJoinable?: boolean;
  rightIcon?: 'arrow' | 'plus' | null;
  mode?: 'all' | 'query';
  onToggle?: (isOpen: boolean) => void;
  onMemberToggle?: (name: string) => void;
}

export function SidePanelCubeItem({
  isOpen,
  name,
  mode = 'all',
  isFiltered,
  isNonJoinable,
  isSelected,
  onToggle,
  filterString,
  onMemberToggle,
}: CubeListItemProps) {
  const textRef = useRef<HTMLDivElement>(null);
  const {
    query,
    grouping,
    dateRanges,
    dimensions: dimensionsUpdater,
    measures: measuresUpdater,
    segments: segmentsUpdater,
    filters,
    // queryStats,
    cubes,
    meta,
    usedCubes,
    usedMembers,
  } = useQueryBuilderContext();
  const cube = cubes.find((cube) => cube.name === name);
  // @ts-ignore
  const type = cube?.type || 'cube';
  const isUsed = usedCubes.includes(name);
  // @ts-ignore
  const isPrivate = cube?.public === false;
  // const stats = queryStats[name];
  const isMissing = !cube;
  const [showAllMembers, setShowAllMembers] = useState(false);

  // @ts-ignore
  const { title, description } = cube || {};

  const {
    measures: shownMeasures,
    dimensions: shownDimensions,
    segments: shownSegments,
  } = useFilteredMembers(filterString || '', {
    measures: cube?.measures || [],
    dimensions: cube?.dimensions || [],
    segments: cube?.segments || [],
  });

  const dimensions = (filterString ? shownDimensions : cube?.dimensions || [])
    .map((d) => d.name)
    .filter(
      (d) =>
        (mode === 'all' && isOpen) ||
        filterString ||
        showAllMembers ||
        usedMembers.filter((d) => d.startsWith(`${name}.`)).includes(d)
    );
  const measures = (filterString ? shownMeasures : cube?.measures || [])
    .map((m) => m.name)
    .filter(
      (m) =>
        (mode === 'all' && isOpen) ||
        filterString ||
        showAllMembers ||
        usedMembers.filter((m) => m.startsWith(`${name}.`)).includes(m)
    );
  const segments = (filterString ? shownSegments : cube?.segments || [])
    .map((s) => s.name)
    .filter(
      (s) =>
        (mode === 'all' && isOpen) ||
        showAllMembers ||
        query?.segments?.includes(s)
    );

  if (!filterString) {
    query?.dimensions?.forEach((dimension) => {
      if (
        !dimensions?.includes(dimension) &&
        dimension.startsWith(`${name}.`)
      ) {
        dimensions.push(dimension);
      }
    });

    query?.measures?.forEach((measure) => {
      if (!measures?.includes(measure) && measure.startsWith(`${name}.`)) {
        measures.push(measure);
      }
    });

    query?.segments?.forEach((segment) => {
      if (!segments?.includes(segment) && segment.startsWith(`${name}.`)) {
        segments.push(segment);
      }
    });
  }

  dimensions.sort();
  measures.sort();
  segments.sort();

  function addFilter(name: string) {
    filters.add({ member: name, operator: 'set' });
  }

  function removeFilter(name: string) {
    filters.removeByMember(name);
  }

  function addDateRange(name: string) {
    dateRanges.set(name);
  }

  function removeDateRange(name: string) {
    dateRanges.remove(name);
  }

  const showMembers =
    (isOpen || mode === 'query' || isUsed || !!filterString) && !isNonJoinable;

  const dimensionsSection = useMemo(() => {
    return showMembers && dimensions.length ? (
      <>
        {dimensions.map((name) => {
          const item = cube?.dimensions?.find((d) => d.name === name);

          // @TODO: support missing dimensions
          if (!item || !cube) {
            return null;
          }

          if (cube && item?.type === 'time') {
            return (
              <TimeListMember
                key={item.name}
                cube={cube}
                member={item}
                filterString={filterString}
                isCompact={(mode === 'query' && !showAllMembers) || !isOpen}
                isSelected={(granularity) => {
                  if (granularity) {
                    return (
                      query?.timeDimensions?.some(
                        (td) =>
                          td.dimension === item.name &&
                          td.granularity === granularity
                      ) || false
                    );
                  }

                  return query?.dimensions?.includes(item.name) || false;
                }}
                isFiltered={
                  query?.timeDimensions?.some(
                    (td) => td.dimension === item.name && td.dateRange
                  ) || false
                }
                onDimensionToggle={(dimension) => {
                  dimensionsUpdater?.toggle(dimension);
                  onMemberToggle?.(dimension);
                }}
                onGranularityToggle={(name, granularity) => {
                  grouping.toggle(name, granularity);
                  onMemberToggle?.(name);
                }}
                onToggleDataRange={
                  !dateRanges.list.includes(item.name)
                    ? addDateRange
                    : removeDateRange
                }
              />
            );
          }

          return (
            <ListMember
              key={name}
              cube={cube}
              member={item}
              category="dimensions"
              filterString={filterString}
              isSelected={query?.dimensions?.includes(name) || false}
              isFiltered={
                query?.filters?.some(
                  (filter) => 'member' in filter && filter.member === name
                ) || false
              }
              onAddFilter={addFilter}
              onRemoveFilter={removeFilter}
              onToggle={() => {
                dimensionsUpdater?.toggle(name);
                onMemberToggle?.(name);
              }}
            />
          );
        })}
      </>
    ) : null;
  }, [
    dimensions.join(','),
    query?.dimensions?.join(','),
    JSON.stringify(query?.timeDimensions),
    showMembers,
    mode,
    isOpen,
    meta,
    filterString,
    JSON.stringify(query?.filters),
  ]);

  const measuresSection = useMemo(() => {
    return showMembers && measures.length ? (
      <>
        {measures.map((name) => {
          const item = cube?.measures?.find((m) => m.name === name);

          // @TODO: support missing measures
          if (!item || !cube) {
            return null;
          }

          return (
            <ListMember
              key={name}
              cube={cube}
              member={item}
              category="measures"
              filterString={filterString}
              isSelected={query?.measures?.includes(name) || false}
              isFiltered={
                query?.filters?.some(
                  (filter) => 'member' in filter && filter.member === name
                ) || false
              }
              onAddFilter={addFilter}
              onRemoveFilter={removeFilter}
              onToggle={() => {
                measuresUpdater?.toggle(name);
                onMemberToggle?.(name);
              }}
            />
          );
        })}
      </>
    ) : null;
  }, [
    measures.join(','),
    query?.measures?.join(','),
    showMembers,
    filterString,
    mode,
    meta,
    JSON.stringify(query?.filters),
  ]);

  const segmentsSection = useMemo(() => {
    return showMembers && segments.length ? (
      <>
        {segments.map((name) => {
          const item = cube?.segments?.find((s) => s.name === name);

          if (!item || !cube) {
            return null;
          }

          return (
            <ListMember
              key={name}
              cube={cube}
              member={item}
              category="segments"
              isSelected={query?.segments?.includes(name) || false}
              onToggle={() => {
                segmentsUpdater?.toggle(name);
              }}
            />
          );
        })}
      </>
    ) : null;
  }, [segments.join(','), query?.segments?.join(','), showMembers, mode, meta]);

  const hasOverflow = useHasOverflow(textRef);

  const noVisibleMembers =
    !dimensions.length && !measures.length && !segments.length;

  useEffect(() => {
    setShowAllMembers(false);
  }, [mode, filterString]);

  if (noVisibleMembers) {
    if (mode === 'query' && !isUsed) {
      return null;
    }

    if (filterString && !isFiltered) {
      return null;
    }
  }

  if (filterString && isNonJoinable) {
    return null;
  }

  const memberList = (() => {
    if (showMembers) {
      if (!noVisibleMembers) {
        return (
          <Space flow="column" gap="1bw" padding="1ow 1x 0 4.5x">
            {dimensionsSection}
            {measuresSection}
            {segmentsSection}
            {mode === 'query' ? (
              <Button
                type="neutral"
                size="small"
                icon={
                  !showAllMembers ? (
                    <ArrowIcon direction="bottom" />
                  ) : (
                    <ArrowIcon direction="top" />
                  )
                }
                placeContent="start"
                onPress={() => setShowAllMembers(!showAllMembers)}
              >
                {!showAllMembers ? 'Show all members' : 'Hide unused members'}
              </Button>
            ) : null}
          </Space>
        );
      } else if (filterString) {
        return null;
      } else if (isOpen || mode === 'query') {
        return (
          <Block padding=".5x 0 .5x 4.5x">
            No members{mode === 'query' ? ' selected' : ''}
          </Block>
        );
      }
    } else {
      return null;
    }
  })();

  const isCollapsable = isNonJoinable || !!filterString;
  const cubeButton = (
    <CubeButton
      qaVal={name}
      icon={
        isMissing ? (
          <QuestionCircleOutlined
            style={{ color: 'var(--danger-text-color)' }}
          />
        ) : type === 'cube' ? (
          <CubeIcon color="#purple" />
        ) : (
          <ViewIcon color="#purple" />
        )
      }
      rightIcon={
        mode === 'all' && !isNonJoinable ? (
          <ArrowIconWrapper>
            <ArrowIcon
              direction={!isCollapsable ? (isOpen ? 'top' : 'bottom') : 'right'}
              style={{ color: 'var(--purple-color)' }}
            />
          </ArrowIconWrapper>
        ) : isNonJoinable ? (
          <ArrowIconWrapper />
        ) : undefined
      }
      mods={{
        open: isOpen,
        joinable: !isNonJoinable,
        missing: isMissing,
        collapsable: isCollapsable,
      }}
      flow="column"
      placeContent="space-between"
      onPress={() => !isMissing && !isNonJoinable && onToggle?.(!isOpen)}
    >
      <Text ref={textRef} ellipsis>
        {filterString ? (
          <FilteredLabel text={name} filter={filterString} />
        ) : (
          name
        )}
      </Text>
      {description ? (
        <ItemInfoIcon title={title} description={description} />
      ) : undefined}
      {isPrivate ? <NonPublicIcon type="cube" /> : undefined}
    </CubeButton>
  );

  return (
    <Space flow="column" gap="0">
      <CubeWrapper>
        {hasOverflow ? (
          <TooltipProvider
            delay={1000}
            title={
              <>
                <b>{name}</b>
              </>
            }
            placement="right"
          >
            {cubeButton}
          </TooltipProvider>
        ) : (
          cubeButton
        )}
      </CubeWrapper>
      {memberList}
    </Space>
  );
}
