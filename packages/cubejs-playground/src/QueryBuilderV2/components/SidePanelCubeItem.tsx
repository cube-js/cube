import { Cube } from '@cubejs-client/core';
import { ReactElement, useEffect, useMemo, useRef, useState } from 'react';
import { Block, Button, Space, tasty, Text, CubeIcon, ViewIcon } from '@cube-dev/ui-kit';

import { TCubeHierarchy } from '../types';
import { useQueryBuilderContext } from '../context';
import { ChevronIcon } from '../icons/ChevronIcon';
import { NonPublicIcon } from '../icons/NonPublicIcon';
import { ItemInfoIcon } from '../icons/ItemInfoIcon';
import { useEvent, useFilteredMembers } from '../hooks';
import { titleize } from '../utils';

import { HierarchyMember } from './HierarchyMember';
import { Folder } from './Folder';
import { ListMember } from './ListMember';
import { TimeListMember } from './TimeListMember';
import { FilteredLabel } from './FilteredLabel';
import { InstanceTooltipProvider } from './InstanceTooltipProvider';

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
      '!joinable | missing': '#dark.04',
    },
    padding: '(.75x - 1bw) 1x (.75x - 1bw) (1x - 1bw)',
    cursor: {
      joinable: 'pointer',
      disabled: 'default',
    },
    placeContent: 'space-between',
    gridTemplateColumns: 'auto 1fr auto 2x',
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

function sortFn(a: string, b: string) {
  return a.localeCompare(b);
}

interface CubeListItemProps {
  isOpen: boolean;
  cubeName: string;
  showStats?: boolean;
  filterString?: string;
  isPrivate?: boolean;
  isNonJoinable?: boolean;
  rightIcon?: 'arrow' | 'plus' | null;
  mode?: 'all' | 'query';
  onToggle?: (isOpen: boolean) => void;
  onMemberToggle: (name: string, cube?: string) => void;
  // For toggling hierarchies and folders
  onHierarchyToggle?: (name?: string) => void;
}

export function SidePanelCubeItem(props: CubeListItemProps) {
  const {
    isOpen,
    cubeName,
    mode = 'all',
    isNonJoinable,
    onToggle,
    filterString,
    onMemberToggle,
    onHierarchyToggle,
  } = props;
  const textRef = useRef<HTMLDivElement>(null);
  const {
    query,
    grouping,
    dateRanges,
    dimensions: dimensionsUpdater,
    measures: measuresUpdater,
    segments: segmentsUpdater,
    filters,
    cubes,
    meta,
    members,
    usedCubes,
    usedMembers,
    usedMembersInFilters,
    usedGranularities,
    missingMembers,
    memberViewType,
    isQueryEmpty,
    queryStats,
  } = useQueryBuilderContext();

  const cube = cubes.find((cube) => cube.name === cubeName);
  // @ts-ignore
  const type = cube?.type || 'cube';
  const isUsed = usedCubes.includes(cubeName);
  // @ts-ignore
  const isPrivate = cube?.public === false;
  // const stats = queryStats[name];
  const isMissing = !cube;
  const shownName = memberViewType === 'name' ? cubeName : (cube?.title ?? titleize(cubeName));

  // @ts-ignore
  const { title, description } = cube || {};

  const folders = cube?.folders || [];
  const hierarchies = cube?.hierarchies || [];
  const hierarchyNames = hierarchies.map((h) => h.name);
  const [openTimeDimensions, setOpenTimeDimensions] = useState<string[]>([]);
  const [openFolders, setOpenFolders] = useState<string[]>([]);
  const [openHierarchies, setOpenHierarchies] = useState<string[]>([]);

  const folderMembers = folders.reduce((acc, folder) => {
    return acc.concat(folder.members);
  }, [] as string[]);
  const hierarchyMembers = hierarchies.reduce((acc, hierarchy) => {
    return acc.concat(hierarchy.levels);
  }, [] as string[]);
  const importedDimensionsInHierarchies = hierarchies
    .reduce((acc, hierarchy) => {
      return acc.concat(hierarchy.levels.filter((level) => cube && !level.startsWith(cube?.name)));
    }, [] as string[])
    .map((member) => members.dimensions[member])
    .filter(Boolean);
  const importedDimensionsInHierarchiesNames = importedDimensionsInHierarchies.map((d) => d.name);

  const cubeDimensions = (cube?.dimensions ?? [])
    .map((d) => d.name)
    .concat(importedDimensionsInHierarchiesNames);
  const cubeMeasures = cube?.measures.map((m) => m.name) ?? [];
  const cubeSegments = cube?.segments.map((s) => s.name) ?? [];

  missingMembers.forEach(({ name, category }) => {
    switch (category) {
      case 'measures':
        if (name.startsWith(`${cubeName}.`) && !cubeMeasures.includes(name)) {
          cubeMeasures.push(name);
        }
        break;
      case 'segments':
        if (name.startsWith(`${cubeName}.`) && !cubeSegments.includes(name)) {
          cubeSegments.push(name);
        }
        break;
      case 'dimensions':
      case 'timeDimensions':
        if (name.startsWith(`${cubeName}.`) && !cubeDimensions.includes(name)) {
          cubeDimensions.push(name);
        }
        break;
    }
  });

  const {
    measures: filteredMeasures,
    dimensions: filteredDimensions,
    segments: filteredSegments,
    folders: filteredFolders,
    hierarchies: filteredHierarchies,
    members: filteredMembers,
  } = useFilteredMembers(
    filterString || '',
    {
      measures: cubeMeasures.map((m) => members.measures[m] ?? { name: m, type: 'number' }),
      dimensions: cubeDimensions.map((d) => members.dimensions[d] ?? { name: d }),
      segments: cubeSegments.map((s) => members.segments[s] ?? { name: s }),
      folders: cube?.folders ?? [],
      hierarchies: cube?.hierarchies ?? [],
    },
    memberViewType
  );

  const filteredDimensionNames = filteredDimensions.map((dimension) => dimension.name);
  const filteredFolderNames = filteredFolders.map((folder) => folder.name);
  const filteredHierarchyNames = filteredHierarchies.map((hierarchy) => hierarchy.name);
  const filteredMemberNames = filteredMembers.map((member) => member.name);

  function filterMembers(members: string[]) {
    return members.filter(
      (m) =>
        (mode === 'all' && isOpen) ||
        filterString ||
        usedMembers.filter((m) => m.startsWith(`${cubeName}.`)).includes(m)
    );
  }

  const dimensions = filterMembers(
    filterString ? filteredDimensions.map((d) => d.name) : cubeDimensions
  );
  const measures = filterMembers(filterString ? filteredMeasures.map((m) => m.name) : cubeMeasures);
  const segments = filterMembers(filterString ? filteredSegments.map((s) => s.name) : cubeSegments);

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

  const showMembers = (isOpen || mode === 'query' || isUsed || !!filterString) && !isNonJoinable;

  function cacheOfMembers(members?: string[], ignore?: string[]) {
    return (
      members
        ?.filter((dim) => dim.startsWith(`${cubeName}.`) || ignore?.includes(dim))
        .sort()
        .join() || ''
    );
  }

  // Opens folders that contain specific member
  const openContainingFolder = function (name: string) {
    const foldersToOpen = folders.filter((folder) => folder.members.includes(name));

    if (foldersToOpen.length) {
      setOpenFolders(() => {
        const newOpenFolders: string[] = [];

        foldersToOpen.forEach((folder) => {
          if (!newOpenFolders.includes(folder.name)) {
            newOpenFolders.push(folder.name);
          }
        });

        return newOpenFolders;
      });
    } else {
      setOpenFolders([]);
    }
  };

  const toggleTimeDimension = useEvent((isOpen: boolean, name: string) => {
    if (mode === 'query' || !!filterString) {
      onMemberToggle?.(name);
      if (isOpen) {
        setOpenTimeDimensions([name]);
        openContainingFolder(name);
        setOpenHierarchies([]);
      }
    } else {
      if (!isOpen) {
        setOpenTimeDimensions((timeDimensions) => timeDimensions.filter((f) => f !== name));
      } else {
        setOpenTimeDimensions((timeDimensions) => timeDimensions.concat([name]));
      }
    }
  });

  const dimensionsElementMap = useMemo(() => {
    return cubeDimensions.reduce(
      (map, memberName) => {
        const member = members.dimensions[memberName];
        const timeDimension = usedGranularities.find((td) => td.dimension === memberName);
        const isSelected = query?.dimensions?.includes(memberName) ?? false;
        const isImported = importedDimensionsInHierarchiesNames.includes(memberName);

        if (!member || !cube) {
          const missingMember = missingMembers.find((m) => m.name === memberName);

          if (
            !missingMember ||
            (missingMember.category !== 'dimensions' && missingMember.category !== 'timeDimensions')
          ) {
            return map;
          }

          if (missingMember.category === 'dimensions') {
            map[memberName] = (
              <ListMember
                key={memberName}
                cube={cube ?? { name: cubeName }}
                member={{ name: memberName, type: undefined }}
                category="dimensions"
                filterString={filterString}
                memberViewType={memberViewType}
                isImported={isImported}
                isMissing={true}
                isSelected={isSelected}
                isFiltered={usedMembersInFilters.includes(memberName)}
                onRemoveFilter={removeFilter}
                onToggle={() => {
                  dimensionsUpdater?.toggle(memberName);
                  onMemberToggle?.(memberName);
                }}
              />
            );
          } else {
            map[memberName] = (
              <TimeListMember
                key={memberName}
                isMissing
                cube={cube ?? { name: cubeName }}
                member={{ name: memberName, type: 'time' }}
                filterString={filterString}
                selectedGranularities={timeDimension?.granularities}
                memberViewType={memberViewType}
                isSelected={(granularity) => {
                  if (granularity) {
                    return timeDimension?.granularities.includes(granularity) ?? false;
                  }

                  return isSelected;
                }}
                isFiltered={usedMembersInFilters.includes(memberName)}
                isDateRangeFiltered={dateRanges.list.includes(memberName)}
                onDimensionToggle={(dimension) => {
                  dimensionsUpdater?.toggle(dimension);
                  onMemberToggle?.(dimension);
                }}
                onGranularityToggle={(name, granularity) => {
                  grouping.toggle(name, granularity);
                }}
                onAddDataRange={addDateRange}
                onRemoveDataRange={removeDateRange}
                onAddFilter={addFilter}
                onRemoveFilter={removeFilter}
                onToggle={toggleTimeDimension}
              />
            );
          }
        } else if (cube && member?.type === 'time') {
          map[memberName] = (
            <TimeListMember
              key={memberName}
              isOpen={!filterString && mode === 'all' && openTimeDimensions.includes(memberName)}
              cube={cube}
              member={member}
              filterString={filterString}
              memberViewType={memberViewType}
              selectedGranularities={timeDimension?.granularities}
              isSelected={(granularity) => {
                if (granularity) {
                  return timeDimension?.granularities.includes(granularity) ?? false;
                }

                return isSelected;
              }}
              isDateRangeFiltered={dateRanges.list.includes(memberName)}
              isFiltered={usedMembersInFilters.includes(memberName)}
              onDimensionToggle={(dimension) => {
                dimensionsUpdater?.toggle(dimension);
                onMemberToggle?.(dimension);
              }}
              onGranularityToggle={(name, granularity) => {
                grouping.toggle(name, granularity);
              }}
              onAddDataRange={addDateRange}
              onRemoveDataRange={removeDateRange}
              onAddFilter={addFilter}
              onRemoveFilter={removeFilter}
              onToggle={toggleTimeDimension}
            />
          );
        } else {
          map[memberName] = (
            <ListMember
              key={memberName}
              cube={cube}
              member={member}
              category="dimensions"
              filterString={filterString}
              memberViewType={memberViewType}
              isImported={isImported}
              isSelected={isSelected}
              isFiltered={usedMembersInFilters.includes(memberName)}
              onAddFilter={addFilter}
              onRemoveFilter={removeFilter}
              onToggle={() => {
                dimensionsUpdater?.toggle(memberName);
                onMemberToggle?.(memberName);
              }}
            />
          );
        }

        return map;
      },
      {} as Record<string, ReactElement>
    );
  }, [
    cacheOfMembers(query.dimensions, importedDimensionsInHierarchiesNames),
    cacheOfMembers(usedMembersInFilters),
    cacheOfMembers(dateRanges.list),
    JSON.stringify(query.timeDimensions?.filter((dim) => dim.dimension.startsWith(`${cubeName}.`))),
    meta,
    openTimeDimensions.join(),
    memberViewType,
    filterString,
  ]);

  const toggleFolder = useEvent((isOpen: boolean, name: string) => {
    if (mode === 'query' || !!filterString) {
      onHierarchyToggle?.(cube?.name);
      if (isOpen) {
        setOpenFolders([name]);
        setOpenTimeDimensions([]);
        setOpenHierarchies([]);
      }
    } else {
      if (!isOpen) {
        setOpenFolders((openFolders) => openFolders.filter((f) => f !== name));
      } else {
        setOpenFolders((openFolders) => openFolders.concat([name]));
      }
    }
  });

  const toggleHierarchy = useEvent((isOpen: boolean, name: string) => {
    if (mode === 'query' || !!filterString) {
      onHierarchyToggle?.(cube?.name);
      if (isOpen) {
        setOpenHierarchies([name]);
        setOpenTimeDimensions([]);
        openContainingFolder(name);
      }
    } else {
      if (!isOpen) {
        setOpenHierarchies((openHierarchies) => openHierarchies.filter((f) => f !== name));
      } else {
        setOpenHierarchies((openHierarchies) => openHierarchies.concat([name]));
      }
    }
  });

  const measuresElementMap = useMemo(() => {
    return cubeMeasures.reduce(
      (map, memberName) => {
        const member = cube?.measures?.find((m) => m.name === memberName);

        if (!member || !cube) {
          const missingMember = missingMembers.find((m) => m.name === memberName);

          if (!missingMember || missingMember.category !== 'measures') {
            return map;
          }

          map[memberName] = (
            <ListMember
              key={memberName}
              cube={cube ?? { name: cubeName }}
              member={{ name: memberName, type: 'number' }}
              category="measures"
              filterString={filterString}
              memberViewType={memberViewType}
              isMissing={true}
              isSelected={query.measures?.includes(memberName) ?? false}
              isFiltered={usedMembersInFilters.includes(memberName)}
              onRemoveFilter={removeFilter}
              onToggle={() => {
                measuresUpdater?.toggle(memberName);
                onMemberToggle?.(memberName);
              }}
            />
          );
        } else {
          map[memberName] = (
            <ListMember
              key={memberName}
              cube={cube}
              member={member}
              category="measures"
              filterString={filterString}
              memberViewType={memberViewType}
              isSelected={query.measures?.includes(memberName) ?? false}
              isFiltered={usedMembersInFilters.includes(memberName)}
              onAddFilter={addFilter}
              onRemoveFilter={removeFilter}
              onToggle={() => {
                measuresUpdater?.toggle(memberName);
                onMemberToggle?.(memberName);
              }}
            />
          );
        }

        return map;
      },
      {} as Record<string, ReactElement>
    );
  }, [
    cacheOfMembers(query.measures),
    cacheOfMembers(usedMembersInFilters),
    meta,
    memberViewType,
    filterString,
  ]);

  const segmentElementMap = useMemo(() => {
    return cubeSegments.reduce(
      (map, memberName) => {
        const member = cube?.segments?.find((s) => s.name === memberName);

        if (!member || !cube) {
          const missingMember = missingMembers.find((m) => m.name === memberName);

          if (!missingMember || missingMember.category !== 'segments') {
            return map;
          }

          map[memberName] = (
            <ListMember
              key={memberName}
              cube={cube ?? { name: cubeName }}
              member={{ name: memberName }}
              category="segments"
              filterString={filterString}
              memberViewType={memberViewType}
              isMissing={true}
              isSelected={true}
              onToggle={() => {
                dimensionsUpdater?.toggle(memberName);
                onMemberToggle?.(memberName);
              }}
            />
          );
        } else {
          map[memberName] = (
            <ListMember
              key={memberName}
              cube={cube}
              member={member}
              category="segments"
              memberViewType={memberViewType}
              isSelected={usedMembers.includes(memberName)}
              onToggle={() => {
                segmentsUpdater?.toggle(memberName);
              }}
            />
          );
        }

        return map;
      },
      {} as Record<string, ReactElement>
    );
  }, [cacheOfMembers(query.segments), meta, memberViewType, filterString]);

  const membersByFolderMap = folders.reduce(
    (acc, folder) => {
      acc[folder.name] = [];
      acc[folder.name].push(
        // sort hierarchies and dimensions together
        ...[
          ...hierarchyNames.filter((hierarchy) => folder.members.includes(hierarchy)),
          ...dimensions.filter((dimension) => folder.members.includes(dimension)),
        ].sort(sortFn),
        ...measures.filter((measure) => folder.members.includes(measure)).sort(sortFn),
        ...segments.filter((segment) => folder.members.includes(segment)).sort(sortFn)
      );

      return acc;
    },
    {} as Record<string, string[]>
  );

  membersByFolderMap[''] = [
    // sort hierarchies and dimensions together
    ...[
      ...hierarchyNames.filter((hierarchy) => !folderMembers.includes(hierarchy)),
      ...dimensions.filter(
        (dimension) => !folderMembers.includes(dimension) && !hierarchyMembers.includes(dimension)
      ),
    ].sort(sortFn),
    ...measures.filter((measure) => !folderMembers.includes(measure)).sort(sortFn),
    ...segments.filter((segment) => !folderMembers.includes(segment)).sort(sortFn),
  ];

  // When switching between to and from search mode reset the open instances
  // Leave the latest one, because that one is selected by the switch
  useEffect(() => {
    if (openFolders.length > 1) {
      setOpenFolders([openFolders.slice(-1)[0]]);
    }
    if (openHierarchies.length > 1) {
      setOpenHierarchies([openHierarchies.slice(-1)[0]]);
    }
    if (openTimeDimensions.length > 1) {
      setOpenTimeDimensions([openTimeDimensions.slice(-1)[0]]);
    }
  }, [filterString]);

  useEffect(() => {
    const folderNames = folders.map((folder) => folder.name);

    if (mode === 'all') {
      setOpenFolders(
        folderNames.filter((folderName) =>
          membersByFolderMap[folderName].some(
            (memberName) => usedMembers.includes(memberName) || usedHierarchies.includes(memberName)
          )
        )
      );
    } else {
      setOpenFolders([]);
      setOpenHierarchies([]);
      setOpenTimeDimensions([]);
    }
  }, [mode]);

  useEffect(() => {
    if (!isOpen) {
      setOpenFolders([]);
      setOpenHierarchies([]);
      setOpenTimeDimensions([]);
    }
  }, [isOpen]);

  useEffect(() => {
    const closeHiddenMembers = (openMembers: string[]) => {
      return openMembers.filter((memberName) => {
        return (
          // if the hierarchy on the top level
          membersByFolderMap[''].includes(memberName) ||
          // or if an open folder contains it
          openFolders.some((folderName) => {
            return folders
              .find((folder) => folder.name === folderName)
              ?.members.includes(memberName);
          })
        );
      });
    };

    // When open folders changes, close all open hierarchies within closed folders
    setOpenHierarchies(closeHiddenMembers);
    // When open folders changes, close all open time dimensions within closed folders
    setOpenTimeDimensions(closeHiddenMembers);
  }, [openFolders]);

  let mapElements = (members: string[], skipHierarchies = false) =>
    members
      .map(
        (memberName: string) =>
          dimensionsElementMap[memberName] ??
          measuresElementMap[memberName] ??
          segmentElementMap[memberName] ??
          (!skipHierarchies && hierarchiesElementMap[memberName])
      )
      .filter((el) => el);

  const hierarchiesElementMap = useMemo(() => {
    return hierarchies.reduce(
      (map: Record<string, ReactElement | null>, hierarchy: TCubeHierarchy) => {
        const isHierarchyOpen = openHierarchies.includes(hierarchy.name);
        const shownDimensions: string[] = hierarchy.levels.filter((dimensionName: string) =>
          // Show all members if open and used ones when it's closed
          !filterString
            ? isHierarchyOpen || usedMembers?.includes(dimensionName)
            : filteredDimensionNames.includes(dimensionName)
        );
        const children = mapElements(shownDimensions, true);
        const isFiltered = filterString && filteredHierarchyNames.includes(hierarchy.name);

        map[hierarchy.name] =
          // That the place where we also hide the hierarchy if we show only used member
          // and there are none of the inside this hierarchy
          (!filterString && (mode === 'all' || shownDimensions.length) && isOpen) || isFiltered ? (
            <HierarchyMember
              key={hierarchy.name}
              cube={cube as Cube}
              isOpen={isHierarchyOpen && mode === 'all' && !filterString}
              member={hierarchy}
              memberViewType={memberViewType}
              filterString={filterString}
              onToggle={toggleHierarchy}
            >
              {children}
            </HierarchyMember>
          ) : null;

        return map;
      },
      {} as Record<string, ReactElement | null>
    );
  }, [
    openHierarchies.join(),
    dimensionsElementMap,
    meta,
    mode,
    memberViewType,
    filterString,
    isOpen,
  ]);

  if (filterString && isNonJoinable) {
    return null;
  }

  const usedHierarchies = hierarchies
    .filter((hierarchy) => {
      return hierarchy.levels.find((member) => usedMembers.includes(member));
    })
    .map((hierarchy) => hierarchy.name);

  const memberList = (() => {
    if (showMembers) {
      if (mode === 'all' || !cube || queryStats[cubeName]?.isUsed) {
        return (
          <Space flow="column" gap="1bw" padding="1ow 1ow 0 2.5x">
            {folders.map((folder) => {
              const isFolderOpen = openFolders.includes(folder.name);
              const shownMembers = membersByFolderMap[folder.name].filter((memberName) =>
                !filterString
                  ? isFolderOpen
                    ? true
                    : usedMembers.includes(memberName) || usedHierarchies.includes(memberName)
                  : filteredMemberNames.includes(memberName) ||
                    filteredHierarchyNames.includes(memberName)
              );
              const children = mapElements(shownMembers);

              return (
                !filterString
                  ? (mode === 'all' || shownMembers.length) && isOpen
                  : filteredFolderNames.includes(folder.name)
              ) ? (
                <Folder
                  key={folder.name}
                  name={folder.name}
                  isOpen={!filterString && mode === 'all' && isFolderOpen}
                  filterString={filterString}
                  onToggle={toggleFolder}
                >
                  {children}
                </Folder>
              ) : null;
            })}
            {mapElements(
              membersByFolderMap[''].filter((memberName) =>
                !filterString
                  ? mode === 'all' ||
                    usedMembers.includes(memberName) ||
                    usedHierarchies.includes(memberName)
                  : filteredMemberNames.includes(memberName) ||
                    filteredHierarchyNames.includes(memberName)
              )
            )}
            {mode === 'query' && !isMissing && onToggle && queryStats[cube?.name]?.isUsed ? (
              <Button
                type="neutral"
                size="small"
                icon={<ChevronIcon direction="bottom" />}
                placeContent="start"
                onPress={() => onToggle(true)}
              >
                Show all members
              </Button>
            ) : null}
          </Space>
        );
      } else if (filterString) {
        return null;
      } else if (isOpen) {
        return (
          <Block padding=".5x 0 .5x 4.5x">No members{mode === 'query' ? ' selected' : ''}</Block>
        );
      }
    } else {
      return null;
    }
  })();

  const isLocked = isOpen && type === 'view' && !isQueryEmpty;
  const isCollapsable = isNonJoinable || !!filterString;
  const cubeButton = (
    <CubeButton
      qaVal={cubeName}
      icon={
        type === 'cube' ? (
          <CubeIcon color={isMissing ? '#danger-text' : '#purple'} />
        ) : (
          <ViewIcon color={isMissing ? '#danger-text' : '#purple'} />
        )
      }
      rightIcon={
        mode === 'all' && !isNonJoinable && !isLocked ? (
          <ArrowIconWrapper>
            <ChevronIcon
              direction={!isCollapsable ? (isOpen ? 'top' : 'bottom') : 'right'}
              style={{ color: 'var(--purple-color)' }}
            />
          </ArrowIconWrapper>
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
      onPress={() => !isMissing && !isNonJoinable && !isLocked && onToggle?.(!isOpen)}
    >
      <Text ref={textRef} ellipsis>
        {filterString ? <FilteredLabel text={shownName} filter={filterString} /> : shownName}
      </Text>
      <Space padding=".5x right">
        {description ? <ItemInfoIcon description={description} /> : undefined}
        {isPrivate ? <NonPublicIcon type="cube" /> : undefined}
      </Space>
    </CubeButton>
  );

  return (
    <Space flow="column" gap="0">
      <CubeWrapper>
        <InstanceTooltipProvider name={cubeName} title={title} overflowRef={textRef}>
          {cubeButton}
        </InstanceTooltipProvider>
      </CubeWrapper>
      {memberList}
    </Space>
  );
}
