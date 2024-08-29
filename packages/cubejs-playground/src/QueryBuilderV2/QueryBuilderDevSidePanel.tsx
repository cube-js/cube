import {
  Badge,
  Block,
  Button,
  DialogContainer,
  Divider,
  Flex,
  Grid,
  Radio,
  SearchInput,
  Space,
  tasty,
  Text,
  Title,
} from '@cube-dev/ui-kit';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { EditOutlined } from '@ant-design/icons';
import { TCubeDimension, validateQuery } from '@cubejs-client/core';

import { useDeepMemo, useEvent, usePrevious } from './hooks';
import { Panel } from './components/Panel';
import { QueryVisualization } from './components/QueryVisualization';
import { ListCube } from './components/ListCube';
import { ListMember } from './components/ListMember';
import { useQueryBuilderContext } from './context';
import { TimeListMember } from './components/TimeListMember';
import { EditQueryDialogForm } from './components/EditQueryDialogForm';
import { useFilteredMembers } from './hooks/filtered-members';
import { useFilteredCubes } from './hooks/filtered-cubes';
import { MemberSection } from './components/MemberSection';

const RadioButton = tasty(Radio.Button, {
  styles: { flexGrow: 1, placeItems: 'stretch' },
  inputStyles: { textAlign: 'center' },
});

const CountBadge = tasty(Badge, {
  styles: {
    fill: '#purple',
    border: '#purple',
    color: '#white',
    padding: '0 1ow',
  },
});

const StyledDivider = tasty(Divider, {
  styles: {
    gridArea: 'initial',
    margin: '0 -1x',
  },
});

export function QueryBuilderDevSidePanel() {
  const {
    query,
    queryHash,
    cubes: items = [],
    selectCube,
    isQueryEmpty,
    dateRanges,
    measures: measuresUpdater,
    dimensions: dimensionsUpdater,
    segments: segmentsUpdater,
    grouping,
    filters,
    joinableCubes,
    isCubeJoined,
    selectedCube,
    meta,
    apiVersion,
    setQuery,
  } = useQueryBuilderContext();
  const [isPasteDialogOpen, setIsPasteDialogOpen] = useState(false);
  const [filterString, setFilterString] = useState('');
  const previousFilterString = usePrevious(filterString);
  const isMemberFilterOnly = useRef(false);

  const contentRef = useRef<HTMLDivElement>(null);
  const [selectedType, setSelectedType] = useState<'cubes' | 'views'>('cubes');

  items.sort((a, b) => a.name.localeCompare(b.name));

  const cubes = items
    // @ts-ignore
    .filter((item) => item.type === 'cube')
    .filter((cube) => joinableCubes.includes(cube));
  // @ts-ignore
  const views = items.filter((item) => item.type === 'view');

  const preparedFilterString = filterString.trim().replaceAll('_', ' ');

  // Filtered members
  const measures = selectedCube?.measures || [];
  const dimensions = selectedCube?.dimensions || [];
  const segments = selectedCube?.segments || [];

  const {
    measures: shownMeasures,
    dimensions: shownDimensions,
    segments: shownSegments,
  } = useFilteredMembers(preparedFilterString, {
    measures,
    dimensions,
    segments,
  });

  // Filtered cubes
  const {
    cubes: shownCubes,
    membersByCube: filteredMembersByCube,
    isFiltered: areCubesFiltered,
  } = useFilteredCubes(preparedFilterString, selectedType === 'cubes' ? cubes : views);
  const totalCubes = (selectedType === 'cubes' ? cubes : views).length;
  const connectedCubes = joinableCubes.filter((cube) => !isCubeJoined(cube.name));

  const onItemSelect = useEvent((cubeName: string) => {
    selectCube(cubeName);
  });

  const resetScrollAndContentSize = useCallback(() => {
    if (contentRef?.current) {
      const element = contentRef.current;

      element.scrollTop = 0;

      setTimeout(() => {
        element.scrollTop = 0;
      }, 0);
    }
  }, [contentRef?.current]);

  const editQueryButton = useMemo(
    () => (
      <Button
        aria-label="Edit Query"
        type="primary"
        size="small"
        icon={<EditOutlined />}
        onPress={() => setIsPasteDialogOpen(true)}
      />
    ),
    []
  );

  useEffect(() => {
    resetScrollAndContentSize();
  }, [selectedCube?.name, meta]);

  useEffect(() => {
    resetScrollAndContentSize();
  }, [selectedType, preparedFilterString]);

  useEffect(() => {
    const selectedView = views.find((cube) => isCubeJoined(cube.name));
    const selectedCube = cubes.find((cube) => isCubeJoined(cube.name));

    if (selectedType === 'cubes' && selectedView) {
      setSelectedType('views');
      selectCube(selectedView?.name);
    } else if (selectedType === 'views' && selectedCube) {
      setSelectedType('cubes');
    }
  }, [selectedType, query]);

  // Reset filter string when switching between cubes and views if it was applied for members first
  useEffect(() => {
    if (selectedCube && !previousFilterString?.trim().length && filterString.trim().length) {
      isMemberFilterOnly.current = true;
    }
  }, [filterString, selectedCube]);

  useEffect(() => {
    if (isMemberFilterOnly.current && filterString) {
      isMemberFilterOnly.current = false;
      setFilterString('');
    }
  }, [selectedCube]);

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

  const switchType = useCallback(
    async (type: 'cubes' | 'views') => {
      setSelectedType(type);
    },
    [isQueryEmpty]
  );

  const typeSwitcher = useMemo(() => {
    return (
      <Space gap="1x">
        {editQueryButton}
        <Radio.ButtonGroup
          aria-label="Cube type"
          value={selectedType}
          styles={{ flexGrow: 1 }}
          onChange={(val) => switchType(val as 'cubes' | 'views')}
        >
          <RadioButton qa="QueryBuilderTab-cubes" value="cubes" isDisabled={!cubes.length}>
            Cubes <CountBadge radius="1r">{cubes.length}</CountBadge>
          </RadioButton>
          <RadioButton qa="QueryBuilderTab-views" value="views" isDisabled={!views.length}>
            Views <CountBadge radius="1r">{views.length}</CountBadge>
          </RadioButton>
        </Radio.ButtonGroup>
      </Space>
    );
  }, [selectedType, meta, cubes.length, views.length]);

  const searchInput = useMemo(() => {
    const description = `Search for ${
      selectedCube ? 'members' : `${selectedType === 'cubes' ? 'cubes' : 'views'} and members`
    }`;

    return (
      <SearchInput
        isClearable
        qa="QueryBuilderSearch"
        size="small"
        aria-label={description}
        placeholder={description}
        value={filterString}
        onChange={(val) => setFilterString(val)}
      />
    );
  }, [selectedCube, selectedType, meta, filterString]);

  const cubeList = useDeepMemo(() => {
    return (
      <Flex gap="1bw" flow="column">
        {shownCubes
          .filter((cube) => !isCubeJoined(cube.name))
          .map((item) => (
            <ListCube
              key={item.name}
              // @ts-ignore
              type={item.type}
              name={item.name}
              title={item.title}
              // @ts-ignore
              description={item.description}
              // @ts-ignore
              isPrivate={item.public === false}
              stats={areCubesFiltered ? filteredMembersByCube[item.name] : undefined}
              rightIcon={isQueryEmpty ? 'arrow' : 'plus'}
              onItemSelect={() => onItemSelect(item.name)}
            />
          ))}
      </Flex>
    );
  }, [shownCubes, meta]);

  const selectedCubeTitle = useDeepMemo(() => {
    // @ts-ignore
    return selectedCube && selectedCube?.type !== 'view' ? (
      <Grid flow="column" gap=".5x" placeContent="space-between">
        <Title ellipsis gridArea={null} level={5}>
          {selectedCube?.name}
        </Title>
      </Grid>
    ) : null;
  }, [selectedCube?.name, meta, queryHash]);

  const dimensionsSection = useDeepMemo(() => {
    return selectedCube && dimensions.length ? (
      <MemberSection
        name="dimension"
        hasFilter={areCubesFiltered}
        totalItems={dimensions.length}
        totalShownItems={shownDimensions.length}
      >
        {shownDimensions.map((item) =>
          (item as TCubeDimension).type === 'time' ? (
            <TimeListMember
              key={item.name}
              cube={selectedCube}
              member={item as TCubeDimension}
              isSelected={(granularity) => {
                if (granularity) {
                  return (
                    query?.timeDimensions?.some(
                      (td) => td.dimension === item.name && td.granularity === granularity
                    ) || false
                  );
                }

                return query?.dimensions?.includes(item.name) || false;
              }}
              isFiltered={
                query?.timeDimensions?.some((td) => td.dimension === item.name && td.dateRange) ||
                false
              }
              onDimensionToggle={(dimension) => {
                dimensionsUpdater?.toggle(dimension);
              }}
              onGranularityToggle={(name, granularity) => {
                grouping.toggle(name, granularity);
              }}
              onToggleDataRange={
                !dateRanges.list.includes(item.name) ? addDateRange : removeDateRange
              }
            />
          ) : (
            <ListMember
              key={item.name}
              cube={selectedCube}
              member={item}
              category="dimensions"
              isSelected={query?.dimensions?.includes(item.name) || false}
              isFiltered={
                query?.filters?.some(
                  (filter) => 'member' in filter && filter.member === item.name
                ) || false
              }
              onAddFilter={addFilter}
              onRemoveFilter={removeFilter}
              onToggle={() => dimensionsUpdater?.toggle(item.name)}
            />
          )
        )}
      </MemberSection>
    ) : undefined;
  }, [
    meta,
    shownDimensions,
    filterString,
    JSON.stringify(query?.dimensions),
    JSON.stringify(query?.timeDimensions),
    JSON.stringify(query?.filters),
    selectedCube?.name,
  ]);

  const measuresSection = useDeepMemo(() => {
    return selectedCube && measures.length ? (
      <MemberSection
        name="measure"
        hasFilter={areCubesFiltered}
        totalItems={measures.length}
        totalShownItems={shownMeasures.length}
      >
        {shownMeasures.map((item) => (
          <ListMember
            key={item.name}
            cube={selectedCube}
            member={item}
            category="measures"
            isSelected={query?.measures?.includes(item.name) || false}
            isFiltered={
              query?.filters?.some((filter) => 'member' in filter && filter.member === item.name) ||
              false
            }
            onAddFilter={addFilter}
            onToggle={() => measuresUpdater?.toggle(item.name)}
          />
        ))}
      </MemberSection>
    ) : undefined;
  }, [
    meta,
    JSON.stringify(query?.measures),
    JSON.stringify(query?.filters),
    shownMeasures,
    filterString,
    selectedCube?.name,
  ]);

  const segmentsSection = useDeepMemo(() => {
    return selectedCube && segments.length ? (
      <MemberSection
        name="segment"
        hasFilter={areCubesFiltered}
        totalItems={segments.length}
        totalShownItems={shownSegments.length}
      >
        {shownSegments.map((item) => (
          <ListMember
            key={item.name}
            cube={selectedCube}
            member={item}
            category="segments"
            isSelected={query?.segments?.includes(item.name) || false}
            onToggle={() => {
              segmentsUpdater?.toggle(item.name);
            }}
          />
        ))}
      </MemberSection>
    ) : undefined;
  }, [meta, JSON.stringify(query?.segments), shownSegments, filterString, selectedCube?.name]);

  const onPaste = useCallback(async (query) => {
    try {
      const validatedQuery = validateQuery(query);

      setQuery(validatedQuery);
    } catch (e) {
      throw 'Invalid query';
    }
  }, []);

  const rows = `${isQueryEmpty && !selectedCube ? 'max-content ' : ''}${isQueryEmpty && !selectedCube ? '' : 'max-content '} ${selectedCube ? 'max-content ' : ''}${!selectedCube && !isQueryEmpty ? 'max-content' : ''} ${connectedCubes.length && (isQueryEmpty || selectedType !== 'views' || selectedCube) ? 'max-content ' : ''}${
    (!selectedCube && !isQueryEmpty && selectedType === 'cubes') ||
    (filterString.trim().length && !selectedCube && (selectedType === 'cubes' || isQueryEmpty))
      ? 'max-content'
      : ''
  } minmax(0, 1fr)`;

  return (
    <Panel padding="1x 1x 0 1x" gap="1x" gridRows={rows}>
      <DialogContainer
        isDismissable
        isOpen={isPasteDialogOpen}
        onDismiss={() => setIsPasteDialogOpen(false)}
      >
        <EditQueryDialogForm query={query} apiVersion={apiVersion} onSubmit={onPaste} />
      </DialogContainer>

      {/* Navigation */}
      {isQueryEmpty && !selectedCube ? <>{typeSwitcher}</> : undefined}

      <QueryVisualization
        actions={editQueryButton}
        type={selectedType}
        onReset={resetScrollAndContentSize}
      />

      {/* Selected cubes list with stats */}
      {selectedCube ? (
        <Block gap="1x">
          <StyledDivider />
          {selectedCubeTitle}
        </Block>
      ) : undefined}
      {!selectedCube && !isQueryEmpty ? <StyledDivider /> : undefined}
      {connectedCubes.length && (isQueryEmpty || selectedType !== 'views' || selectedCube)
        ? searchInput
        : undefined}
      {!selectedCube && !isQueryEmpty && selectedType === 'cubes' ? (
        <Space placeContent="space-between" placeItems="baseline">
          {connectedCubes.length ? (
            <>
              <Text preset="c2">Connected cubes</Text>
              <Text preset="t3m" color="#dark">
                {connectedCubes.length > shownCubes.length ? `${shownCubes.length}/` : ''}
                {connectedCubes.length}
              </Text>
            </>
          ) : (
            <Text preset="t3m">No cubes to connect</Text>
          )}
        </Space>
      ) : filterString.trim().length &&
        !selectedCube &&
        (selectedType === 'cubes' || isQueryEmpty) ? (
        <Space placeContent="space-between" placeItems="baseline">
          <Text preset="c2">Found</Text>
          <Text preset="t3m" color="#dark">
            {totalCubes > shownCubes.length ? `${shownCubes.length}/` : ''}
            {totalCubes}
          </Text>
        </Space>
      ) : undefined}
      <Panel
        ref={contentRef}
        styles={{
          margin: '0 -1x',
          border: 'top',
        }}
        innerStyles={{ padding: selectedCube ? '1x 1x 2x' : '0 0 2x' }}
      >
        {(connectedCubes.length || selectedCube) &&
        (selectedType === 'cubes' || isQueryEmpty || selectedCube) ? (
          <>
            {!selectedCube ? (
              <>{cubeList}</>
            ) : (
              <Flex gap="2x" flow="column">
                {dimensionsSection}

                {measuresSection}

                {segmentsSection}
              </Flex>
            )}
          </>
        ) : undefined}
      </Panel>
    </Panel>
  );
}
