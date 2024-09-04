import {
  Badge,
  Button,
  DialogContainer,
  Flex,
  Radio,
  SearchInput,
  Space,
  tasty,
  Text,
  Title,
  CloseIcon,
  TooltipProvider,
} from '@cube-dev/ui-kit';
import {
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import {
  EditOutlined,
  LoadingOutlined,
  StarFilled,
  StarOutlined,
} from '@ant-design/icons';
import { validateQuery } from '@cubejs-client/core';

import {
  useDebouncedValue,
  useFilteredCubes,
  useDeepMemo,
  useEvent,
} from './hooks';
import { useQueryBuilderContext } from './context';
import { Panel } from './components/Panel';
import { EditQueryDialogForm } from './components/EditQueryDialogForm';
import { SidePanelCubeItem } from './components/SidePanelCubeItem';

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

type Props = {
  defaultSelectedType?: 'cubes' | 'views';
  customTypeSwitcher?: React.ReactNode;
  showEditQueryButton?: boolean;
};

export function QueryBuilderSidePanel({
  defaultSelectedType = 'cubes',
  customTypeSwitcher = null,
  showEditQueryButton = true,
}: Props) {
  const {
    query,
    cubes: items = [],
    selectCube,
    isQueryEmpty,
    joinableCubes,
    isCubeUsed,
    meta,
    isVerifying,
    clearQuery,
    setQuery,
    usedCubes,
    joinedCubes,
    usedMembers,
    apiVersion,
  } = useQueryBuilderContext();

  const contentRef = useRef<HTMLDivElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const [scrollToCubeName, setScrollToCubeName] = useState<string | null>(null);

  const [viewMode, setViewMode] = useState<'all' | 'query'>(
    !joinedCubes.length ? 'all' : 'query'
  );
  const [openCubes, setOpenCubes] = useState<Set<string>>(
    isQueryEmpty ? new Set() : new Set(usedCubes)
  );
  const [isPasteDialogOpen, setIsPasteDialogOpen] = useState(false);
  const [filterString, setFilterString] = useState('');

  const [selectedType, setSelectedType] = useState<'cubes' | 'views'>(
    defaultSelectedType
  );

  items.sort((a, b) => a.name.localeCompare(b.name));

  // @ts-ignore
  const cubes = items.filter((item) => item.type === 'cube');
  // @ts-ignore
  const views = items.filter((item) => item.type === 'view');

  const preparedFilterString = filterString
    .trim()
    .replaceAll('_', ' ')
    .toLowerCase();
  const debouncedFilterString = useDebouncedValue(preparedFilterString, 500);
  const appliedFilterString =
    preparedFilterString.length < 2 ? '' : debouncedFilterString;

  const allCubes = selectedType === 'cubes' ? cubes : views;
  const allJoinableCubes =
    selectedType === 'views' && usedCubes.length
      ? allCubes.filter((cube) => usedCubes[0] === cube.name)
      : allCubes.filter((cube) => joinableCubes.includes(cube));

  const highlightedCubes = useMemo(() => {
    if (appliedFilterString) {
      return usedCubes;
    }

    return [];
  }, [appliedFilterString]);

  allCubes.sort((a, b) => {
    if (
      highlightedCubes.includes(a.name) &&
      !highlightedCubes.includes(b.name)
    ) {
      return -1;
    }

    if (
      !highlightedCubes.includes(a.name) &&
      highlightedCubes.includes(b.name)
    ) {
      return 1;
    }

    return a.name.localeCompare(b.name);
  });

  // Filtered cubes
  const { cubes: filteredCubes } = useFilteredCubes(
    appliedFilterString,
    allJoinableCubes
  );

  const resetScrollAndContentSize = useCallback(() => {
    if (contentRef?.current) {
      const element = contentRef.current;

      element.scrollTop = 0;

      setTimeout(() => {
        element.scrollTop = 0;
      }, 0);
    }
  }, [contentRef?.current]);

  useEffect(() => {
    resetScrollAndContentSize();

    if (appliedFilterString && viewMode === 'query') {
      setViewMode('all');
    }
  }, [selectedType, appliedFilterString, meta]);

  useEffect(() => {
    const usedView = views.find((cube) => isCubeUsed(cube.name));
    const usedCube = cubes.find((cube) => isCubeUsed(cube.name));

    if (selectedType === 'cubes' && usedView && !usedCube) {
      setSelectedType('views');
      selectCube(usedView?.name);
    } else if (selectedType === 'views' && usedCube) {
      setSelectedType('cubes');
    }
  }, [selectedType, query]);

  const switchType = useCallback(
    async (type: 'cubes' | 'views') => {
      setSelectedType(type);
    },
    [isQueryEmpty]
  );

  const editQueryButton = useMemo(
    () => (
      <Button
        qa="EditQueryButton"
        aria-label="Edit Query"
        type="primary"
        size="small"
        icon={<EditOutlined />}
        onPress={() => setIsPasteDialogOpen(true)}
      />
    ),
    []
  );

  const typeSwitcher = useMemo(() => {
    return (
      <Space qa="QueryBuilderSwitcher" gap="1x">
        {editQueryButton}
        <Radio.ButtonGroup
          aria-label="Cube type"
          value={selectedType}
          styles={{ flexGrow: 1 }}
          onChange={(val) => switchType(val as 'cubes' | 'views')}
        >
          <RadioButton
            qa="QueryBuilderTab-cubes"
            value="cubes"
            isDisabled={!cubes.length}
            inputStyles={{ placeContent: 'center' }}
          >
            Cubes <CountBadge radius="1r">{cubes.length}</CountBadge>
          </RadioButton>
          <RadioButton
            qa="QueryBuilderTab-views"
            value="views"
            isDisabled={!views.length}
            inputStyles={{ placeContent: 'center' }}
          >
            Views <CountBadge radius="1r">{views.length}</CountBadge>
          </RadioButton>
        </Radio.ButtonGroup>
      </Space>
    );
  }, [selectedType, meta, cubes.length, views.length]);

  const searchInput = useMemo(() => {
    const description = `Search ${
      selectedType === 'cubes' ? 'cubes' : 'views'
    } and members`;

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
  }, [selectedType, meta, filterString]);

  useLayoutEffect(() => {
    if (scrollToCubeName) {
      setTimeout(() => {
        const element = containerRef.current?.querySelector(
          `[data-qa="CubeButton"][data-qaval="${scrollToCubeName}"]`
        );

        if (element) {
          element.scrollIntoView({
            block: 'start',
          });
        }
      }, 100);

      setScrollToCubeName(null);
    }
  }, [scrollToCubeName]);

  function onCubeToggle(name: string, isOpen: boolean) {
    if (appliedFilterString || viewMode === 'query') {
      setFilterString('');
      setViewMode('all');
      setOpenCubes(new Set([name]));
      setScrollToCubeName(name);

      return;
    }

    if (isOpen) {
      openCubes.add(name);
    } else {
      openCubes.delete(name);
    }

    setOpenCubes(new Set(openCubes));
  }

  const onMemberToggle = useEvent((cubeName: string, memberName: string) => {
    if (
      appliedFilterString &&
      !query?.dimensions?.includes(memberName) &&
      !query?.measures?.includes(memberName)
    ) {
      setScrollToCubeName(cubeName);
      setFilterString('');
      setViewMode('all');
      setOpenCubes(new Set([cubeName]));
    }
  });

  const cubeList = useDeepMemo(() => {
    return (
      <Flex gap="1ow" flow="column" padding="0 0 2x 0">
        {allCubes.map((item) => (
          <SidePanelCubeItem
            key={item.name}
            isNonJoinable={!allJoinableCubes.includes(item)}
            isOpen={openCubes.has(item.name)}
            isFiltered={filteredCubes.includes(item)}
            filterString={appliedFilterString}
            name={item.name}
            mode={viewMode}
            rightIcon={isQueryEmpty ? 'arrow' : 'plus'}
            onToggle={(isOpen) => {
              onCubeToggle(item.name, isOpen);
            }}
            onMemberToggle={(name) => {
              onMemberToggle(item.name, name);
            }}
          />
        ))}
      </Flex>
    );
  }, [
    allCubes,
    viewMode,
    meta,
    openCubes.size,
    appliedFilterString,
    usedCubes.join(','),
  ]);

  const onApplyQuery = useCallback(async (query) => {
    try {
      const validatedQuery = validateQuery(query);

      setQuery(validatedQuery);
    } catch (e: any) {
      throw 'Invalid query';
    }
  }, []);

  useEffect(() => {
    if (viewMode === 'query') {
      if (filterString) {
        setFilterString('');
      }

      setOpenCubes(new Set(usedCubes));

      if (isQueryEmpty) {
        setViewMode('all');
      }
    }
  }, [viewMode, isQueryEmpty]);

  const topBar = useMemo(() => {
    return (
      <Space placeContent="space-between">
        <Space gap="1x">
          {showEditQueryButton ? editQueryButton : null}
          {!usedCubes.length ? (
            <Title preset="h6">All members</Title>
          ) : (
            <TooltipProvider
              title={
                'Toggle between all members and only those that are used in the query'
              }
              placement="top"
            >
              <Button
                qa="ToggleMembersButton"
                qaVal={viewMode === 'all' ? 'all' : 'used'}
                type={viewMode === 'all' ? 'outline' : 'primary'}
                size="small"
                icon={viewMode === 'all' ? <StarOutlined /> : <StarFilled />}
                onPress={() =>
                  setViewMode(viewMode === 'all' ? 'query' : 'all')
                }
              >
                {viewMode === 'all' ? 'All' : 'Used'} members
              </Button>
            </TooltipProvider>
          )}
          {isVerifying && <LoadingOutlined />}
        </Space>
        <Space gap=".5x">
          <TooltipProvider title="Reset the query">
            <Button
              qa="ResetQuery"
              aria-label="Reset the query"
              size="small"
              type="secondary"
              theme="danger"
              icon={<CloseIcon />}
              onPress={() => {
                clearQuery();
                selectCube(null);
                setOpenCubes(new Set());
                resetScrollAndContentSize();
              }}
            >
              Reset
            </Button>
          </TooltipProvider>
        </Space>
      </Space>
    );
  }, [
    viewMode,
    isQueryEmpty,
    usedMembers.length,
    appliedFilterString,
    isVerifying,
  ]);

  return (
    <Panel
      ref={containerRef}
      padding="1x 1x 0 1x"
      gap="1x"
      gridRows={`max-content max-content ${
        appliedFilterString && !filteredCubes.length ? 'max-content ' : ' '
      } minmax(0, 1fr)`}
    >
      <DialogContainer
        isOpen={isPasteDialogOpen}
        onDismiss={() => setIsPasteDialogOpen(false)}
      >
        <EditQueryDialogForm
          query={query}
          defaultType={'json'}
          apiVersion={apiVersion}
          onSubmit={onApplyQuery}
        />
      </DialogContainer>

      {!usedCubes.length ? <>{customTypeSwitcher ?? typeSwitcher}</> : topBar}

      {searchInput}

      {appliedFilterString && !filteredCubes.length ? (
        <Space
          placeContent="space-between"
          placeItems="baseline"
          border="top"
          margin="0 -1x"
          padding="1x 1x 0 1x"
        >
          {!filteredCubes.length ? (
            <Text preset="c2">
              No {selectedType === 'cubes' ? 'cubes' : 'views'} or members found
            </Text>
          ) : null}
        </Space>
      ) : undefined}

      <Panel
        styles={{
          margin: '0 -1x',
          border: 'top',
        }}
      >
        {cubeList}
      </Panel>
    </Panel>
  );
}
