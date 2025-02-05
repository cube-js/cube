import {
  Badge,
  Button,
  DialogContainer,
  Flex,
  Radio,
  Panel,
  SearchInput,
  Space,
  tasty,
  Text,
  Title,
  CloseIcon,
  TooltipProvider,
  ResizablePanel,
  ClearIcon,
} from '@cube-dev/ui-kit';
import {
  ReactNode,
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import { EditOutlined, LoadingOutlined, StarFilled, StarOutlined } from '@ant-design/icons';

import { useDebouncedValue, useFilteredCubes, useEvent, useLocalStorage } from './hooks';
import { useQueryBuilderContext } from './context';
import { EditQueryDialogForm } from './components/EditQueryDialogForm';
import { SidePanelCubeItem } from './components/SidePanelCubeItem';
import { validateQuery } from './utils';

const DEFAULT_SIDEBAR_SIZE = 315;

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
  customTypeSwitcher?: ReactNode;
  showEditQueryButton?: boolean;
  width?: string;
};

export function QueryBuilderSidePanel({
  defaultSelectedType = 'cubes',
  customTypeSwitcher = null,
  showEditQueryButton = true,
  width,
}: Props) {
  const {
    query,
    cubes: cubesAndViews = [],
    selectCube,
    isQueryEmpty,
    joinableCubes,
    isCubeUsed,
    meta,
    isVerifying,
    clearQuery,
    setQuery,
    usedCubes,
    usedMembers,
    queryStats,
    members,
    missingCubes,
    apiVersion,
    isMetaLoading,
    memberViewType,
    disableSidebarResizing,
  } = useQueryBuilderContext();

  const contentRef = useRef<HTMLDivElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const [scrollToCubeName, setScrollToCubeName] = useState<string | null>(null);

  const [viewMode, setViewMode] = useState<'all' | 'query'>(!usedCubes.length ? 'all' : 'query');
  const [isPasteDialogOpen, setIsPasteDialogOpen] = useState(false);
  const [filterString, setFilterString] = useState('');

  const [selectedType, setSelectedType] = useState<'cubes' | 'views'>(defaultSelectedType);

  const [sidebarSize, setSidebarSize] = useLocalStorage(
    'QueryBuilder:Sidebar:size',
    DEFAULT_SIDEBAR_SIZE
  );

  const cubes = cubesAndViews.filter((item) => item.type === 'cube');
  const views = cubesAndViews.filter((item) => item.type === 'view');

  const preparedFilterString = filterString.trim().toLowerCase();
  const debouncedFilterString = useDebouncedValue(preparedFilterString, 500);
  const appliedFilterString = preparedFilterString.length < 2 ? '' : debouncedFilterString;

  const cubesOrViews = selectedType === 'cubes' ? cubes : views;
  const allJoinableCubes =
    selectedType === 'views' && usedCubes.length
      ? cubesOrViews.filter((cube) => usedCubes[0] === cube.name)
      : cubesOrViews.filter((cube) => joinableCubes.includes(cube));

  const [openCubes, setOpenCubes] = useState<Set<string>>(new Set());

  useLayoutEffect(() => {
    if (isQueryEmpty) {
      setOpenCubes(cubesOrViews.length === 1 ? new Set([cubesOrViews[0].name]) : new Set());
    }
  }, [cubesOrViews.length, selectedType]);

  const highlightedCubes = appliedFilterString ? usedCubes : [];

  cubesOrViews.sort((a, b) => {
    if (highlightedCubes.includes(a.name) && !highlightedCubes.includes(b.name)) {
      return -1;
    }

    if (!highlightedCubes.includes(a.name) && highlightedCubes.includes(b.name)) {
      return 1;
    }

    return memberViewType === 'name'
      ? a.name.localeCompare(b.name)
      : a.title.localeCompare(b.title);
  });

  // Filtered cubes
  const filteredCubes = useFilteredCubes(
    appliedFilterString,
    allJoinableCubes,
    memberViewType
  ).cubes.map((cube) => cube.name);

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
    const description = `Search ${selectedType === 'cubes' ? 'cubes' : 'views'} and members`;

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

  useEffect(() => {
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
      });

      setScrollToCubeName(null);
    }
  }, [scrollToCubeName]);

  // Close all disabled cubes to avoid layout shift on deselecting member.
  useEffect(() => {
    const currentSize = openCubes.size;
    const allJoinableCubeNames = allJoinableCubes.map((cube) => cube.name);

    openCubes.forEach((cubeName) => {
      if (!allJoinableCubeNames.includes(cubeName) && !missingCubes.includes(cubeName)) {
        openCubes.delete(cubeName);
      }
    });

    if (currentSize !== openCubes.size) {
      setOpenCubes(new Set(openCubes));
    }
  }, [openCubes.size, missingCubes.length, allJoinableCubes.length]);

  function resetState(cubeName?: string) {
    setFilterString('');
    setViewMode('all');

    if (cubeName) {
      setOpenCubes(new Set([cubeName]));
      setScrollToCubeName(cubeName);
    }
  }

  function onCubeToggle(name: string, isOpen: boolean) {
    if (appliedFilterString || viewMode === 'query') {
      resetState(name);

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
    const isTimeDimension = members.dimensions[memberName]?.type === 'time';

    // Always reset state if we click on time dimension
    if (isTimeDimension || (appliedFilterString && !usedMembers.includes(memberName))) {
      resetState(cubeName);
    }
  });

  const onHierarchyToggle = useEvent((cubeName?: string) => {
    if (appliedFilterString || viewMode === 'query') {
      resetState(cubeName);
    }
  });

  const cubeList = useMemo(() => {
    return (
      <Flex gap="1bw" flow="column" padding="0 0 2x 0">
        {missingCubes
          .filter((cubeName) => (appliedFilterString ? filteredCubes.includes(cubeName) : true))
          .map((cubeName) => (
            <SidePanelCubeItem
              key={cubeName}
              isOpen={openCubes.has(cubeName)}
              filterString={appliedFilterString}
              cubeName={cubeName}
              mode={viewMode}
              rightIcon="arrow"
              onHierarchyToggle={onHierarchyToggle}
              onMemberToggle={(name) => {
                onMemberToggle(cubeName, name);
              }}
            />
          ))}
        {cubesOrViews
          .filter((cube) =>
            appliedFilterString
              ? // If filter is applied, show only filtered cubes
                filteredCubes.includes(cube.name)
              : viewMode === 'query'
                ? // In query mode, show only used cubes
                  usedCubes.includes(cube.name)
                : true
          )
          .map((cube) => (
            <SidePanelCubeItem
              key={cube.name}
              isNonJoinable={!allJoinableCubes.includes(cube) && !usedCubes.includes(cube.name)}
              isOpen={openCubes.has(cube.name)}
              filterString={appliedFilterString}
              cubeName={cube.name}
              mode={viewMode}
              rightIcon={isQueryEmpty ? 'arrow' : 'plus'}
              onToggle={(isOpen) => {
                onCubeToggle(cube.name, isOpen);
              }}
              onMemberToggle={(name) => {
                onMemberToggle(cube.name, name);
              }}
              onHierarchyToggle={onHierarchyToggle}
            />
          ))}
      </Flex>
    );
  }, [
    viewMode,
    queryStats,
    [...openCubes.values()].join(),
    appliedFilterString,
    memberViewType,
    selectedType,
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

      setScrollToCubeName(usedCubes[0]);

      if (isQueryEmpty) {
        setViewMode('all');
      }
    }
  }, [viewMode, isQueryEmpty]);

  const topBar = useMemo(() => {
    return (
      <Space placeContent="space-between" gap="1x">
        <Space gap="1x">
          {showEditQueryButton ? editQueryButton : null}
          {!usedCubes.length ? (
            <Title preset="h6">All members</Title>
          ) : (
            <TooltipProvider
              title={'Toggle between all members and only those that are used in the query'}
              placement="top"
            >
              <Button
                qa="ToggleMembersButton"
                qaVal={viewMode === 'all' ? 'all' : 'used'}
                type={viewMode === 'all' ? 'outline' : 'primary'}
                size="small"
                icon={viewMode === 'all' ? <StarOutlined /> : <StarFilled />}
                onPress={() => setViewMode(viewMode === 'all' ? 'query' : 'all')}
              >
                {viewMode === 'all' ? 'All members' : 'Used only'}
              </Button>
            </TooltipProvider>
          )}
          {isVerifying || isMetaLoading ? <LoadingOutlined /> : null}
        </Space>
        <Space gap=".5x">
          <TooltipProvider title="Reset the query">
            <Button
              qa="ResetQuery"
              aria-label="Reset the query"
              size="small"
              type="secondary"
              theme="danger"
              icon={<ClearIcon />}
              onPress={() => {
                clearQuery();
                setOpenCubes(
                  cubesOrViews.length === 1 ? new Set([cubesOrViews[0].name]) : new Set()
                );
                resetScrollAndContentSize();
              }}
            >
              Reset
            </Button>
          </TooltipProvider>
        </Space>
      </Space>
    );
  }, [viewMode, isQueryEmpty, isMetaLoading, usedMembers.length, appliedFilterString, isVerifying]);

  const content = (
    <>
      <DialogContainer isOpen={isPasteDialogOpen} onDismiss={() => setIsPasteDialogOpen(false)}>
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

      <Panel margin="0 -1x" border="top" flexGrow={1}>
        {cubeList}
      </Panel>
    </>
  );

  return disableSidebarResizing ? (
    <Panel
      key="disabled-siderbar-resizing"
      ref={containerRef}
      isFlex
      qa="QueryBuilderSidePanel"
      flow="column"
      padding="1x 1x 0 1x"
      gap="1x"
      border="1ow right"
      width={width ?? `max ${DEFAULT_SIDEBAR_SIZE}px`}
      innerStyles={{
        overflowX: 'clip',
      }}
    >
      {content}
    </Panel>
  ) : (
    <ResizablePanel
      key="resizable-siderbar"
      ref={containerRef}
      isFlex
      qa="QueryBuilderSidePanel"
      flow="column"
      direction="right"
      size={disableSidebarResizing ? DEFAULT_SIDEBAR_SIZE : sidebarSize}
      isDisabled={disableSidebarResizing}
      minSize={DEFAULT_SIDEBAR_SIZE}
      maxSize="35%"
      padding="1x 1x 0 1x"
      gap="1x"
      innerStyles={{
        overflowX: 'clip',
      }}
      onSizeChange={setSidebarSize}
    >
      {content}
    </ResizablePanel>
  );
}
