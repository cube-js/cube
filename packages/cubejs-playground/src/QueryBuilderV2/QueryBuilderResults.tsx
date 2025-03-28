import {
  Button,
  CubeButtonProps,
  Grid,
  Item,
  Menu,
  MenuTrigger,
  mergeProps,
  Paragraph,
  Select,
  Space,
  Styles,
  Tag,
  tasty,
  Text,
  Title,
  Panel,
  CloseIcon,
} from '@cube-dev/ui-kit';
import { Key, ReactNode, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { formatDistance } from 'date-fns';
import {
  ArrowDownOutlined,
  ArrowUpOutlined,
  ClearOutlined,
  FilterOutlined,
  LeftOutlined,
  LoadingOutlined,
  MoreOutlined,
  RightOutlined,
} from '@ant-design/icons';
import { QueryOrder, TimeDimensionGranularity } from '@cubejs-client/core';
import {
  AriaOptionProps,
  DroppableCollectionReorderEvent,
  ListDropTargetDelegate,
  ListKeyboardDelegate,
  useDraggableCollection,
  useDraggableItem,
  useDropIndicator,
  useDroppableCollection,
  useFocusRing,
  useListBox,
  useOption,
} from 'react-aria';
import {
  DraggableCollectionState,
  DroppableCollectionState,
  ListProps,
  ListState,
  useDraggableCollectionState,
  useDroppableCollectionState,
  useListState,
} from 'react-stately';

import { PREDEFINED_GRANULARITIES } from './values';
import { formatCurrency, formatNumber } from './utils/formatters';
import { useDeepMemo, useIntervalEffect } from './hooks';
import { OutdatedLabel } from './components/OutdatedLabel';
import { CopyButton } from './components/CopyButton';
import { ListMemberButton } from './components/ListMemberButton';
import { useQueryBuilderContext } from './context';
import { formatDateByGranularity } from './utils/format-date-by-granularity';
import { MemberBadge } from './components/Badge';
import { MemberLabel } from './components/MemberLabel';
import { areQueriesRelated } from './utils/query-helpers';
import { ORDER_LABEL_BY_TYPE } from './utils/labels';

const StyledTag = tasty(Tag, {
  styles: {
    position: 'static',
  },
});

const StyledCopyButton = tasty(CopyButton, {
  dontShowToast: true,
  styles: {
    radius: 0,
    placeSelf: 'stretch',
    height: 'auto',
  },
});

const TableContainer = tasty({
  qa: 'ResultsTableContainer',
  styles: {
    styledScrollbar: true,
    maxWidth: '100%',
    overflow: 'auto',
  },
});

const TableFooter = tasty(Space, {
  qa: 'ResultsTableFooter',
  styles: {
    fill: '#white',
    padding: '1x',
    width: '100%',
    placeContent: 'center space-between',
    height: '5x',
    border: 'top',
  },
});

const OptionsButtonElement = tasty(ListMemberButton, {
  'aria-label': 'Options',
  icon: <MoreOutlined />,
  styles: {
    color: '#dark',
    gridColumns: 'auto',
    placeContent: 'center',
    padding: 0,
    width: '3.5x',
    height: '4x',
    margin: '-.5x -.5x -.5x .5x',
    ButtonIcon: { fontSize: '20px' },
  },
});

const DisclaimerContainer = tasty({
  styles: {
    display: 'grid',
    gridColumns: 'auto',
    placeContent: 'center',
    placeItems: 'center',
    gap: '2x',
    height: 'min 20x',
    padding: '1x',
  },
});

function getOrderIcon(direction?: QueryOrder) {
  if (direction === 'asc') {
    return <ArrowDownOutlined />;
  } else if (direction === 'desc') {
    return <ArrowUpOutlined />;
  } else {
    return null;
  }
}

interface PaginationProps {
  page: number;
  perPage?: number;
  total: number;
  onChange: (page: number) => void;
}

interface GetPaginationOptionLabelProps {
  page: number;
  perPage: number;
  total: number;
}

function getPaginationOptionLabel({ page, perPage, total }: GetPaginationOptionLabelProps) {
  const firstItem = (page - 1) * perPage + 1;
  const lastItem = Math.min(total, page * perPage);

  return `${firstItem}...${lastItem}`;
}

function renderValue(value: string | number | null | undefined, fallback?: string) {
  if (value === undefined || Number.isNaN(value)) {
    if (fallback) {
      return <StyledTag>{fallback}</StyledTag>;
    } else {
      return <StyledTag>UNDEFINED</StyledTag>;
    }
  }

  return typeof value === 'string' && value.startsWith('{{') && value.endsWith('}}') ? (
    <StyledTag>{value.replaceAll(/[{}]+/g, '')}</StyledTag>
  ) : fallback ? (
    value || <StyledTag>{fallback}</StyledTag>
  ) : (
    value
  );
}

function Pagination(props: PaginationProps) {
  const { page, perPage = 100, total, onChange } = props;
  const numberOfPages = Math.ceil(total / perPage);

  const onSelectionChange = useCallback(
    (val) => {
      onChange(Number(val as string));
    },
    [onChange]
  );

  return (
    <Space gap=".5x" aria-label="Pagination">
      <Button
        aria-label="Previous page"
        isDisabled={page === 1}
        size="small"
        type="outline"
        icon={<LeftOutlined />}
        onPress={() => onChange(page - 1)}
      />
      <Select
        aria-label="Select page"
        size="small"
        selectedKey={String(page)}
        width="min 15x"
        onSelectionChange={onSelectionChange}
      >
        {[...Array(numberOfPages)].map((a, i) => {
          return (
            <Select.Item key={i + 1} textValue={String(i + 1)}>
              {getPaginationOptionLabel({ page: i + 1, perPage, total })}
            </Select.Item>
          );
        })}
      </Select>
      <Button
        aria-label="Next page"
        isDisabled={page === numberOfPages}
        size="small"
        type="outline"
        icon={<RightOutlined />}
        onPress={() => onChange(page + 1)}
      />
    </Space>
  );
}

interface OptionsButtonProps extends Omit<CubeButtonProps, 'order'> {
  name: string;
  member: 'dimension' | 'measure' | 'timeDimension';
  order: 'none' | 'asc' | 'desc';
  onOrderChange?: (order?: QueryOrder) => void;
  onMemberRemove: (member: string) => void;
  onAddFilter?: (member: string) => void;
  type: 'string' | 'number' | 'time' | 'boolean';
}

function OptionsButton(props: OptionsButtonProps) {
  const { name, member, type, order, onAddFilter, onOrderChange, onMemberRemove, ...otherProps } =
    props;

  const onAction = useCallback(
    (key: Key) => {
      switch (key) {
        case 'none':
        case 'asc':
        case 'desc':
          onOrderChange?.(key === 'none' ? undefined : key);
          break;
        case 'remove':
          onMemberRemove(name);
          break;
        case 'filter':
          onAddFilter?.(name);
          break;
      }
    },
    [onOrderChange, onMemberRemove, onAddFilter, name]
  );

  const disabledKeys = type === 'boolean' ? ['filter'] : [];

  const onMemberRemoveLocal = useCallback(() => onMemberRemove(name), [onMemberRemove, name]);

  if (!onAddFilter && !onOrderChange) {
    return (
      <OptionsButtonElement
        icon={<CloseIcon />}
        data-member={member}
        {...otherProps}
        onPress={onMemberRemoveLocal}
      />
    );
  }

  return (
    <MenuTrigger>
      <OptionsButtonElement data-member={member} {...otherProps} />
      <Menu disabledKeys={disabledKeys} onAction={onAction}>
        {[
          ...(onOrderChange
            ? [
                <Menu.Section key="sorting" title="Sorting">
                  <Menu.Item key="none" icon={<ClearOutlined style={{ fontSize: 16 }} />}>
                    Do not sort
                  </Menu.Item>
                  <Menu.Item
                    key="asc"
                    icon={<ArrowDownOutlined style={{ fontSize: 16 }} />}
                    textValue="Sort ASC"
                  >
                    Sort <Text.Strong>{ORDER_LABEL_BY_TYPE[type]?.[0] || 'ASC'}</Text.Strong>
                  </Menu.Item>
                  <Menu.Item
                    key="desc"
                    icon={<ArrowUpOutlined style={{ fontSize: 16 }} />}
                    textValue="Sort DESC"
                  >
                    Sort <Text.Strong>{ORDER_LABEL_BY_TYPE[type]?.[1] || 'DESC'}</Text.Strong>
                  </Menu.Item>
                </Menu.Section>,
              ]
            : []),
          <Menu.Section key="actions" title="Actions">
            {onAddFilter && (
              <Menu.Item key="filter" icon={<FilterOutlined style={{ fontSize: 16 }} />}>
                Add filter
              </Menu.Item>
            )}
            <Menu.Item
              key="remove"
              icon={<CloseIcon color="#danger-text" />}
              textValue="Remove member"
            >
              <Text color="#danger-text">Remove member</Text>
            </Menu.Item>
          </Menu.Section>,
        ]}
      </Menu>
    </MenuTrigger>
  );
}

const CELL_STYLES: Styles = {
  display: 'grid',
  flow: 'column',
  gridColumns: '1fr auto',
  color: {
    '': '#dark',
    inactive: '#dark.5',
  },
  preset: 't3',
  fill: '#white',
  textOverflow: 'ellipsis',
  overflow: 'hidden',
  whiteSpace: 'nowrap',
  placeSelf: 'stretch',
  width: 'min 140px',
  transition: 'color .2s ease-in-out',
};

const GridTable = tasty(Grid, {
  styles: {
    position: 'relative',
    minWidth: 'min-content',
    fill: {
      '': '#dark-04.8',
      inactive: '#dark-04.4',
    },
    gap: '1bw',
    placeContent: 'stretch',
    border: 'bottom',

    Row: {
      display: 'contents',
    },

    CellValue: {
      textOverflow: 'ellipsis',
      overflow: 'hidden',
      whiteSpace: 'nowrap',
      padding: '1x',
    },

    Cell: {
      ...CELL_STYLES,
      cursor: {
        '': 'pointer',
        inactive: 'default',
      },
    },

    NumberCell: {
      ...CELL_STYLES,
      placeContent: 'end',
      cursor: {
        '': 'pointer',
        inactive: 'default',
      },
    },

    SelectedCell: {
      ...CELL_STYLES,
      shadow: '0 0 0 1bw #purple.5, 0 0 0 (1ow - 1bw) #purple.5 inset',
      zIndex: 1,
    },

    SelectedNumberCell: {
      ...CELL_STYLES,
      placeContent: 'end',
      shadow: '0 0 0 1bw #purple.5, 0 0 0 (1ow - 1bw) #purple.5 inset',
      zIndex: 1,
    },
  },
});

const ColumnHeader = tasty({
  styles: {
    position: 'sticky',
    top: 0,
    display: 'grid',
    gridColumns: 'max-content max-content',
    placeContent: 'center space-between',
    placeItems: 'center',
    color: '#dark',
    preset: 't3m',
    padding: '1x',
    fill: {
      '': '#missing-active',
      '[data-member="measure"]': '#measure-active',
      '[data-member="dimension"]': '#dimension-active',
      '[data-member="timeDimension"]': '#time-dimension-active',
    },
    cursor: {
      '': 'default',
      movable: 'move',
    },
    zIndex: 2,
  },
});

const EMPTY_DATA: any[] = [];

const ReorderableMemberListElement = tasty({
  as: 'ul',
  styles: {
    display: 'contents',
    flow: 'row',
    padding: 0,
    margin: 0,
  },
});

interface ReorderableMemberListProps extends ListProps<MemberItem> {
  onMove: (newKeys: string[]) => void;
}

function ReorderableMemberList(props: ReorderableMemberListProps) {
  let { onMove, ...itemProps } = props;
  let state = useListState(props);
  let ref = useRef(null);
  let { listBoxProps } = useListBox(
    {
      ...itemProps,
      // Prevent dragging from changing selection.
      shouldSelectOnPressUp: true,
    },
    state,
    ref
  );

  const onReorder = (e: DroppableCollectionReorderEvent) => {
    const originalKeys = [...(itemProps.items || [])].map((item) => item.id);
    const { target, keys: movableKeys } = e;
    const { dropPosition, key: targetKey } = target;
    const movableKey = [...movableKeys][0];

    const movableIndex = originalKeys.indexOf(movableKey as string);
    const targetIndex = originalKeys.indexOf(targetKey as string);

    // reorder keys
    const newKeys =
      movableIndex !== targetIndex
        ? originalKeys.reduce((arr, key, i) => {
            // if key is the same as the one we are moving, skip it
            if (i === movableIndex) {
              return arr;
            }

            // if key is the same as the target, add the movable key
            if (i === targetIndex) {
              if (dropPosition === 'before') {
                arr.push(movableKey as string);
                arr.push(key);
              } else if (dropPosition === 'after') {
                arr.push(key);
                arr.push(movableKey as string);
              } else {
                arr.push(key);
              }
            } else {
              arr.push(key);
            }

            return arr;
          }, [] as string[])
        : originalKeys;

    onMove(newKeys);
  };

  // Setup drag state for the collection.
  let dragState = useDraggableCollectionState({
    // Pass through events from props.
    ...itemProps,

    // Collection and selection manager come from list state.
    collection: state.collection,
    selectionManager: state.selectionManager,

    // Provide data for each dragged item. This function could
    // also be provided by the user of the component.
    getItems: (keys: Set<Key>) => {
      return [...keys].map((key: any) => {
        let item = state.collection.getItem(key);

        return {
          'text/plain': item?.textValue || '',
        };
      });
    },
    getAllowedDropOperations: () => ['move'],
  });

  useDraggableCollection(props, dragState, ref);

  let dropState = useDroppableCollectionState({
    ...itemProps,
    onReorder,
    collection: state.collection,
    selectionManager: state.selectionManager,
  });

  let { collectionProps } = useDroppableCollection(
    {
      ...itemProps,
      // Provide drop targets for keyboard and pointer-based drag and drop.
      keyboardDelegate: new ListKeyboardDelegate(state.collection, state.disabledKeys, ref),
      dropTargetDelegate: new ListDropTargetDelegate(state.collection, ref),
      onReorder,
    },
    dropState,
    ref
  );

  return (
    <ReorderableMemberListElement {...mergeProps(listBoxProps, collectionProps)} ref={ref}>
      {[...state.collection].map((item) => (
        <ReorderableMember
          key={item.key}
          item={item}
          state={state}
          dragState={dragState}
          dropState={dropState}
        />
      ))}
    </ReorderableMemberListElement>
  );
}

const ReorderableMemberElement = tasty({
  as: 'div',
  styles: {
    position: 'sticky',
    top: 0,
    display: 'block',
    shadow: {
      '': '0',
      focused: '1bw solid #focus inset',
    },
  },
});

interface ReorderableMemberProps {
  item: AriaOptionProps;
  state: ListState<any>;
  dragState: DraggableCollectionState;
  dropState: DroppableCollectionState;
}

function ReorderableMember({ item, state, dragState, dropState }: ReorderableMemberProps) {
  // Setup listbox option as normal. See useListBox docs for details.
  let ref = useRef(null);
  let { optionProps } = useOption({ key: item.key }, state, ref);
  let { isFocusVisible, focusProps } = useFocusRing();

  // Register the item as a drag source.
  let { dragProps } = useDraggableItem(
    {
      key: item.key,
    },
    dragState
  );

  return (
    <>
      <ReorderableMemberElement
        {...mergeProps(optionProps, dragProps, focusProps)}
        ref={ref}
        mods={{
          focused: isFocusVisible,
        }}
      >
        <DropIndicator
          position="before"
          target={{ type: 'item', key: item.key, dropPosition: 'before' }}
          dropState={dropState}
        />
        {/* @ts-ignore */}
        {item.rendered}
        {state.collection.getKeyAfter(item.key) == null && (
          <DropIndicator
            position="after"
            target={{ type: 'item', key: item.key, dropPosition: 'after' }}
            dropState={dropState}
          />
        )}
      </ReorderableMemberElement>
    </>
  );
}

const DropIndicatorElement = tasty({
  styles: {
    zIndex: 10,
    position: 'absolute',
    pointerEvents: 'none',
    opacity: {
      '': 0,
      dropTarget: 1,
    },
    fill: '#purple',
    width: '.5x',
    top: 0,
    bottom: 0,
    left: {
      '': 'auto',
      before: '-2px',
    },
    right: {
      '': 'auto',
      after: '-2px',
    },
  },
});

interface DropIndicatorProps {
  position: 'before' | 'after';
  target: any;
  dropState: any;
}

function DropIndicator(props: DropIndicatorProps) {
  const { position, target } = props;

  let ref = useRef(null);
  let { dropIndicatorProps, isHidden, isDropTarget } = useDropIndicator(
    { target },
    props.dropState,
    ref
  );
  if (isHidden) {
    return null;
  }

  return (
    <DropIndicatorElement
      ref={ref}
      role="option"
      {...dropIndicatorProps}
      mods={{
        'drop-target': isDropTarget,
        after: position === 'after',
        before: position === 'before',
      }}
    />
  );
}

interface MemberItem {
  id: string;
  rendered: ReactNode;
  textValue: string;
}

export function QueryBuilderResults({ forceMinHeight }: { forceMinHeight?: boolean }) {
  const {
    isLoading,
    isResultOutdated,
    query,
    members,
    measures: measuresUpdater,
    dimensions: dimensionsUpdater,
    filters: filtersUpdater,
    executedQuery,
    resultSet,
    order,
    cubes,
    error,
    usedCubes,
    updateQuery,
    grouping,
    totalRows,
    memberViewType,
    meta,
  } = useQueryBuilderContext();

  const isCompact = usedCubes.length === 1;
  const [selectedCell, setSelectedCell] = useState<[number, string] | null>(null);
  const dataRef = useRef<{ [k: string]: string | number }[] | undefined>(EMPTY_DATA);
  const tableRef = useRef<HTMLDivElement>(null);
  const [page, setPage] = useState(1);

  // @ts-ignore
  if (resultSet?.loadResponse?.results[0].data) {
    // @ts-ignore
    dataRef.current = resultSet?.loadResponse?.results[0].data;
  }

  const queryRelated = executedQuery && areQueriesRelated(query, executedQuery);

  if (executedQuery && !queryRelated) {
    dataRef.current = undefined;
  }

  let data = dataRef.current;

  const measures = query?.measures || [];
  const dimensions = query?.dimensions || [];
  const timeDimensions = query?.timeDimensions?.filter((member) => !!member.granularity) || [];
  const totalColumns = measures.length + dimensions.length + grouping.getAll().length;
  const isColumnsSelected = !!totalColumns;

  // scroll table to the top when page is changed
  useEffect(() => {
    if (tableRef.current) {
      tableRef.current.scrollTop = 0;
    }

    if (selectedCell) {
      setSelectedCell(null);
    }
  }, [page]);

  // reset pagination when data is changed
  useEffect(() => {
    setPage(1);
  }, [dataRef.current]);

  useEffect(() => {
    setSelectedCell(null);
  }, [isLoading]);

  if (!data) {
    data = EMPTY_DATA;
  }

  function formatCellData(
    dimensionName: string,
    value?: string | number,
    granularity?: TimeDimensionGranularity
  ) {
    if (value === null) {
      return ['{{NULL}}', 'unknown'];
    }

    if (value === undefined) {
      return ['–', 'unknown'];
    }

    const cubeName = dimensionName.split('.')[0];
    const cube = cubes.find((cube) => cube.name === cubeName);

    if (!cube) {
      return [value, 'unknown'];
    }

    let member = [...cube.dimensions, ...cube.measures].find(
      (member) => member.name === dimensionName
    );

    if (!member) {
      return [value, 'unknown'];
    }

    const type = typeof value !== 'string' ? typeof value : member.type;

    switch (type) {
      case 'number':
        // @ts-ignore
        switch (member.format) {
          case 'currency':
            return [
              formatCurrency(typeof value === 'string' ? parseFloat(value) : value),
              'number',
            ];
          case 'percent':
            return [
              `${formatNumber(typeof value === 'string' ? parseFloat(value) : value)}%`,
              'percent',
            ];
          default:
            return [formatNumber(typeof value === 'string' ? parseFloat(value) : value), 'number'];
        }
      case 'time':
        try {
          if (granularity) {
            return [formatDateByGranularity(new Date(value), granularity), 'time'];
          } else {
            return [formatDateByGranularity(new Date(value), 'second'), 'time'];
          }
        } catch (e: any) {
          return [value, 'unknown'];
        }
      case 'boolean':
        return [value && value !== '0' && value !== 'false' ? '{{TRUE}}' : '{{FALSE}}', 'boolean'];
      default:
        return [String(value) || '{{EMPTY STRING}}', 'string'];
    }
  }

  const onTableClick = useCallback(
    (e) => {
      if (isLoading) {
        return;
      }

      const { row, name } = e.nativeEvent.target.parentNode.dataset;

      if (row != null && name != null) {
        const newSelectedCell = [Number(row), name];

        if (JSON.stringify(newSelectedCell) !== JSON.stringify(selectedCell)) {
          setSelectedCell([Number(row), name]);
        } else {
          setSelectedCell(null);
        }
      } else {
        setSelectedCell(null);
      }
    },
    [selectedCell, isLoading]
  );

  const tableData = useMemo(() => {
    return (
      <>
        {data?.slice((page - 1) * 100, (page - 1) * 100 + 100).map((row, rowId) => {
          return (
            <div key={rowId} data-element="Row" data-qa={`QueryBuilderResult-row_${rowId}`}>
              {dimensions.map((dimension) => {
                const isSelected =
                  selectedCell && selectedCell[0] === rowId && selectedCell[1] === dimension;
                const [value, type] = formatCellData(dimension, row[dimension]);
                const copyButton =
                  isSelected &&
                  value !== '–' &&
                  (typeof value !== 'string' || !value.startsWith('{{')) ? (
                    <StyledCopyButton value={String(value)} />
                  ) : null;

                const renderedValue = renderValue(value);

                return (
                  <div
                    key={dimension}
                    data-row={rowId}
                    data-name={dimension}
                    data-element={`${isSelected ? 'Selected' : ''}${
                      type === 'number' || type === 'percent' ? 'Number' : ''
                    }Cell`}
                  >
                    <div data-element="CellValue">{renderedValue}</div>
                    {copyButton}
                  </div>
                );
              })}
              {timeDimensions.map((timeDimension) => {
                const isSelected =
                  selectedCell &&
                  selectedCell[0] === rowId &&
                  selectedCell[1] === timeDimension.dimension;
                const rawValue = row[timeDimension.dimension + '.' + timeDimension.granularity];
                let value = rawValue ? String(rawValue) : undefined;

                try {
                  value =
                    value != null
                      ? formatDateByGranularity(new Date(value), timeDimension.granularity)
                      : '–';
                } catch (e: any) {}

                const copyButton =
                  isSelected && value !== '–' ? <StyledCopyButton value={value} /> : null;

                const renderedValue = renderValue(value, 'NULL');

                return (
                  <div
                    key={`time-dimension.${timeDimension.dimension}`}
                    data-row={rowId}
                    data-name={timeDimension.dimension}
                    data-element={`${isSelected ? 'Selected' : ''}Cell`}
                  >
                    <div data-element="CellValue">{renderedValue}</div>
                    {copyButton}
                  </div>
                );
              })}
              {measures.map((measure) => {
                const isSelected =
                  selectedCell && selectedCell[0] === rowId && selectedCell[1] === measure;
                const [value, type] = formatCellData(measure, row[measure]);
                const copyButton =
                  isSelected && value !== '–' ? <StyledCopyButton value={String(value)} /> : null;
                const renderedValue = renderValue(value);

                return (
                  <div
                    key={measure}
                    data-row={rowId}
                    data-name={measure}
                    data-element={`${isSelected ? 'Selected' : ''}${
                      type === 'number' || type === 'percent' ? 'Number' : ''
                    }Cell`}
                  >
                    <div data-element="CellValue">{renderedValue}</div>
                    {copyButton}
                  </div>
                );
              })}
            </div>
          );
        })}
      </>
    );
  }, [
    isLoading,
    JSON.stringify(measures),
    JSON.stringify(dimensions),
    JSON.stringify(timeDimensions),
    page,
    selectedCell,
    data,
    meta,
  ]);

  function addFilter(name: string) {
    filtersUpdater.add({ member: name, operator: 'set' });
  }

  const dimensionColumns = useDeepMemo(() => {
    if (!dimensions.length) {
      return null;
    }

    const items = dimensions.map((dimension) => {
      const member = members.dimensions[dimension];
      const cubeName = dimension.split('.')[0];
      const cube = cubes.find((cube) => cube.name === cubeName);

      return {
        id: dimension,
        textValue: dimension,
        rendered: (
          <ColumnHeader
            key={dimension}
            data-member={member ? 'dimension' : undefined}
            mods={{ movable: dimensions.length > 1 }}
          >
            <MemberLabel
              isMissing={!member}
              name={dimension}
              memberName={member?.name}
              cubeName={cube?.name}
              memberTitle={member?.shortTitle}
              cubeTitle={cube?.title}
              isCompact={isCompact}
              memberViewType={memberViewType}
              memberType="dimension"
              type={member?.type}
            >
              {getOrderIcon(order.get(dimension))}
            </MemberLabel>

            <OptionsButton
              name={dimension}
              member="dimension"
              order={order.get(dimension)}
              type={members.dimensions[dimension]?.type || 'string'}
              onAddFilter={member ? addFilter : undefined}
              onOrderChange={
                member
                  ? (ord?: QueryOrder) => {
                      if (ord) {
                        order.set(dimension, ord);
                      } else {
                        order.remove(dimension);
                      }
                    }
                  : undefined
              }
              onMemberRemove={(name) => dimensionsUpdater?.remove(name)}
            />
          </ColumnHeader>
        ),
      };
    });

    if (dimensions.length === 1) {
      return items[0].rendered;
    }

    return (
      <ReorderableMemberList
        aria-label="Dimensions"
        selectionBehavior="replace"
        items={items}
        onMove={(newKeys) => {
          updateQuery({
            dimensions: newKeys,
          });
        }}
      >
        {(item) => <Item textValue={item.textValue}>{item.rendered}</Item>}
      </ReorderableMemberList>
    );
  }, [dimensions, JSON.stringify(query.order), meta, memberViewType, isCompact]);

  const measuresColumns = useDeepMemo(() => {
    if (!measures.length) {
      return null;
    }

    const items = measures.map((measure) => {
      const member = members.measures[measure];
      const cubeName = measure.split('.')[0];
      const cube = cubes.find((cube) => cube.name === cubeName);

      return {
        id: measure,
        textValue: measure,
        rendered: (
          <ColumnHeader
            key={measure}
            data-member={member ? 'measure' : undefined}
            mods={{ movable: measures.length > 1 }}
          >
            <MemberLabel
              isMissing={!member}
              name={measure}
              memberName={member?.name}
              cubeName={cube?.name}
              memberTitle={member?.shortTitle}
              cubeTitle={cube?.title}
              isCompact={isCompact}
              memberViewType={memberViewType}
              memberType="measure"
              type={member?.type ?? 'number'}
            >
              {getOrderIcon(order.get(measure))}
            </MemberLabel>

            <OptionsButton
              name={measure}
              member="measure"
              order={order.get(measure)}
              type={members.measures[measure]?.type || 'string'}
              onAddFilter={member ? addFilter : undefined}
              onOrderChange={
                member
                  ? (ord?: QueryOrder) => {
                      if (ord) {
                        order.set(measure, ord);
                      } else {
                        order.remove(measure);
                      }
                    }
                  : undefined
              }
              onMemberRemove={(name) => measuresUpdater?.remove(name)}
            />
          </ColumnHeader>
        ),
      };
    });

    if (measures.length === 1) {
      return items[0].rendered;
    }

    return (
      <ReorderableMemberList
        aria-label="Measures"
        selectionBehavior="replace"
        items={items}
        onMove={(newKeys) => {
          updateQuery({
            measures: newKeys,
          });
        }}
      >
        {(item) => <Item textValue={item.textValue}>{item.rendered}</Item>}
      </ReorderableMemberList>
    );
  }, [measures, JSON.stringify(query.order), meta, memberViewType, isCompact]);

  const timeDimensionsColumns = useDeepMemo(() => {
    if (!timeDimensions.length) {
      return null;
    }

    const items = timeDimensions.map((timeDimension) => {
      const member = members.dimensions[timeDimension.dimension];
      const ordering = order.get(timeDimension.dimension);
      const availableGranularities = [
        ...((member && 'granularities' in member && member?.granularities?.map((g) => g.name)) ||
          []),
        ...PREDEFINED_GRANULARITIES,
      ];
      const cubeName = timeDimension.dimension.split('.')[0];
      const cube = cubes.find((cube) => cube.name === cubeName);
      const granularity =
        timeDimension.granularity &&
        member &&
        'granularities' in member &&
        member?.granularities?.find((g) => g.name === timeDimension.granularity);

      return {
        id: timeDimension.dimension,
        textValue: timeDimension.dimension,
        rendered: (
          <ColumnHeader
            key={`time-dimension.${timeDimension.dimension}`}
            data-member={member ? 'timeDimension' : undefined}
            mods={{ movable: timeDimensions.length > 1 }}
          >
            <MemberLabel
              isMissing={!member}
              name={timeDimension.dimension}
              memberName={member?.name}
              cubeName={cube?.name}
              memberTitle={member?.shortTitle}
              cubeTitle={cube?.title}
              isCompact={isCompact}
              memberViewType={memberViewType}
              memberType="timeDimension"
              type={member?.type ?? 'time'}
            >
              {granularity ? (
                <MemberBadge
                  isSpecial
                  isMissing={
                    !availableGranularities.includes(timeDimension.granularity as any) || !member
                  }
                  type="timeDimension"
                >
                  {memberViewType === 'title'
                    ? granularity.title
                    : (timeDimension.granularity ?? timeDimension.granularity)}
                </MemberBadge>
              ) : undefined}
              {getOrderIcon(ordering)}
            </MemberLabel>

            <OptionsButton
              name={timeDimension.dimension}
              member="timeDimension"
              order={ordering}
              type="time"
              onAddFilter={member ? addFilter : undefined}
              onOrderChange={
                member
                  ? (ord?: QueryOrder) => {
                      if (ord) {
                        order.set(timeDimension.dimension, ord);
                      } else {
                        order.remove(timeDimension.dimension);
                      }
                    }
                  : undefined
              }
              onMemberRemove={(name) => grouping.remove(name)}
            />
          </ColumnHeader>
        ),
      };
    });

    if (timeDimensions.length === 1) {
      return items[0].rendered;
    }

    return (
      <ReorderableMemberList
        aria-label="Time dimensions"
        selectionBehavior="replace"
        items={items}
        onMove={(newKeys) => {
          grouping.reorder(newKeys);
        }}
      >
        {(item) => <Item textValue={item.textValue}>{item.rendered}</Item>}
      </ReorderableMemberList>
    );
  }, [timeDimensions, JSON.stringify(query.order), meta, memberViewType, isCompact]);

  const timestamp = useMemo(() => {
    return new Date();
  }, [tableData]);

  const [timeDistance, setTimeDistance] = useState(
    formatDistance(timestamp, new Date(), { addSuffix: true })
  );

  useIntervalEffect(() => {
    setTimeDistance(formatDistance(timestamp, new Date(), { addSuffix: true }));
  }, 60 * 1000);

  const noResultsDisclaimer = (
    <DisclaimerContainer>
      <Title level={4} gridArea={false}>
        No results available
      </Title>
      <Paragraph>Compose and run a query to see the results.</Paragraph>
    </DisclaimerContainer>
  );

  return (
    <Panel
      qa="QueryBuilderResults"
      flow="column"
      gridRows="minmax(0, 1fr) min-content"
      overflow="clip"
      height={forceMinHeight ? 'min 31x' : 'initial'}
    >
      {isColumnsSelected ? (
        <Panel gridRows="minmax(0, 1fr)">
          <TableContainer ref={tableRef} onClick={onTableClick} onTouchStart={onTableClick}>
            <GridTable
              columns={`repeat(${totalColumns}, auto)`}
              mods={{ inactive: !!(isLoading || error) }}
            >
              {dimensionColumns}
              {timeDimensionsColumns}
              {measuresColumns}
              {tableData}
            </GridTable>
            {!executedQuery ? noResultsDisclaimer : null}
          </TableContainer>
        </Panel>
      ) : (
        noResultsDisclaimer
      )}

      <TableFooter>
        <Space>
          {isLoading ? <LoadingOutlined /> : isResultOutdated ? <OutdatedLabel /> : undefined}
          {executedQuery && !isLoading && isColumnsSelected && queryRelated && (
            <Space gap=".75x">
              <Text preset="t3m">
                {data.length
                  ? `${data.length} result${data.length > 1 ? 's' : ''}${
                      totalRows
                        ? totalRows === data.length
                          ? ' in total'
                          : ` out of ${totalRows} in total`
                        : ''
                    }`
                  : 'No results'}
              </Text>
              <Text preset="t3">received {timeDistance}</Text>
            </Space>
          )}
        </Space>
        <Space>
          {data.length > 100 ? (
            <Pagination page={page} total={data.length} onChange={setPage} />
          ) : null}
        </Space>
      </TableFooter>
    </Panel>
  );
}
