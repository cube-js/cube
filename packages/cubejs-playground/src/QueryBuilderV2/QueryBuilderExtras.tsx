import { DragOutlined } from '@ant-design/icons';
import {
  Button,
  Checkbox,
  ComboBox,
  Content,
  Dialog,
  DialogTrigger,
  DownIcon,
  Flow,
  Grid,
  InfoCircleIcon,
  Link,
  NumberInput,
  Radio,
  Select,
  Space,
  Tag,
  tasty,
  Text,
  Title,
  TooltipProvider,
} from '@cube-dev/ui-kit';
import { forwardRef, Key, useEffect, useMemo, useState } from 'react';
import { DragDropContext, Draggable, Droppable, OnDragEndResponder } from 'react-beautiful-dnd';
import { TCubeMemberType } from '@cubejs-client/core';

import { useStoredTimezones, useEvent } from './hooks';
import { MemberLabel } from './components/MemberLabel';
import { InfoIconButton } from './components/InfoIconButton';
import { useQueryBuilderContext } from './context';
import { ORDER_LABEL_BY_TYPE } from './utils/labels';
import { formatNumber } from './utils/formatters';
import { TIMEZONES } from './utils/timezones';

const DEFAULT_LIMIT = 5_000;

const ALL_TIMEZONES: {
  tzCode: string;
  label: string;
  name: string;
  utc: string;
}[] = [
  {
    tzCode: '',
    label: 'UTC (default)',
    name: 'Coordinated Universal Time',
    utc: '+00:00',
  },
  ...TIMEZONES,
];
const AVAILABLE_TIMEZONES = ALL_TIMEZONES.map((tz) => tz.tzCode);

const LIMIT_OPTIONS: { key: number; label: string }[] = [
  { key: 100, label: '100' },
  { key: 1000, label: '1,000' },
  { key: 5000, label: '5,000' },
  { key: 0, label: 'Default limit' },
];
const LIMIT_OPTION_VALUES = LIMIT_OPTIONS.map((option) => option.key) as number[];

function timezoneByName(name: string) {
  return {
    tzCode: name,
    label: name,
    name: name,
    utc: '',
  };
}

const SortButton = tasty(Radio.Button, {
  inputStyles: {
    display: 'grid',
    placeContent: 'center',
    preset: 't4m',
    height: '3x',
    padding: '.5x .75x',
    fill: {
      '': '#white',
      // hovered: '#clear',
      checked: '#active',
      disabled: '#dark.04',
    },
    color: {
      '': '#dark-02',
      hovered: '#active',
      checked: '#white',
    },
    border: {
      '': '#hover',
      checked: '#active',
    },
    backgroundClip: 'padding-box',
  },
});

const OrderListContainer = tasty(Flow, {
  qa: 'OrderList',
  styles: {
    flow: 'column',
    gap: '1ow',
  },
});

const OrderListItemStyled = tasty(Grid, {
  qa: 'OrderItem',
  styles: {
    gridTemplateColumns: 'auto 1fr auto',
    flow: 'row',
    gap: '1x',
    fill: {
      '': '#fill',
      ':hover': '#hover',
    },
    radius: true,
    padding: '1ow 1ow 1ow .75x',
    placeContent: 'start',
    placeItems: 'center start',
    transition: 'theme',

    '--active-color': {
      '': '#dark',
      '[data-member="measure"]': '#measure-text',
      '[data-member="dimension"]': '#dimension-text',
      '[data-member="timeDimension"]': '#time-dimension-text',
      '[data-member="segment"]': '#segment-text',
    },

    '--fill-color': {
      '': '#dark-05',
      '[data-member="measure"]': '#measure-hover',
      '[data-member="dimension"]': '#dimension-hover',
      '[data-member="timeDimension"]': '#time-dimension-hover',
      '[data-member="segment"]': '#segment-hover',
    },

    '--hover-color': {
      '': '#dark',
      '[data-member="measure"]': '#measure-active',
      '[data-member="dimension"]': '#dimension-active',
      '[data-member="timeDimension"]': '#time-dimension-active',
      '[data-member="segment"]': '#segment-active',
    },
  },
});

export function sortFields(fields: string[], sortedFields: string[]) {
  return [...fields].sort((a, b) => {
    if (sortedFields.includes(a) && sortedFields.includes(b)) {
      return sortedFields.indexOf(a) - sortedFields.indexOf(b);
    }

    if (sortedFields.includes(a)) {
      return -1;
    }

    if (sortedFields.includes(b)) {
      return 1;
    }

    return 0;
  });
}

export type SortDirection = 'asc' | 'desc' | 'none';

export type CubeMember = 'measure' | 'dimension' | 'timeDimension';

type OrderListItemProps = {
  name: string;
  label?: string;
  memberType: CubeMember | undefined;
  cubeMemberKind: TCubeMemberType | undefined;
  defaultSorting?: SortDirection;
  onSortChange: (name: string, sorting: SortDirection) => void;
};

export const OrderListItem = forwardRef(function OrderListItem(props: OrderListItemProps, ref) {
  const {
    name,
    memberType,
    cubeMemberKind,
    defaultSorting = 'none',
    onSortChange,
    ...otherProps
  } = props;

  const label = props.label ?? name;

  return (
    <OrderListItemStyled ref={ref} key={name} data-member={memberType} {...otherProps}>
      <DragOutlined style={{ fontSize: 16 }} />

      <MemberLabel name={label} memberType={memberType} />

      <Radio.ButtonGroup
        aria-label="Sorting"
        defaultValue={defaultSorting}
        orientation="horizontal"
        onChange={(val) => onSortChange(name, val as SortDirection)}
      >
        <SortButton data-member={memberType} aria-label="Ascending" value="asc">
          {ORDER_LABEL_BY_TYPE[cubeMemberKind ?? 'string'][0]}
        </SortButton>

        <SortButton data-member={memberType} aria-label="Descending" value="desc">
          {ORDER_LABEL_BY_TYPE[cubeMemberKind ?? 'string'][1]}
        </SortButton>

        <SortButton data-member={memberType} aria-label="No sorting" value="none">
          None
        </SortButton>
      </Radio.ButtonGroup>
    </OrderListItemStyled>
  );
});

export function QueryBuilderExtras() {
  const { query, members, updateQuery, order } = useQueryBuilderContext();
  const [showOrder, setShowOrder] = useState(true);
  const fields = [...(query?.dimensions ?? []), ...(query?.measures ?? [])];
  const storedTimezones = useStoredTimezones(query.timezone);
  const timeDimensions =
    query?.timeDimensions?.filter((time) => time.granularity).map((time) => time.dimension) ?? [];

  timeDimensions.forEach((name) => {
    if (name && !fields.includes(name)) {
      fields.push(name);
    }
  });

  const sortedFields = order.getOrder();
  const [allFields, setAllFields] = useState(sortFields(fields, sortedFields));

  fields.forEach((name) => {
    if (!allFields.includes(name)) {
      allFields.push(name);
    }
  });

  const copyFields = [...allFields];

  allFields.splice(0);

  allFields.push(...copyFields.filter((name) => fields.includes(name)));

  useEffect(() => {
    const filtered = allFields.filter((name) => sortedFields.includes(name));

    let newAllFields = allFields;

    if (filtered.join(',') !== sortedFields.join(',')) {
      newAllFields = sortFields(allFields, sortedFields);

      setAllFields(newAllFields);
    }

    // remove all deleted fields from sorted
    sortedFields.forEach((name) => {
      if (!newAllFields.includes(name)) {
        order.remove(name);
      }
    });

    setShowOrder(false);
  }, [allFields.concat(sortedFields).join(',')]);

  useEffect(() => {
    if (!showOrder) {
      setShowOrder(true);
    }
  }, [showOrder]);

  function getMemberType(name: string) {
    if (timeDimensions.includes(name)) {
      return 'timeDimension';
    }

    if (query?.dimensions?.includes(name)) {
      return 'dimension';
    }

    if (query?.measures?.includes(name)) {
      return 'measure';
    }

    return undefined;
  }

  function getMember(name: string) {
    return members?.measures[name] || members?.dimensions[name];
  }

  function onSortChange(name: string, sorting: 'asc' | 'desc' | 'none') {
    if (sorting === 'none') {
      order.remove(name);
    } else {
      order.set(name, sorting);
      order.setOrder(allFields);
    }
  }

  const onDrag: OnDragEndResponder = useEvent(({ source, destination }) => {
    if (!destination) {
      return;
    }

    const newOrder = [...allFields];

    newOrder.splice(source.index, 1);
    newOrder.splice(destination.index, 0, allFields[source.index]);

    order.setOrder(newOrder);
    setAllFields(newOrder);
  });

  const optionsPopover = useMemo(() => {
    // ungrouped
    const isSelected =
      query.ungrouped ||
      query.total ||
      query.timezone ||
      query.offset ||
      (query.limit && query.limit !== DEFAULT_LIMIT);
    const selectedCount =
      (query.ungrouped ? 1 : 0) +
      (query.total ? 1 : 0) +
      (query.timezone ? 1 : 0) +
      (query.limit && query.limit !== DEFAULT_LIMIT ? 1 : 0) +
      (query.offset ? 1 : 0);

    // timezone
    const timezone = query?.timezone || '';
    const optionsWithStored = [...ALL_TIMEZONES];

    [...storedTimezones].reverse().forEach((name) => {
      if (!AVAILABLE_TIMEZONES.includes(name)) {
        optionsWithStored.unshift(timezoneByName(name));
      } else {
        const option = optionsWithStored.find((tz) => tz.tzCode === name);

        if (option) {
          optionsWithStored.splice(optionsWithStored.indexOf(option), 1);
          optionsWithStored.unshift(option);
        }
      }
    });

    const options = optionsWithStored.map((tz) => tz.tzCode).includes(timezone)
      ? optionsWithStored
      : [timezoneByName(timezone), ...optionsWithStored];

    return (
      <DialogTrigger type="popover" placement="bottom end">
        <Button
          qa="QueryOptions"
          aria-label="Query options"
          type={isSelected ? 'primary' : 'secondary'}
          size="small"
          rightIcon={<DownIcon />}
        >
          Options
          {selectedCount ? (
            <Tag color="#purple-text" fill="#white" border={false}>
              {selectedCount}
            </Tag>
          ) : null}
        </Button>
        {(close) => (
          <Dialog width="36x">
            <Content padding="1x 1.5x" gap="1.5x">
              <Flow gap="1x">
                <Space gap=".25x">
                  <Title level={4} preset="h6">
                    Query
                  </Title>
                  <InfoIconButton
                    tooltip="Click to learn more about the query format"
                    tooltipSuffix=""
                    to="!https://cube.dev/docs/product/apis-integrations/rest-api/query-format#query-properties"
                  />
                </Space>
                <Checkbox
                  aria-label="Ungrouped"
                  isSelected={query.ungrouped ?? false}
                  onChange={(ungrouped) => {
                    updateQuery({ ungrouped: ungrouped || undefined });
                    close();
                  }}
                >
                  Ungrouped
                </Checkbox>
                <Checkbox
                  aria-label="Show total number of rows"
                  isSelected={query.total ?? false}
                  onChange={(total) => {
                    updateQuery({ total: total || undefined });
                    close();
                  }}
                >
                  Show total number of rows
                </Checkbox>
              </Flow>
              <ComboBox
                aria-label="Timezone"
                label="Time zone"
                size="small"
                listBoxStyles={{ height: '41x' }}
                extra={
                  timezone ? (
                    <Link
                      onPress={() => {
                        updateQuery({ timezone: undefined });
                        close();
                      }}
                    >
                      Reset
                    </Link>
                  ) : null
                }
                selectedKey={timezone}
                onSelectionChange={(val: Key | null) => {
                  if (!val) {
                    return;
                  }

                  const timezone = val as string;

                  updateQuery(() => ({
                    timezone: timezone === '' ? undefined : timezone,
                  }));

                  close();
                }}
              >
                {options.map((tz) => {
                  const name = tz.tzCode;
                  const zone = tz.utc;

                  return (
                    <Select.Item key={tz.tzCode} textValue={tz.label}>
                      <Space placeContent="space-between" preset="t3m">
                        <Text nowrap ellipsis block styles={{ width: 'max 40x' }}>
                          {name || 'UTC (Default)'}
                        </Text>
                        {zone ? (
                          <Text nowrap font="monospace" preset="c2">
                            GMT{zone}
                          </Text>
                        ) : undefined}
                      </Space>
                    </Select.Item>
                  );
                })}
              </ComboBox>

              <QueryBuilderLimitSelect />

              <NumberInput
                label="Offset"
                size="small"
                wrapperStyles={{ width: 'auto' }}
                extra={
                  query?.offset ? (
                    <Link
                      onPress={() => {
                        updateQuery({ offset: undefined });
                        close();
                      }}
                    >
                      Reset
                    </Link>
                  ) : null
                }
                minValue={0}
                value={query?.offset ?? 0}
                onChange={(val) => {
                  updateQuery({ offset: val });
                }}
                onKeyDown={(e) => {
                  // close on Enter
                  if (e.key === 'Enter') {
                    close();
                    e.preventDefault();
                  }
                }}
              />
            </Content>
          </Dialog>
        )}
      </DialogTrigger>
    );
  }, [
    query.ungrouped,
    query.timezone,
    query.offset,
    query.total,
    storedTimezones.join('::'),
    query.limit,
  ]);

  const orderSelector = useMemo(() => {
    if (!allFields.length) {
      return;
    }

    return (
      <DialogTrigger type="popover" placement="bottom end">
        <Button
          qa="OrderButton"
          type={sortedFields.length ? 'primary' : 'secondary'}
          size="small"
          rightIcon={<DownIcon />}
        >
          {sortedFields.length ? (
            <>
              Order
              <Tag color="#purple-text" fill="#white" border={false}>
                {sortedFields.length}
              </Tag>
            </>
          ) : (
            'Order'
          )}
        </Button>
        <Dialog width="max 80x">
          <Content padding="(1.5x - 1ow)" style={{ minHeight: `${30 * allFields.length + 18}px` }}>
            <DragDropContext onDragEnd={onDrag}>
              <Droppable droppableId="queryOrder">
                {(provided) => (
                  <OrderListContainer ref={provided.innerRef} {...provided.droppableProps}>
                    {allFields.map((name, index) => {
                      const memberType = getMemberType(name);

                      return (
                        <Draggable key={name} draggableId={name} index={index}>
                          {({ draggableProps, dragHandleProps, innerRef }) => (
                            <OrderListItem
                              ref={innerRef}
                              key={name}
                              name={name}
                              data-member={memberType}
                              memberType={memberType}
                              defaultSorting={order.get(name) ?? 'none'}
                              cubeMemberKind={getMember(name)?.type}
                              onSortChange={onSortChange}
                              {...draggableProps}
                              {...dragHandleProps}
                            />
                          )}
                        </Draggable>
                      );
                    })}

                    {provided.placeholder}
                  </OrderListContainer>
                )}
              </Droppable>
            </DragDropContext>
          </Content>
        </Dialog>
      </DialogTrigger>
    );
  }, [JSON.stringify(order.map), JSON.stringify(allFields), showOrder]);

  return (
    <Space gap="1x">
      {orderSelector}
      {optionsPopover}
    </Space>
  );
}

export function QueryBuilderLimitSelect() {
  const { query, updateQuery } = useQueryBuilderContext();

  const limit = query.limit ?? DEFAULT_LIMIT;
  const limitOptions = LIMIT_OPTION_VALUES.includes(limit)
    ? LIMIT_OPTIONS
    : [{ key: limit, label: formatNumber(limit) }, ...LIMIT_OPTIONS].sort((a, b) => a.key - b.key);

  return (
    <Select
      label="Limit"
      size="small"
      extra={
        limit !== DEFAULT_LIMIT ? (
          <Link
            onPress={() => {
              updateQuery({ limit: undefined });
              close();
            }}
          >
            Reset
          </Link>
        ) : null
      }
      labelSuffix={
        <InfoIconButton
          tooltip="Click to learn more about the row limit"
          tooltipSuffix=""
          to="!https://cube.dev/docs/product/apis-integrations/queries#row-limit"
        />
      }
      selectedKey={query.limit == null ? '0' : String(query.limit)}
      onSelectionChange={(val: Key) => {
        updateQuery(() => ({ limit: val === '0' ? undefined : Number(val as string) }));
        close();
      }}
    >
      {limitOptions.map((option) => (
        <Select.Item key={option.key} textValue={option.label}>
          {option.label}
        </Select.Item>
      ))}
    </Select>
  );
}
