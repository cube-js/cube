import { DragOutlined } from '@ant-design/icons';
import {
  Button,
  ComboBox,
  Content,
  Dialog,
  DialogTrigger,
  Flow,
  Grid,
  Radio,
  Select,
  Space,
  Tag,
  tasty,
  Text,
} from '@cube-dev/ui-kit';
import { forwardRef, Key, useEffect, useMemo, useState } from 'react';
import {
  DragDropContext,
  Draggable,
  Droppable,
  OnDragEndResponder,
} from 'react-beautiful-dnd';
import { TCubeMemberType } from '@cubejs-client/core';

import { useStoredTimezones, useEvent } from './hooks';
import { MemberLabel } from './components/MemberLabel';
import { useQueryBuilderContext } from './context';
import { ArrowIcon } from './icons/ArrowIcon';
import { ORDER_LABEL_BY_TYPE } from './utils/labels';
import { formatNumber } from './utils/formatters';
import { TIMEZONES } from './utils/timezones';

const allTimeZones: {
  tzCode: string;
  label: string;
  name: string;
  utc: string;
}[] = [
  {
    tzCode: '',
    label: 'UTC (Default)',
    name: 'Coordinated Universal Time',
    utc: '+00:00',
  },
  ...TIMEZONES,
];
const availableTimeZones = allTimeZones.map((tz) => tz.tzCode);

const limitOptions = [
  { key: 100, label: '100' },
  { key: 1000, label: '1,000' },
  { key: 5000, label: '5,000' },
  { key: 50000, label: '50,000 (MAX)' },
];
const limitOptionValues = limitOptions.map((option) => option.key);

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

export const OrderListItem = forwardRef(function OrderListItem(
  props: OrderListItemProps,
  ref
) {
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
    <OrderListItemStyled
      ref={ref}
      key={name}
      data-member={memberType}
      {...otherProps}
    >
      <DragOutlined style={{ fontSize: 16 }} />

      <MemberLabel name={label} member={memberType} />

      <Radio.ButtonGroup
        aria-label="Sorting"
        defaultValue={defaultSorting}
        orientation="horizontal"
        onChange={(val) => onSortChange(name, val as SortDirection)}
      >
        <SortButton data-member={memberType} aria-label="Ascending" value="asc">
          {ORDER_LABEL_BY_TYPE[cubeMemberKind ?? 'string'][0]}
        </SortButton>

        <SortButton
          data-member={memberType}
          aria-label="Descending"
          value="desc"
        >
          {ORDER_LABEL_BY_TYPE[cubeMemberKind ?? 'string'][1]}
        </SortButton>

        <SortButton
          data-member={memberType}
          aria-label="No sorting"
          value="none"
        >
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
    query?.timeDimensions
      ?.filter((time) => time.granularity)
      .map((time) => time.dimension) ?? [];

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
          rightIcon={<ArrowIcon direction="bottom" />}
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
          <Content
            padding="(1.5x - 1ow)"
            style={{ minHeight: `${30 * allFields.length + 18}px` }}
          >
            <DragDropContext onDragEnd={onDrag}>
              <Droppable droppableId="queryOrder">
                {(provided) => (
                  <OrderListContainer
                    ref={provided.innerRef}
                    {...provided.droppableProps}
                  >
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

  const limitSelector = useMemo(() => {
    const limit = query.limit || 5_000;
    const options = limitOptionValues.includes(limit)
      ? limitOptions
      : [
          { key: query?.limit, label: formatNumber(limit) },
          ...limitOptions,
        ].sort((a, b) => (a.key as number) - (b.key as number));

    return (
      <Select
        label="Limit"
        labelPosition="side"
        size="small"
        selectedKey={String(query.limit)}
        onSelectionChange={(val: Key) => {
          updateQuery(() => ({ limit: Number(val as string) }));
        }}
      >
        {options.map((option) => (
          <Select.Item key={option.key} textValue={option.label}>
            {option.label}
          </Select.Item>
        ))}
      </Select>
    );
  }, [query.limit]);

  const timezoneSelector = useMemo(() => {
    const timezone = query?.timezone || '';
    const optionsWithStored = [...allTimeZones];

    [...storedTimezones].reverse().forEach((name) => {
      if (!availableTimeZones.includes(name)) {
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
      <ComboBox
        aria-label="Timezone"
        size="small"
        width="25x"
        listBoxStyles={{ height: '41x' }}
        selectedKey={timezone}
        onSelectionChange={(val: Key) => {
          const timezone = val as string;

          updateQuery(() => ({
            timezone: timezone === '' ? undefined : timezone,
          }));
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
    );
  }, [query?.timezone, storedTimezones.join('::')]);

  return (
    <Space placeContent="space-between">
      {timezoneSelector}
      <Space>
        {orderSelector}
        {limitSelector}
      </Space>
    </Space>
  );
}
