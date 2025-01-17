import { Key, useCallback, useState } from 'react';
import { Item, Select, Space, Text, TooltipProvider } from '@cube-dev/ui-kit';
import { DateRange, TimeDimension } from '@cubejs-client/core';
import formatDate from 'date-fns/format';

import { capitalize } from '../utils/capitalize';
import { DATA_RANGES } from '../values';

import { FilterLabel } from './FilterLabel';
import { TimeDateRangeSelector } from './TimeDateRangeSelector';
import { DeleteFilterButton } from './DeleteFilterButton';

interface TimeDimensionFilterProps {
  member: TimeDimension;
  isCompact?: boolean;
  isMissing?: boolean;
  onChange: (dateRange?: DateRange) => void;
  onRemove: () => void;
}

export function DateRangeFilter(props: TimeDimensionFilterProps) {
  const { member, isCompact, isMissing, onRemove, onChange } = props;
  const [open, setOpen] = useState(false);

  // const onGranularityChange = useCallback(
  //   (granularity?: Key) => {
  //     if (granularity === 'w/o grouping') {
  //       onChange({ ...member, granularity: undefined });
  //     } else {
  //       onChange({ ...member, granularity: granularity as TimeDimensionGranularity });
  //     }
  //   },
  //   [onChange]
  // );

  const onDateRangeChange = useCallback(
    (dateRange?: Key) => {
      if (dateRange === 'custom') {
        onChange([formatDate(new Date(), 'yyyy-MM-dd'), formatDate(new Date(), 'yyyy-MM-dd')]);

        return;
      }

      if (dateRange === 'all time') {
        onChange(undefined);
      } else {
        onChange(dateRange as string);
      }
    },
    [onChange]
  );

  const onDataRangeChangeInPicker = useCallback(
    (dateRange: [string, string]) => {
      onChange(dateRange);
    },
    [onChange]
  );

  const onOpenChange = (open: boolean) => {
    setOpen(open);
  };

  return (
    <Space gap="1x">
      <TooltipProvider title="Delete this date range">
        <DeleteFilterButton onPress={onRemove} />
      </TooltipProvider>
      <FilterLabel
        isCompact={isCompact}
        isMissing={isMissing}
        type="time"
        member="timeDimension"
        name={member.dimension}
      />
      <Text>for</Text>
      <Select
        aria-label="Date range preset"
        size="small"
        placeholder="Select range"
        width="min 20x"
        validationState={!open && !member.dateRange ? 'invalid' : undefined}
        // @ts-ignore
        selectedKey={
          Array.isArray(member.dateRange) ? 'custom' : (member.dateRange?.toLowerCase() as Key)
        }
        onSelectionChange={onDateRangeChange}
        onOpenChange={onOpenChange}
      >
        {DATA_RANGES.map((range) => {
          return (
            <Item key={range} textValue={capitalize(range)}>
              <Text preset="t3m">{capitalize(range)}</Text>
            </Item>
          );
        })}
      </Select>
      {Array.isArray(member.dateRange) ? (
        <TimeDateRangeSelector value={member.dateRange} onChange={onDataRangeChangeInPicker} />
      ) : undefined}
      {/*<Text>by</Text>*/}
      {/*<Select*/}
      {/*  size="small"*/}
      {/*  selectedKey={member.granularity || GRANULARITIES[0]}*/}
      {/*  onSelectionChange={onGranularityChange}*/}
      {/*>*/}
      {/*  {GRANULARITIES.map((key) => {*/}
      {/*    return (*/}
      {/*      <Item key={key} textValue={capitalize(key)}>*/}
      {/*        <Text preset="t3m">{capitalize(key)}</Text>*/}
      {/*      </Item>*/}
      {/*    );*/}
      {/*  })}*/}
      {/*</Select>*/}
    </Space>
  );
}
