import { DateRangeSeparatedPicker, parseAbsoluteDate } from '@cube-dev/ui-kit';
import { useCallback, useMemo } from 'react';

interface TimeDateRangeSelectorProps {
  type?: 'time' | 'date';
  value: [string | undefined, string | undefined];
  onChange: (value: [string, string]) => void;
}

export function TimeDateRangeSelector(props: TimeDateRangeSelectorProps) {
  const { value, onChange } = props;

  const onChangeHandler = useCallback(
    (val) => {
      onChange([
        val.start.toString().split(/[+\]]/)[0].replace('T00:00:00', ''),
        val.end.toString().split(/[+\]]/)[0].replace('T00:00:00', ''),
      ]);
    },
    [onChange]
  );

  const dateValue = useMemo(() => {
    const startDate = parseAbsoluteDate(value[0]);
    const endDate = parseAbsoluteDate(value[1]);

    return startDate && endDate
      ? {
          start: startDate,
          end: endDate,
        }
      : null;
  }, [value[0], value[1]]);

  return useMemo(
    () => (
      <DateRangeSeparatedPicker
        aria-label="Date range picker"
        size="small"
        defaultValue={dateValue}
        onChange={onChangeHandler}
      />
    ),
    [onChangeHandler]
  );
}
