import { DatePicker, parseAbsoluteDate } from '@cube-dev/ui-kit';
import { useCallback, useMemo } from 'react';

interface TimeDateSelectorProps {
  type?: 'time' | 'date';
  value?: string;
  onChange: (value: string) => void;
}

const MIN_DATE_VALUE = parseAbsoluteDate('1980-01-01');

export function TimeDateSelector(props: TimeDateSelectorProps) {
  const { value, onChange } = props;

  const onChangeHandler = useCallback(
    (val: fakeAny) => {
      onChange(val.toString().split(/[+\]]/)[0]);
    },
    [onChange]
  );

  const dateValue = useMemo(() => parseAbsoluteDate(value), [value]);

  return (
    <DatePicker
      aria-label="Date picker"
      size="small"
      minValue={MIN_DATE_VALUE}
      defaultValue={dateValue}
      onChange={onChangeHandler}
    />
  );
}
