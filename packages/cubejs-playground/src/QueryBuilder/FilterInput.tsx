import { Input } from 'antd';
import { debounce } from 'throttle-debounce';
import { useRef, useState } from 'react';

import { Select } from '../components';

const FilterInputs = {
  string: ({ values = [], disabled, onChange }) => (
    <Select
      key="input"
      disabled={disabled}
      style={{ width: 300 }}
      mode="tags"
      value={values}
      maxTagCount="responsive"
      onChange={onChange}
    />
  ),
  number: ({ values = [], disabled, onChange }) => (
    <Input
      key="input"
      disabled={disabled}
      style={{ width: 300 }}
      onChange={(e) => onChange([e.target.value])}
      value={values?.[0] || ''}
    />
  ),
};

export default function FilterInput({
  member,
  disabled = false,
  updateMethods,
}) {
  const Filter = FilterInputs[member.dimension.type] || FilterInputs.string;

  const ref = useRef(
    debounce<(member: any, values: string[]) => void>(500, (member, values) => {
      updateMethods.update(member, { ...member, values });
    })
  );
  const [values, setValues] = useState<string[]>(member.values);

  return (
    <Filter
      key="filter"
      disabled={disabled}
      values={values}
      onChange={(values) => {
        setValues(values);
        ref.current(member, values);
      }}
    />
  );
}
