import { Input } from 'antd';

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
      value={(values && values[0]) || ''}
    />
  ),
};

export default function FilterInput({
  member,
  disabled = false,
  updateMethods,
}) {
  const Filter = FilterInputs[member.dimension.type] || FilterInputs.string;
  return (
    <Filter
      key="filter"
      disabled={disabled}
      values={member.values}
      onChange={(values) => updateMethods.update(member, { ...member, values })}
    />
  );
}
