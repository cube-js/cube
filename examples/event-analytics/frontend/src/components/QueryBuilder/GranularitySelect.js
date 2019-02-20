import React from 'react';

import Select from 'react-select';

const options = [
  { value: 'hour', label: 'Hourly' },
  { value: 'day', label: 'Daily', default: true },
  { value: 'week', label: 'Weekly' },
  { value: 'month', label: 'Montly' },
]
export const defaultGranularity = options.find(i => i.default)

const customStyles = {
  container: (provided) => ({
    ...provided,
    width: 120
  })
}

const GranularitySelect = ({ defaultValue, onChange }) => (
  <>
    <Select
      defaultValue={defaultValue}
      styles={customStyles}
      options={options}
      onChange={(value, action) => (
        onChange({ type: 'CHANGE_GRANULARITY', value: value.value })
      )}
    />
  </>
)

export default GranularitySelect;
