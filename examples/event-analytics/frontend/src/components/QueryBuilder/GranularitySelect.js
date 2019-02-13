import React from 'react';

import Select from 'react-select';

const options = [
  { value: 'hour', label: 'Hourly' },
  { value: 'day', label: 'Daily' },
  { value: 'week', label: 'Weekly' },
  { value: 'month', label: 'Montly' },
]

const customStyles = {
  container: (provided) => ({
    ...provided,
    width: 120
  })
}

const setDefaultValue = (value) => (
  options.find(i => i.value === value)
)

const GranularitySelect = ({ value, onChange }) => (
  <>
    <Select
      defaultValue={setDefaultValue(value)}
      styles={customStyles}
      options={options}
      onChange={(value, action) => (
        onChange({ type: 'CHANGE_GRANULARITY', value: value.value })
      )}
    />
  </>
)

export default GranularitySelect;
