import React from 'react';
import Select from 'react-select';
import moment from 'moment';

const generateDateRange = (start, end) => ([
  moment().subtract(...start).format('YYYY-MM-DD'),
  moment().subtract(...end).format('YYYY-MM-DD')
])

const options = [
  { value: generateDateRange([0, 'd'], [0, 'd']), label: 'Current day' },
  { value: generateDateRange([1, 'd'], [1, 'd']), label: 'Previous day' },
  { value: generateDateRange([7, 'd'], [0, 'd']), label: 'Last 7 days' },
  { value: generateDateRange([14, 'd'], [0, 'd']), label: 'Last 14 days', default: true },
  { value: generateDateRange([30, 'd'], [0, 'd']), label: 'Last 30 days' },
  { value: generateDateRange([12, 'months'], [0, 'd']), label: 'Last 12 months' }
]

export const defaultDateRange = options.find(i => i.default);

const customStyles = {
  container: (provided) => ({
    ...provided,
    width: 200
  })
}

const DateRangeSelect = ({ defaultValue, onChange }) => (
  <>
    <Select
      styles={customStyles}
      defaultValue={defaultValue}
      options={options}
      onChange={(value, action) => {
        onChange({
          type: 'CHANGE_DATERANGE',
          value: value.value
        })
        window.snowplow('trackStructEvent', 'Reports', 'Date Range Changed');
      }}
    />
  </>
)

export default DateRangeSelect;
