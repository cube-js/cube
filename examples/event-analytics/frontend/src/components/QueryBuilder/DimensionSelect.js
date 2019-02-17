import React from 'react';
import Select from 'react-select';

const options = [
  {
    label: "User properties",
    options: [
      { value: 'Users.id', label: 'User ID' },
      { value: 'Users.lastSeen', label: 'Last Seen' },
    ]
  },
  {
    label: "Event properties",
    options: [
      { value: 'Events.pageTitle', label: 'Page Title' },
      { value: 'Events.referrer', label: 'Referrer' }
    ]
  }
]

const customStyles = {
  container: (provided) => ({
    ...provided,
    width: 300
  })
}

const handleChange = (value, action, onChangeProp) => {
  onChangeProp({ type: "REMOVE_DIMENSION" })
  if (value) {
    onChangeProp({
      type: 'ADD_DIMENSION',
      value: value.value
    })
  }
}

const DimensionSelect = ({ onChange }) => (
  <>
    <Select
      isClearable
      styles={customStyles}
      options={options}
      onChange={(value, action) => handleChange(value, action, onChange)}
    />
  </>
)

export default DimensionSelect;
