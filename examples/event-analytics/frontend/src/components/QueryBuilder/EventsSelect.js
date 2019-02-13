import React from 'react';
import Select from 'react-select';

const options = [
  { value: 'Events.anyEvent', label: 'Any Event' },
  { value: 'Events.pageView', label: 'Page View' },
  { value: 'Events.Navigation__Menu_Closed', label: 'Navigation: Menu Closed' },
  { value: 'Events.Navigation__Menu_Opened', label: 'Navigation: Menu Opened' }
]

const handleChange = (value, action, onChangeProp) => {
  onChangeProp({ type: "REMOVE_MEASURE" })
  onChangeProp({
    type: 'ADD_MEASURE',
    measure: value.value
  })
}

const customStyles = {
  container: (provided) => ({
    ...provided,
    width: 300
  })
}

const EventsSelect = ({ onChange }) => (
  <>
    <Select
      styles={customStyles}
      options={options}
      onChange={(value, action) => handleChange(value, action, onChange)}
    />
  </>
)

export default EventsSelect;
