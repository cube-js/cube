import React from 'react';
import Button from '@material-ui/core/Button';

const SaveButton = ({ disabled })  => (
  <Button
    // because of Github corner
    style={{ zIndex: 1203 }}
    variant="contained"
    color="primary"
    disabled={disabled}
    onClick={() => {
      window.snowplow('trackStructEvent', 'Reports', 'Save Button Clicked');
      alert("Saving reports in demo mode is not available")
    }}
  >
    Save
  </Button>
)

export default SaveButton;
