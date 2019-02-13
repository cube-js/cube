import React from 'react';
import Button from '@material-ui/core/Button';

const SaveButton = ({ disabled })  => (
  <Button
    variant="contained"
    color="primary"
    disabled={disabled}
    onClick={() => alert("Saving reports in demo mode is not available")}
  >
    Save
  </Button>
)

export default SaveButton;
