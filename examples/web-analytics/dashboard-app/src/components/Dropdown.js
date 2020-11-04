import React from 'react';
import Button from '@material-ui/core/Button';
import Menu from '@material-ui/core/Menu';
import MenuItem from '@material-ui/core/MenuItem';
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';

const ITEM_HEIGHT = 48;

export default function Dropdown({ value, options }) {
  const [anchorEl, setAnchorEl] = React.useState(null);
  const open = Boolean(anchorEl);

  const handleClick = event => {
    setAnchorEl(event.currentTarget);
  };

  const handleClose = (callback) => {
    setAnchorEl(null);
    callback && callback();
  };

  return (
    <div>
      <Button
        color="inherit"
        aria-haspopup="true"
        onClick={handleClick}
      >
        { value }
        <ExpandMoreIcon fontSize="small" />
      </Button>
      <Menu
        id="long-menu"
        anchorEl={anchorEl}
        keepMounted
        open={open}
        onClose={() => handleClose() }
        PaperProps={{
          style: {
            maxHeight: ITEM_HEIGHT * 4.5,
            width: 200,
          },
        }}
      >
        {Object.keys(options).map(option => (
          <MenuItem key={option} onClick={() => handleClose(options[option])}>
            {option}
          </MenuItem>
        ))}
      </Menu>
    </div>
  );
}
