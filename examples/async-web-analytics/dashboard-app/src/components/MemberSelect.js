import React from "react";
import Button from '@material-ui/core/Button';
import Menu from '@material-ui/core/Menu';
import MenuItem from '@material-ui/core/MenuItem';
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';
import ClearIcon from '@material-ui/icons/Clear';
import { makeStyles } from "@material-ui/core/styles";

const useStyles = makeStyles(theme => ({
  memberName: {
    margin: theme.spacing(0, 0.5, 0, 1),
    display: 'none',
    [theme.breakpoints.up('md')]: {
      display: 'block',
    }
  },
  clearIcon: {
    verticalAlign: 'middle',
    margin: theme.spacing(0, 1, 0, 1),
    cursor: 'pointer'
  }
}));

const MemberSelect = ({ member, onSelect, onRemove, title, availableMembers }) => {
  const [menu, setMenu] = React.useState(null);
  const handleIconClick = event => {
    setMenu(event.currentTarget);
  };
  const handleMenuClose = (event, newMember) => {
    if (event.currentTarget.nodeName === 'LI') {
      if (!!member) {
        onSelect(member, newMember);
      } else {
        onSelect(newMember)
      }
    }
    setMenu(null);
  };
  const classes = useStyles();
  return ([
    <Button
      color="inherit"
      aria-owns={menu ? 'member-menu' : undefined}
      aria-haspopup="true"
      aria-label='Change App'
      onClick={handleIconClick}
      variant={member ? 'outlined' : 'contained'}
    >
      { member ? ([
        <span className={classes.memberName}>
          {member.shortTitle}
        </span>,
        <ExpandMoreIcon fontSize="small" />
      ]) : `+ add ${title}`
      }
    </Button>,
    member && <ClearIcon onClick={() => onRemove(member)} className={classes.clearIcon} />,
    <Menu
      id="language-menu"
      anchorEl={menu}
      open={Boolean(menu)}
      onClose={handleMenuClose}
    >
      {availableMembers.map(member => (
       <MenuItem
          data-no-link="true"
          key={member.name}
          onClick={(event) => handleMenuClose(event, member)}
        >
          {member.shortTitle}
        </MenuItem>
      ))}
    </Menu>
  ])
};

export default MemberSelect;
