import React from "react";
import { Link } from "react-router-dom";
import { withRouter } from "react-router";
import AppBar from "@material-ui/core/AppBar";
import Toolbar from "@material-ui/core/Toolbar";
import Typography from "@material-ui/core/Typography";
import IconButton from "@material-ui/core/IconButton";
import Tooltip from '@material-ui/core/Tooltip';
import Button from '@material-ui/core/Button';
import Menu from '@material-ui/core/Menu';
import MenuItem from '@material-ui/core/MenuItem';
import MenuIcon from "@material-ui/icons/Menu";
import AppsIcon from '@material-ui/icons/Apps';
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';
import { makeStyles } from "@material-ui/core/styles";

import { drawerWidth } from "./SidePanel";

const useStyles = makeStyles(theme => ({
  appBar: {
    zIndex: theme.zIndex.drawer + 1
  },
  grow: {
    flex: '1 1 auto',
  },
  appName: {
    margin: theme.spacing(0, 0.5, 0, 1),
    display: 'none',
    [theme.breakpoints.up('md')]: {
      display: 'block',
    }
  }
}));

const Header = ({ location }) => {
   const [appMenu, setAppMenu] = React.useState(null);
   const handleAppIconClick = event => {
    setAppMenu(event.currentTarget);
  };
  const handleAppMenuClose = event => {
    if (event.currentTarget.nodeName === 'A') {
      // TODO
    }
    setAppMenu(null);
  };
  const classes = useStyles();
  return (
    <AppBar position="fixed" className={classes.appBar}>
      <Toolbar>
        <Typography variant="h6" color="inherit">
          Web Analytics
        </Typography>
        <div className={classes.grow} />
        <Tooltip title='Change App' enterDelay={300}>
          <Button
            color="inherit"
            aria-owns={appMenu ? 'app-menu' : undefined}
            aria-haspopup="true"
            aria-label='Change App'
            onClick={handleAppIconClick}
          >
            <AppsIcon />
            <span className={classes.appName}>
              My Demo App
            </span>
            <ExpandMoreIcon fontSize="small" />
          </Button>
        </Tooltip>
        <Menu
          id="language-menu"
          anchorEl={appMenu}
          open={Boolean(appMenu)}
          onClose={handleAppMenuClose}
        >
          {["My Demo App", "My Demo App #2"].map(appName => (
           <MenuItem
              component="a"
              data-no-link="true"
              href='/'
              key={appName}
              onClick={handleAppMenuClose}
            >
              {appName}
            </MenuItem>
          ))}
        </Menu>
      </Toolbar>
    </AppBar>
  )
};

export default withRouter(Header);
