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
import { getUserPreference, setUserPreference } from "../utils.js";

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

const apps = [
  { id: 1, title: "Demo app #1" },
  { id: 2, title: "Demo app #2" }
];

const Header = ({ location }) => {
   const [appMenu, setAppMenu] = React.useState(null);
   const [currentApp, setCurrentApp] = React.useState(apps[0]);
   const handleAppIconClick = event => {
    setAppMenu(event.currentTarget);
  };
  const handleAppMenuClose = (app) => {
    setCurrentApp(app)
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
              { currentApp.title }
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
          {apps.map(({ id, title }) => (
           <MenuItem
              component="a"
              data-no-link="true"
              key={id}
              onClick={() => handleAppMenuClose({ id, title })}
            >
              {title}
            </MenuItem>
          ))}
        </Menu>
      </Toolbar>
    </AppBar>
  )
};

export default withRouter(Header);
