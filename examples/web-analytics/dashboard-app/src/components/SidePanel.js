import React from "react";
import Drawer from '@material-ui/core/Drawer';
import Divider from '@material-ui/core/Divider';

import List from '@material-ui/core/List';

import ListItem from '@material-ui/core/ListItem';
import ListItemIcon from '@material-ui/core/ListItemIcon';
import ListItemText from '@material-ui/core/ListItemText';
import PersonIcon from '@material-ui/icons/Person';
import WebIcon from '@material-ui/icons/Web';
import ShareIcon from '@material-ui/icons/Share';
import AssignmentIcon from '@material-ui/icons/Assignment';

import { Link } from "react-router-dom";

import { makeStyles } from "@material-ui/core/styles";

export const drawerWidth = 240;

const useStyles = makeStyles(theme => ({
  drawer: {
    width: drawerWidth,
    flexShrink: 0,
  },
  drawerPaper: {
    whiteSpace: 'nowrap',
    width: drawerWidth
  },
  toolbar: theme.mixins.toolbar
}));


const mainListItems = (
  <div>
    <ListItem button to="/" component={Link}>
      <ListItemIcon>
        <PersonIcon />
      </ListItemIcon>
      <ListItemText primary="Audience" />
    </ListItem>
    <ListItem button to="/behavior" component={Link}>
      <ListItemIcon>
        <WebIcon />
      </ListItemIcon>
      <ListItemText primary="Behavior" />
    </ListItem>
    <ListItem button to="/acquisition" component={Link}>
      <ListItemIcon>
        <ShareIcon />
      </ListItemIcon>
      <ListItemText primary="Acquisition" />
    </ListItem>
    <ListItem button to="/custom-reports" component={Link}>
      <ListItemIcon>
        <AssignmentIcon />
      </ListItemIcon>
      <ListItemText primary="Custom Reports" />
    </ListItem>
  </div>
);

const SidePanel = () => {
  const classes = useStyles();
  return (
    <Drawer
      className={classes.drawer}
      classes={{paper: classes.drawerPaper}}
      variant="permanent"
      open={true}
    >
      <div className={classes.toolbar} />
      <List>{mainListItems}</List>
    </Drawer>
  );
};

export default SidePanel;
