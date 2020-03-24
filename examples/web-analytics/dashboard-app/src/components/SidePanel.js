import React from "react";
import Drawer from '@material-ui/core/Drawer';
import Divider from '@material-ui/core/Divider';
import { useLocation } from "react-router-dom";
import { matchPath } from "react-router";


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

const MenuItem = ({ to, title, icon, seletedPaths }) => {
  const { pathname } = useLocation();
  const selected = (seletedPaths || [to]).some(path => matchPath(pathname, {path: path, exact: true}))
  return (
    <ListItem button to={to} component={Link} selected={selected}>
      <ListItemIcon>
        {icon}
      </ListItemIcon>
      <ListItemText primary={title} />
    </ListItem>
  )
};

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
      <List>
        <MenuItem to="/" icon={<PersonIcon />} title="Audience" />
        <MenuItem to="/behavior" icon={<WebIcon />} title="Behavior" />
        <MenuItem to="/acquisition" icon={<ShareIcon />} title="Acquisition" />
        <MenuItem
          to="/custom-reports-overview"
          icon={<AssignmentIcon />}
          title="Custom Reports"
          seletedPaths={[
            "/custom-reports-overview",
            "/custom-reports/:id",
            "/custom-reports-builder/:id?"
          ]}
        />
      </List>
    </Drawer>
  );
};

export default SidePanel;
