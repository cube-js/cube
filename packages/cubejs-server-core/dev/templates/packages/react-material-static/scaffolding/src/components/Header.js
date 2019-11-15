import React from "react";
import { Link } from "react-router-dom";
import { withRouter } from "react-router";
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import Typography from '@material-ui/core/Typography';
import IconButton from '@material-ui/core/IconButton';
import MenuIcon from '@material-ui/icons/Menu';

const Header = ({ location }) => (
  <AppBar position="static">
    <Toolbar variant="dense">
      <IconButton edge="start" color="inherit" aria-label="menu">
        <MenuIcon />
      </IconButton>
      <Typography variant="h6" color="inherit">
        My Dashboard
      </Typography>
    </Toolbar>
  </AppBar>
);

export default withRouter(Header);
