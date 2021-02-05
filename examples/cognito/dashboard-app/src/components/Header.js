import React from "react";
import { withRouter } from "react-router";
import MenuIcon from "@material-ui/icons/Menu";
import { Button, IconButton, Typography, Toolbar, AppBar } from '@material-ui/core';
import { makeStyles } from '@material-ui/core/styles';
import { useAmplify } from '../libs/amplify';

const useStyles = makeStyles((theme) => ({
  menuButton: {
    marginRight: theme.spacing(2),
  },
  title: {
    flexGrow: 1,
  },
}));

const Header = ({ location }) => {
  const { logout, authenticated } = useAmplify();
  const classes = useStyles();

  return (
    <AppBar position="static">
      <Toolbar variant="dense">
        <IconButton className={classes.menuButton} edge="start" color="inherit" aria-label="menu">
          <MenuIcon />
        </IconButton>
        <Typography className={classes.title} variant="h6" color="inherit">
          My Dashboard
        </Typography>
        {authenticated && (
          <Button onClick={() => {
            logout();
          }}>
            Log Out
          </Button>
        )}
      </Toolbar>
    </AppBar>
  );
};

export default withRouter(Header);
