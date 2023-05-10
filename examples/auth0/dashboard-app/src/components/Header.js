import React from "react";
import { withRouter } from "react-router";
import { useAuth0 } from "@auth0/auth0-react";
import { Button, Typography, Toolbar, AppBar } from '@material-ui/core';
import { makeStyles } from '@material-ui/core/styles';

const useStyles = makeStyles((theme) => ({
  menuButton: {
    marginRight: theme.spacing(2),
  },
  title: {
    flexGrow: 1,
  },
}));

const Header = ({ location }) => {
  const { logout } = useAuth0();
  const classes = useStyles();

  return (
    <AppBar position="static">
      <Toolbar variant="dense">
        <Typography className={classes.title} variant="h6" color="inherit">
          My Dashboard
        </Typography>
        <Button onClick={() => logout({ returnTo: location.origin })} color="inherit">
          Log Out
        </Button>
      </Toolbar>
    </AppBar>
  );
};

export default withRouter(Header);
