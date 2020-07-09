import React from "react";
import { withRouter } from "react-router";
import AppBar from "@material-ui/core/AppBar";
import Toolbar from "@material-ui/core/Toolbar";
import Button from "@material-ui/core/Button";
import GitHubIcon from '@material-ui/icons/GitHub';
import Typography from "@material-ui/core/Typography";
import { makeStyles } from "@material-ui/core/styles";

const useStyles = makeStyles(theme => ({
  appBar: {
    zIndex: theme.zIndex.drawer + 1
  },
  grow: {
    flex: '1 1 auto',
  }
}));

const Header = ({ location }) => {
  const classes = useStyles();
  return (
    <AppBar position="fixed" className={classes.appBar}>
      <Toolbar>
        <Typography variant="h6" color="inherit" noWrap className={classes.title}>
          Cube.js Web Analytics Demo
        </Typography>
        <div className={classes.grow} />
        <Button
          component="a"
          href="https://github.com/cube-js/cube.js/tree/master/examples/web-analytics"
          endIcon={<GitHubIcon />}
          color="inherit"
        >
          Source Code
        </Button>
      </Toolbar>
    </AppBar>
  )
};

export default withRouter(Header);
