import React from "react";
import { makeStyles } from '@material-ui/core/styles';
import { Link } from "react-router-dom";
import { withRouter } from "react-router";
import AppBar from "@material-ui/core/AppBar";
import Toolbar from "@material-ui/core/Toolbar";
import Typography from "@material-ui/core/Typography";
import IconButton from "@material-ui/core/IconButton";
import MenuIcon from "@material-ui/icons/Menu";
import logo from "./cubejs-logo-white.svg";

const useStyles = makeStyles((theme) => ({
  logo: {
    display: "flex",
    alignItems: "center"
  },
  exampleName: {
    marginLeft: 15,
    color: "#A1A1B5"
  },
  appBar: {
    backgroundColor: "#43436B"
  },
  toolBar: {
    minHeight: 60
  }
}));

const Header = ({ location }) => {
  const classes = useStyles();
  return (
    <AppBar position="static" className={classes.appBar}>
      <Toolbar variant="dense" className={classes.toolBar}>
        <div className={classes.logo}>
          <img src={logo} />
          <span className={classes.exampleName}>Drill Downs</span>
        </div>
      </Toolbar>
    </AppBar>
  );
};

export default withRouter(Header);
