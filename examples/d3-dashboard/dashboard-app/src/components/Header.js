import React from "react";
import { withRouter } from "react-router";
import AppBar from "@material-ui/core/AppBar";
import Toolbar from "@material-ui/core/Toolbar";
import Typography from "@material-ui/core/Typography";
import GithubIcon from "@material-ui/icons/GitHub";
import Button from "@material-ui/core/Button";

const Header = ({ location }) => (
  <AppBar position="static">
    <Toolbar>
      <Typography variant="h6" color="inherit" style={{ flex: 1 }}>
        Cube.js with D3 Example
      </Typography>
      <Button
        color="inherit"
        size="large"
        startIcon={<GithubIcon />}
      >
        View Source Code
      </Button>
    </Toolbar>
  </AppBar>
);

export default withRouter(Header);
