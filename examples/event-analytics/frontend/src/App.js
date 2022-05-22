import React from 'react';
import PropTypes from 'prop-types';
import classNames from 'classnames';

import { withStyles } from '@material-ui/core/styles';
import CssBaseline from '@material-ui/core/CssBaseline';
import Drawer from '@material-ui/core/Drawer';
import List from '@material-ui/core/List';
import Typography from '@material-ui/core/Typography';
import ListItems from './listItems';

const drawerWidth = 240;

const styles = theme => ({
  root: {
    display: 'flex',
    backgroundColor: theme.palette.grey[50]
  },
  toolbar: {
    paddingRight: 24,
  },
  toolbarIcon: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'flex-end',
    padding: '0 8px',
    ...theme.mixins.toolbar,
  },
  appBar: {
    zIndex: theme.zIndex.drawer + 1,
    transition: theme.transitions.create(['width', 'margin'], {
      easing: theme.transitions.easing.sharp,
      duration: theme.transitions.duration.leavingScreen,
    }),
  },
  appBarShift: {
    marginLeft: drawerWidth,
    width: `calc(100% - ${drawerWidth}px)`,
    transition: theme.transitions.create(['width', 'margin'], {
      easing: theme.transitions.easing.sharp,
      duration: theme.transitions.duration.enteringScreen,
    }),
  },
  menuButton: {
    marginLeft: 12,
    marginRight: 36,
  },
  menuButtonHidden: {
    display: 'none',
  },
  title: {
    flexGrow: 1,
  },
  drawerPaper: {
    position: 'relative',
    whiteSpace: 'nowrap',
    width: drawerWidth,
    transition: theme.transitions.create('width', {
      easing: theme.transitions.easing.sharp,
      duration: theme.transitions.duration.enteringScreen,
    }),
  },
  drawerPaperClose: {
    overflowX: 'hidden',
    transition: theme.transitions.create('width', {
      easing: theme.transitions.easing.sharp,
      duration: theme.transitions.duration.leavingScreen,
    }),
    width: theme.spacing.unit * 7,
    [theme.breakpoints.up('sm')]: {
      width: theme.spacing.unit * 9,
    },
  },
  appBarSpacer: theme.mixins.toolbar,
  content: {
    flexGrow: 1,
    padding: theme.spacing.unit * 3,
    minHeight: '100vh',
  },
  contentTitle: {
    marginBottom: theme.spacing.unit * 3,
  },
  h5: {
    marginBottom: theme.spacing.unit * 2,
  },
});

class App extends React.Component {
  render() {
    const { classes, title, children } = this.props;

    return (
      <>
        <div className={classes.root}>
          <CssBaseline />
          <Drawer
            variant="permanent"
            classes={{
              paper: classNames(classes.drawerPaper, classes.drawerPaperClose),
            }}
          >
            <List><ListItems /></List>
          </Drawer>
          <main className={classes.content}>
            <div className={classes.contentTitle}>
              <Typography
                component="h2"
                variant="h4"
              >
                { title }
              </Typography>
            </div>
            { children }
          </main>
        </div>
      </>
    );
  }
}

App.propTypes = {
  classes: PropTypes.object.isRequired,
  title: PropTypes.string
};

App.defaultProps = {
  title: null
};

export default withStyles(styles)(App);
