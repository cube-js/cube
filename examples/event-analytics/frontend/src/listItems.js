import React from 'react';
import { Link } from 'react-router-dom'

import { withStyles } from '@material-ui/core/styles';
import ListItem from '@material-ui/core/ListItem';
import ListItemIcon from '@material-ui/core/ListItemIcon';
import ListItemText from '@material-ui/core/ListItemText';
import DashboardIcon from '@material-ui/icons/Dashboard';
import BarChartIcon from '@material-ui/icons/BarChart';
import FilterListIcon from '@material-ui/icons/FilterList';
import HelpIcon from '@material-ui/icons/Help';

const styles = theme => ({
  link: {
    textDecoration: "none",
  }
})

const mainListItems = ({ classes }) => (
  <div>
    <Link to="/" className={classes.link}>
      <ListItem button>
        <ListItemIcon>
          <DashboardIcon />
        </ListItemIcon>
        <ListItemText primary="Dashboard" />
      </ListItem>
    </Link>
    <Link to="/reports" className={classes.link}>
      <ListItem button>
        <ListItemIcon>
          <BarChartIcon />
        </ListItemIcon>
        <ListItemText primary="Reports" />
      </ListItem>
    </Link>
    <Link to="/funnels" className={classes.link}>
      <ListItem button>
        <ListItemIcon>
          <FilterListIcon />
        </ListItemIcon>
        <ListItemText primary="Funnels" />
      </ListItem>
    </Link>
    <Link to="/about" className={classes.link}>
      <ListItem button>
        <ListItemIcon>
          <HelpIcon />
        </ListItemIcon>
        <ListItemText primary="About" />
      </ListItem>
    </Link>
  </div>
);

export default withStyles(styles)(mainListItems);
