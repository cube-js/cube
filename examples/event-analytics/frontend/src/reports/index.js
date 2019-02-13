import React from 'react';

import { withStyles } from '@material-ui/core/styles';

import WindowTitle from '../components/WindowTitle';
import EventsSelect from '../components/EventsSelect';

const styles = ({
  root: {
    display: 'flex',
    flexWrap: 'wrap',
  },
  formControl: {
    minWidth: 120,
  }
});

const ReportsPage = ({ classes }) => (
  <>
    <WindowTitle title="Reports" />
    <div>
      <EventsSelect />
      <form className={classes.root} autoComplete="off">
      </form>
    </div>
  </>
);

export default withStyles(styles)(ReportsPage);
