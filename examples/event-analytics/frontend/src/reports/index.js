import React from 'react';

import { withStyles } from '@material-ui/core/styles';

import WindowTitle from '../components/WindowTitle';
import QueryBuilder from '../components/QueryBuilder';

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
    <QueryBuilder />
  </>
);

export default withStyles(styles)(ReportsPage);
