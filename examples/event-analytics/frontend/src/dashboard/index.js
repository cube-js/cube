import React from 'react';
import PropTypes from 'prop-types';

import { withStyles } from '@material-ui/core/styles';

import Chart from '../components/Charts';
import WindowTitle from '../components/WindowTitle';
import queries from './queriesList';

const styles = theme => ({
  cardContainerStyles: {
    display: "grid",
    gridColumnGap: "24px",
    gridTemplateColumns: "1fr 1fr",
    rowGap: "24px"
  }
});

const DashboardPage = ({ classes }) => (
  <>
    <WindowTitle title="Dashboard" />
    <div className={classes.cardContainerStyles}>
      {
        queries.map((query, index) => <Chart {...query} key={index} />)
      }
    </div>
  </>
);

DashboardPage.propTypes = {
  classes: PropTypes.object.isRequired,
};

export default withStyles(styles)(DashboardPage);
