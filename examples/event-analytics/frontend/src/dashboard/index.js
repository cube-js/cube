import React from 'react';
import PropTypes from 'prop-types';
import moment from 'moment';

import { withStyles } from '@material-ui/core/styles';

import Chart from '../components/Charts';
import WindowTitle from '../components/WindowTitle';

const styles = theme => ({
  cardContainerStyles: {
    display: "grid",
    gridColumnGap: "24px",
    gridTemplateColumns: "1fr 1fr",
    rowGap: "24px"
  }
});

// TODO: Move away
const dateRange = [
  moment().subtract(14,'d').format('YYYY-MM-DD'),
  moment().format('YYYY-MM-DD'),
];

const queries = [
  {
    title: "Page Views last 14 days",
    type: "line",
    query: {
      "measures": [
        "PageViews.count"
      ],
      "timeDimensions": [
        {
          "dimension": "PageViews.timestamp",
          "dateRange": dateRange,
          "granularity": "day"
        }
      ]
    }
  },
  {
    title: "Top 5 referrers",
    type: "pie",
    query: {
      "measures": [
        "PageViews.count"
      ],
      "dimensions": [
        "PageViews.referrer"
      ],
      limit: 5
    }
  },
  {
    title: "Products",
    type: "table",
    query: {
      "measures": [
        "PageViews.count"
      ],
      "timeDimensions": [
        {
          "dimension": "PageViews.timestamp",
          "dateRange": dateRange,
          "granularity": "day"
        }
      ]
    }
  },
  {
    title: "Visitors by Referrer",
    type: "bar",
    query: {
      "measures": [
        "PageViews.count"
      ],
      dimensions: [
        "PageViews.referrer"
      ],
      "timeDimensions": [
        {
          "dimension": "PageViews.timestamp",
          "dateRange": dateRange,
          "granularity": "day"
        }
      ]
    }
  },
]


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
