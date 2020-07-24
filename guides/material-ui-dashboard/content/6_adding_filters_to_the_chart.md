---
order: 6
title: "Adding Filters to the Chart"
---

In the previous part we've created our first chart and now we're going add a way for users to change the date range.

We'll make a separate component for this - `<BarChartHeader />`.

And we can add `<BarChartHeader/>` for control dateRange.

```jsx
import React from 'react';
import PropTypes from 'prop-types';
import { makeStyles } from '@material-ui/styles';
import { CardHeader, Button } from '@material-ui/core';
import ArrowDropDownIcon from '@material-ui/icons/ArrowDropDown';
import Menu from '@material-ui/core/Menu';
import MenuItem from '@material-ui/core/MenuItem';

const useStyles = makeStyles(() => ({
  headerButton: {
    letterSpacing: '0.4px',
  },
}));

const BarChartHeader = (props) => {
  const { setDateRange, dateRange, dates } = props;
  const defaultDates = ['This week', 'This month', 'Last 7 days', 'Last month'];
  const classes = useStyles();

  const [anchorEl, setAnchorEl] = React.useState(null);
  const handleClick = (event) => {
    setAnchorEl(event.currentTarget);
  };
  const handleClose = (date) => {
    setDateRange(date);
    setAnchorEl(null);
  };
  return (
    <CardHeader
      action={
        <div>
          <Button
            className={classes.headerButton}
            size="small"
            variant="text"
            aria-controls="simple-menu"
            aria-haspopup="true"
            onClick={handleClick}
          >
            {dateRange} <ArrowDropDownIcon />
          </Button>
          <Menu
            id="simple-menu"
            anchorEl={anchorEl}
            keepMounted
            open={Boolean(anchorEl)}
            onClose={() => handleClose(dateRange)}
          >
            {dates ?
              dates.map((date) => (
                <MenuItem key={date} onClick={() => handleClose(date)}>{date}</MenuItem>
              ))
             : defaultDates.map((date) => (
                <MenuItem key={date} onClick={() => handleClose(date)}>{date}</MenuItem>
              ))}
          </Menu>
        </div>
      }
      title="Latest Sales"
    />
  );
};

BarChartHeader.propTypes = {
  className: PropTypes.string,
};

export default BarChartHeader;
```

Now let's add this component to our chart in `<BarChart/>` component.

Make the following changes in the ....

```diff
// ...
import ChartRenderer from './ChartRenderer'
+ import BarChartHeader from "./BarChartHeader";
// ...
return (
      <Card
        {...rest}
        className={clsx(classes.root, className)}
      >
+       <BarChartHeader dateRange={dateRange} setDateRange={setDateRange}/>
+       <Divider/>
        <CardContent>
          <div className={classes.chartContainer}>
            <ChartRenderer vizState={{ query, chartType: 'bar' }}/>
          </div>
        </CardContent>
      </Card>
	);
// ...
```

![https://s3-us-west-2.amazonaws.com/secure.notion-static.com/9214b4d6-9104-48a6-9a69-6ffe6163414f/Screenshot_2020-07-02_at_20.24.48.png](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/9214b4d6-9104-48a6-9a69-6ffe6163414f/Screenshot_2020-07-02_at_20.24.48.png)

Well done! 🎉
