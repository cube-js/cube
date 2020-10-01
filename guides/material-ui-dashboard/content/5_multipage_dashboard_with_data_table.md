---
order: 5
title: "Multi-Page Dashboard with Data Table"
---

Now we have a single-page dashboard that displays aggregated business metrics and provides at-a-glance view of several KPIs. However, there's no way to get information about a particular order or a range of orders.

We're going to fix it by adding a second page to our dashboard with the information about all orders. On that page, we'll use the [Data Table](https://material-ui.com/components/tables/) component from Material UI which is great for displaying tabular data. It privdes many rich features like sorting, searching, pagination, inline-editing, and row selection.

However, we'll need a way to navigate between two pages. So, let's add a navigation side bar.

## Navigation Side Bar

First, let's downaload a pre-built layout and images for our dashboard application. Run these commands, extract the `layout.zip` file to the `src/layouts` folder, and the `images.zip` file to the `public/images` folder:

```jsx
curl -LJO https://github.com/cube-js/cube.js/tree/master/examples/material-ui-dashboard/dashboard-app/src/layouts/layouts.zip
curl -LJO https://github.com/cube-js/cube.js/tree/master/examples/material-ui-dashboard/dashboard-app/public/images/images.zip
```

Now we can add this layout to the application. Let's modify the `src/App.js` file:

```diff
// ...
import 'typeface-roboto';
- import Header from "./components/Header";
+ import { Main } from './layouts'
// ...

const AppLayout = ({children}) => {
  const classes = useStyles();
  return (
    <ThemeProvider theme={theme}>
+      <Main>
        <div className={classes.root}>
-         <Header/>
          <div>{children}</div>
        </div>
+      </Main>
    </ThemeProvider>
  );
};
```

Wow! 🎉 Here's our navigation side bar which can be used to switch between different pages of the dashboard:

![](/images/image-31.png)

## Data Table for Orders

To fetch data for the Data Table, we'll need to customize the data schema and define a number of new metrics: amount of items in an order (its size), an order's price, and a user's full name.

First, let's add the full name in the "Users" schema in the `schema/Users.js` file:

```diff
cube(`Users`, {
  sql: `SELECT * FROM public.users`,

	// ...

  dimensions: {    
		
		// ...

    firstName: {
      sql: `first_name`,
      type: `string`
    },

    lastName: {
      sql: `last_name`,
      type: `string`
    },

+    fullName: {
+      sql: `CONCAT(${firstName}, ' ', ${lastName})`,
+      type: `string`
+    },

    age: {
      sql: `age`,
      type: `number`
    },

    createdAt: {
      sql: `created_at`,
      type: `time`
    }
  }
});
```

Then, let's add other measures to the "Orders" schema in the `schema/Orders.js` file.

For these measures, we're going to use the [subquery](https://cube.dev/docs/subquery#top) feature of Cube.js. You can use subquery dimensions to reference measures from other cubes inside a dimension. Here's how to defined such dimensions:

```diff
cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
+      shown: true
    },

    status: {
      sql: `status`,
      type: `string`
    },

    createdAt: {
      sql: `created_at`,
      type: `time`
    },

    completedAt: {
      sql: `completed_at`,
      type: `time`
    },

+    size: {
+      sql: `${LineItems.count}`,
+      subQuery: true,
+      type: 'number'
+    },
+
+    price: {
+      sql: `${LineItems.price}`,
+      subQuery: true,
+      type: 'number'
+    }
  }
});
```

Now we're ready to add a new page. Open the `src/index.js` file and add a new route and a default redirect:

```diff
import React from "react";
import ReactDOM from "react-dom";
import "./index.css";
import App from "./App";
import * as serviceWorker from "./serviceWorker";
- import { HashRouter as Router, Route } from "react-router-dom";
+ import { HashRouter as Router, Route, Switch, Redirect } from "react-router-dom";
import DashboardPage from "./pages/DashboardPage";
+ import DataTablePage from './pages/DataTablePage';

ReactDOM.render(
  <React.StrictMode>
    <Router>
      <App>
-				<Route key="index" exact path="/" component={DashboardPage} />
+        <Switch>
+         <Redirect exact from="/" to="/dashboard"/>
+          <Route key="index" exact path="/dashboard" component={DashboardPage} />
+          <Route key="table" path="/orders" component={DataTablePage} />
+          <Redirect to="/dashboard" />
+        </Switch>
      </App>
    </Router>
  </React.StrictMode>,
  document.getElementById("root")
); 

serviceWorker.unregister();
```

The next step is to create the page referenced in the new route. Add the `src/pages/DataTablePage.js` file with the following contents:

```jsx
import React from "react";
import { makeStyles } from "@material-ui/styles";

import Table from "../components/Table.js";

const useStyles = makeStyles(theme => ({
  root: {
    padding: theme.spacing(4)
  },
  content: {
    marginTop: 15
  },
}));

const DataTablePage = () => {
  const classes = useStyles();

  const query = {
    "limit": 500,
    "timeDimensions": [
      {
        "dimension": "Orders.createdAt",
        "granularity": "day"
      }
    ],
    "dimensions": [
      "Users.id",
      "Orders.id",
      "Orders.size",
      "Users.fullName",
      "Users.city",
      "Orders.price",
      "Orders.status",
      "Orders.createdAt"
    ]
  };

  return (
    <div className={classes.root}>
      <div className={classes.content}>
        <Table query={query}/>
      </div>
    </div>
  );
};

export default DataTablePage;
```

Note that this component contains a Cube.js query. Later, we'll modify this query to enable filtering of the data.

All data items are rendered with the `<Table />` component, and changes to the query result are reflected in the table. Let's create this `<Table />` component in the `src/components/Table.js` file with the following contents:

```jsx
import React, { useState } from "react";
import clsx from "clsx";
import PropTypes from "prop-types";
import moment from "moment";
import PerfectScrollbar from "react-perfect-scrollbar";
import { makeStyles } from "@material-ui/styles";
import { useCubeQuery } from "@cubejs-client/react";
import CircularProgress from "@material-ui/core/CircularProgress";
import {
  Card,
  CardActions,
  CardContent,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  TablePagination, Typography
} from "@material-ui/core";

import StatusBullet from "./StatusBullet";
import palette from "../theme/palette";

const useStyles = makeStyles(theme => ({
  root: {
    padding: 0
  },
  content: {
    padding: 0
  },
  head: {
    backgroundColor: palette.background.gray
  },
  inner: {
    minWidth: 1050
  },
  nameContainer: {
    display: "flex",
    alignItems: "baseline"
  },
  status: {
    marginRight: theme.spacing(2)
  },
  actions: {
    justifyContent: "flex-end"
  },
}));

const statusColors = {
  completed: "success",
  processing: "info",
  shipped: "danger"
};

const TableComponent = props => {

  const { className, query, cubejsApi, ...rest } = props;

  const classes = useStyles();

  const [rowsPerPage, setRowsPerPage] = useState(10);
  const [page, setPage] = useState(0);

  const tableHeaders = [
    {
      text: "Order id",
      value: "Orders.id"
    },
    {
      text: "Orders size",
      value: "Orders.size"
    },
    {
      text: "Full Name",
      value: "Users.fullName"
    },
    {
      text: "User city",
      value: "Users.city"
    },
    {
      text: "Order price",
      value: "Orders.price"
    },
    {
      text: "Status",
      value: "Orders.status"
    },
    {
      text: "Created at",
      value: "Orders.createdAt"
    }
  ];
  const { resultSet, error, isLoading } = useCubeQuery(query, { cubejsApi });
  if (isLoading) {
    return <div style={{display: 'flex', alignItems: 'center', justifyContent: 'center'}}><CircularProgress color="secondary" /></div>;
  }
  if (error) {
    return <pre>{error.toString()}</pre>;
  }
  if (resultSet) {
    let orders = resultSet.tablePivot();

    const handlePageChange = (event, page) => {
      setPage(page);
    };
    const handleRowsPerPageChange = event => {
      setRowsPerPage(event.target.value);
    };

    return (
      <Card
        {...rest}
        padding={"0"}
        className={clsx(classes.root, className)}
      >
        <CardContent className={classes.content}>
          <PerfectScrollbar>
            <div className={classes.inner}>
              <Table>
                <TableHead className={classes.head}>
                  <TableRow>
                    {tableHeaders.map((item) => (
                      <TableCell key={item.value + Math.random()} 
																 className={classes.hoverable}           
                      >
                        <span>{item.text}</span>
              
                      </TableCell>
                    ))}
                  </TableRow>
                </TableHead>
                <TableBody>
                  {orders.slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage).map(obj => (
                    <TableRow
                      className={classes.tableRow}
                      hover
                      key={obj["Orders.id"]}
                    >
                      <TableCell>
                        {obj["Orders.id"]}
                      </TableCell>
                      <TableCell>
                        {obj["Orders.size"]}
                      </TableCell>
                      <TableCell>
                        {obj["Users.fullName"]}
                      </TableCell>
                      <TableCell>
                        {obj["Users.city"]}
                      </TableCell>
                      <TableCell>
                        {"$ " + obj["Orders.price"]}
                      </TableCell>
                      <TableCell>
                        <StatusBullet
                          className={classes.status}
                          color={statusColors[obj["Orders.status"]]}
                          size="sm"
                        />
                        {obj["Orders.status"]}
                      </TableCell>
                      <TableCell>
                        {moment(obj["Orders.createdAt"]).format("DD/MM/YYYY")}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          </PerfectScrollbar>
        </CardContent>
        <CardActions className={classes.actions}>
          <TablePagination
            component="div"
            count={orders.length}
            onChangePage={handlePageChange}
            onChangeRowsPerPage={handleRowsPerPageChange}
            page={page}
            rowsPerPage={rowsPerPage}
            rowsPerPageOptions={[5, 10, 25, 50, 100]}
          />
        </CardActions>
      </Card>
    );
  } else {
    return null
  }
};

TableComponent.propTypes = {
  className: PropTypes.string,
  query: PropTypes.object.isRequired
};

export default TableComponent;
```

The table contains a cell with a custom `<StatusBullet />` component which displays an order's status with a colorful dot. Let's create this component in the `src/components/StatusBullet.js` file with the following contents:

```jsx
import React from 'react';
import PropTypes from 'prop-types';
import clsx from 'clsx';
import { makeStyles } from '@material-ui/styles';

const useStyles = makeStyles(theme => ({
  root: {
    display: 'inline-block',
    borderRadius: '50%',
    flexGrow: 0,
    flexShrink: 0
  },
  sm: {
    height: theme.spacing(1),
    width: theme.spacing(1)
  },
  md: {
    height: theme.spacing(2),
    width: theme.spacing(2)
  },
  lg: {
    height: theme.spacing(3),
    width: theme.spacing(3)
  },
  neutral: {
    backgroundColor: theme.palette.neutral
  },
  primary: {
    backgroundColor: theme.palette.primary.main
  },
  info: {
    backgroundColor: theme.palette.info.main
  },
  warning: {
    backgroundColor: theme.palette.warning.main
  },
  danger: {
    backgroundColor: theme.palette.error.main
  },
  success: {
    backgroundColor: theme.palette.success.main
  }
}));

const StatusBullet = props => {
  const { className, size, color, ...rest } = props;

  const classes = useStyles();

  return (
    <span
      {...rest}
      className={clsx(
        {
          [classes.root]: true,
          [classes[size]]: size,
          [classes[color]]: color
        },
        className
      )}
    />
  );
};

StatusBullet.propTypes = {
  className: PropTypes.string,
  color: PropTypes.oneOf([
    'neutral',
    'primary',
    'info',
    'success',
    'warning',
    'danger'
  ]),
  size: PropTypes.oneOf(['sm', 'md', 'lg'])
};

StatusBullet.defaultProps = {
  size: 'md',
  color: 'default'
};

export default StatusBullet;
```

Nice! 🎉 Now we have a table which displays information about all orders: 

![](/images/image-47.png)

However, its hard to explore this orders using only the controls provided. To fix this, we'll add a comprehensive toolbar with filters and make our table interactive.

First, let's add a few dependencies. Run the command in the `dashboard-app` folder:

```jsx
npm install --save @date-io/date-fns@1.x date-fns @date-io/moment@1.x moment @material-ui/lab/Autocomplete
```

Then, create the `<Toolbar />` component in the `src/components/Toolbar.js` file with the following contents:

```jsx
import "date-fns";
import React from "react";
import PropTypes from "prop-types";
import clsx from "clsx";
import { makeStyles } from "@material-ui/styles";
import Grid from "@material-ui/core/Grid";
import Typography from "@material-ui/core/Typography";
import Tab from "@material-ui/core/Tab";
import Tabs from "@material-ui/core/Tabs";
import withStyles from "@material-ui/core/styles/withStyles";
import palette from "../theme/palette";

const AntTabs = withStyles({
  root: {
    borderBottom: `1px solid ${palette.primary.main}`,
  },
  indicator: {
    backgroundColor: `${palette.primary.main}`,
  },
})(Tabs);
const AntTab = withStyles((theme) => ({
  root: {
    textTransform: 'none',
    minWidth: 25,
    fontSize: 12,
    fontWeight: theme.typography.fontWeightRegular,
    marginRight: theme.spacing(0),
    color: palette.primary.dark,
    opacity: 0.6,
    '&:hover': {
      color: `${palette.primary.main}`,
      opacity: 1,
    },
    '&$selected': {
      color: `${palette.primary.main}`,
      fontWeight: theme.typography.fontWeightMedium,
      outline: 'none',
    },
    '&:focus': {
      color: `${palette.primary.main}`,
      outline: 'none',
    },
  },
  selected: {},
}))((props) => <Tab disableRipple {...props} />);
const useStyles = makeStyles(theme => ({
  root: {},
  row: {
    marginTop: theme.spacing(1)
  },
  spacer: {
    flexGrow: 1
  },
  importButton: {
    marginRight: theme.spacing(1)
  },
  exportButton: {
    marginRight: theme.spacing(1)
  },
  searchInput: {
    marginRight: theme.spacing(1)
  },
  formControl: {
    margin: 25,
    fullWidth: true,
    display: "flex",
    wrap: "nowrap"
  },
  date: {
    marginTop: 3
  },
  range: {
    marginTop: 13
  }
}));

const Toolbar = props => {
  const { className,
    statusFilter,
    setStatusFilter,
    tabs,
    ...rest } = props;
  const [tabValue, setTabValue] = React.useState(statusFilter);

  const classes = useStyles();

  const handleChangeTab = (e, value) => {
    setTabValue(value);
    setStatusFilter(value);
  };

  return (
    <div
      {...rest}
      className={clsx(classes.root, className)}
    >
      <Grid container spacing={4}>
        <Grid
          item
          lg={3}
          sm={6}
          xl={3}
          xs={12}
          m={2}
        >
          <div className={classes}>
            <AntTabs value={tabValue} onChange={(e,value) => {handleChangeTab(e,value)}} aria-label="ant example">
              {tabs.map((item) => (<AntTab key={item} label={item} />))}
            </AntTabs>
            <Typography className={classes.padding} />
          </div>
        </Grid>
      </Grid>
    </div>
  );
};

Toolbar.propTypes = {
  className: PropTypes.string
};

export default Toolbar;
```

Note that we have customized the `<Tab />` component with styles and and the `setStatusFilter` method which is passed via props. Now we can add this component, props, and filter to the parent component. Let's modify the `src/pages/DataTablePage.js` file:

```diff
import React from "react";
import { makeStyles } from "@material-ui/styles";

+ import Toolbar from "../components/Toolbar.js";
import Table from "../components/Table.js";

const useStyles = makeStyles(theme => ({
  root: {
    padding: theme.spacing(4)
  },
  content: {
    marginTop: 15
  },
}));

const DataTablePage = () => {
  const classes = useStyles();
+  const tabs = ['All', 'Shipped', 'Processing', 'Completed'];
+  const [statusFilter, setStatusFilter] = React.useState(0);

  const query = {
    "limit": 500,
    "timeDimensions": [
      {
        "dimension": "Orders.createdAt",
        "granularity": "day"
      }
    ],
    "dimensions": [
      "Users.id",
      "Orders.id",
      "Orders.size",
      "Users.fullName",
      "Users.city",
      "Orders.price",
      "Orders.status",
      "Orders.createdAt"
    ],
+    "filters": [
+      {
+        "dimension": "Orders.status",
+        "operator": tabs[statusFilter] !== 'All' ? "equals" : "set",
+        "values": [
+          `${tabs[statusFilter].toLowerCase()}`
+        ]
+      }
+    ]
  };

  return (
    <div className={classes.root}>
+      <Toolbar
+        statusFilter={statusFilter}
+        setStatusFilter={setStatusFilter}
+        tabs={tabs}
+      />
      <div className={classes.content}>
        <Table
          query={query}/>
      </div>
    </div>
  );
};

export default DataTablePage;
```

Perfect! 🎉 Now the data table has a filter which switches between different types of orders:

![](/images/image-1.gif)

However, orders have other parameters such as price and dates. Let's create filters for these parameters. To do so, modify the `src/components/Toolbar.js` file:

```diff
import "date-fns";
import React from "react";
import PropTypes from "prop-types";
import clsx from "clsx";
import { makeStyles } from "@material-ui/styles";
import Grid from "@material-ui/core/Grid";
import Typography from "@material-ui/core/Typography";
import Tab from "@material-ui/core/Tab";
import Tabs from "@material-ui/core/Tabs";
import withStyles from "@material-ui/core/styles/withStyles";
import palette from "../theme/palette";
+ import DateFnsUtils from "@date-io/date-fns";
+ import {
+   MuiPickersUtilsProvider,
+   KeyboardDatePicker
+ } from "@material-ui/pickers";
+ import Slider from "@material-ui/core/Slider";

// ...

const Toolbar = props => {
  const { className,
+   startDate,
+   setStartDate,
+   finishDate,
+   setFinishDate,
+   priceFilter,
+   setPriceFilter,
    statusFilter,
    setStatusFilter,
    tabs,
    ...rest } = props;
  const [tabValue, setTabValue] = React.useState(statusFilter);
+ const [rangeValue, rangeSetValue] = React.useState(priceFilter);

  const classes = useStyles();

  const handleChangeTab = (e, value) => {
    setTabValue(value);
    setStatusFilter(value);
  };
+  const handleDateChange = (date) => {
+    setStartDate(date);
+  };
+  const handleDateChangeFinish = (date) => {
+    setFinishDate(date);
+  };
+ const handleChangeRange = (event, newValue) => {
+   rangeSetValue(newValue);
+ };
+ const setRangeFilter = (event, newValue) => {
+   setPriceFilter(newValue);
+ };

  return (
    <div
      {...rest}
      className={clsx(classes.root, className)}
    >
      <Grid container spacing={4}>
        <Grid
          item
          lg={3}
          sm={6}
          xl={3}
          xs={12}
          m={2}
        >
          <div className={classes}>
            <AntTabs value={tabValue} onChange={(e,value) => {handleChangeTab(e,value)}} aria-label="ant example">
              {tabs.map((item) => (<AntTab key={item} label={item} />))}
            </AntTabs>
            <Typography className={classes.padding} />
          </div>
        </Grid>
+        <Grid
+          className={classes.date}
+          item
+          lg={3}
+          sm={6}
+          xl={3}
+          xs={12}
+          m={2}
+        >
+          <MuiPickersUtilsProvider utils={DateFnsUtils}>
+            <Grid container justify="space-around">
+              <KeyboardDatePicker
+                id="date-picker-dialog"
+               label={<span style={{opacity: 0.6}}>Start Date</span>}
+                format="MM/dd/yyyy"
+                value={startDate}
+                onChange={handleDateChange}
+                KeyboardButtonProps={{
+                  "aria-label": "change date"
+                }}
+              />
+            </Grid>
+          </MuiPickersUtilsProvider>
+        </Grid>
+        <Grid
+          className={classes.date}
+          item
+          lg={3}
+          sm={6}
+          xl={3}
+          xs={12}
+          m={2}
+        >
+          <MuiPickersUtilsProvider utils={DateFnsUtils}>
+            <Grid container justify="space-around">
+              <KeyboardDatePicker
+                id="date-picker-dialog-finish"
+                label={<span style={{opacity: 0.6}}>Finish Date</span>}
+                format="MM/dd/yyyy"
+                value={finishDate}
+                onChange={handleDateChangeFinish}
+                KeyboardButtonProps={{
+                  "aria-label": "change date"
+                }}
+              />
+            </Grid>
+          </MuiPickersUtilsProvider>
+        </Grid>
+        <Grid
+          className={classes.range}
+          item
+          lg={3}
+          sm={6}
+          xl={3}
+          xs={12}
+          m={2}
+        >
+          <Typography id="range-slider">
+            Order price range
+          </Typography>
+          <Slider
+            value={rangeValue}
+            onChange={handleChangeRange}
+            onChangeCommitted={setRangeFilter}
+            aria-labelledby="range-slider"
+            valueLabelDisplay="auto"
+            min={0}
+            max={2000}
+          />
+        </Grid>
      </Grid>
    </div>
  );
};

Toolbar.propTypes = {
  className: PropTypes.string
};

export default Toolbar;
```

To make these filters work, we need to connect them to the parent component: add state, modify our query, and add new props to the `<Toolbar />` component. Also, we will add sorting to the data table. So, modify the `src/pages/DataTablePage.js` file like this:

```diff
// ...

const DataTablePage = () => {
  const classes = useStyles();
  const tabs = ['All', 'Shipped', 'Processing', 'Completed'];
  const [statusFilter, setStatusFilter] = React.useState(0);
+ const [startDate, setStartDate] = React.useState(new Date("2019-01-01T00:00:00"));
+ const [finishDate, setFinishDate] = React.useState(new Date("2022-01-01T00:00:00"));
+ const [priceFilter, setPriceFilter] = React.useState([0, 200]);
+ const [sorting, setSorting] = React.useState(['Orders.createdAt', 'desc']);

  const query = {
    "limit": 500,
+    "order": {
+      [`${sorting[0]}`]: sorting[1]
+    },
    "measures": [
      "Orders.count"
    ],
    "timeDimensions": [
      {
        "dimension": "Orders.createdAt",
+				"dateRange": [startDate, finishDate],
        "granularity": "day"
      }
    ],
    "dimensions": [
      "Users.id",
      "Orders.id",
      "Orders.size",
      "Users.fullName",
      "Users.city",
      "Orders.price",
      "Orders.status",
      "Orders.createdAt"
    ],
    "filters": [
      {
        "dimension": "Orders.status",
        "operator": tabs[statusFilter] !== 'All' ? "equals" : "set",
        "values": [
          `${tabs[statusFilter].toLowerCase()}`
        ]
      },
+     {
+        "dimension": "Orders.price",
+        "operator": "gt",
+        "values": [
+         `${priceFilter[0]}`
+       ]
+     },
+     {
+       "dimension": "Orders.price",
+       "operator": "lt",
+       "values": [
+         `${priceFilter[1]}`
+       ]
+     },
    ]
  };

  return (
    <div className={classes.root}>
      <Toolbar
+       startDate={startDate}
+       setStartDate={setStartDate}
+       finishDate={finishDate}
+       setFinishDate={setFinishDate}
+       priceFilter={priceFilter}
+       setPriceFilter={setPriceFilter}
        statusFilter={statusFilter}
        setStatusFilter={setStatusFilter}
        tabs={tabs}
      />
      <div className={classes.content}>
        <Table
+          sorting={sorting}
+          setSorting={setSorting}
          query={query}/>
      </div>
    </div>
  );
};

export default DataTablePage;
```

Fantastic! 🎉 We've added some useful filters. Indeed, you can add even more filters with custom logic. See the [documentation](https://cube.dev/docs/query-format#filters-format) for the filter format options.

And there's one more thing. We've added sorting props to the toolbar, but we also need to pass them to the `<Table />` component. To fix this, let's modify the `src/components/Table.js` file:

```diff
// ...

+ import KeyboardArrowUpIcon from "@material-ui/icons/KeyboardArrowUp";
+ import KeyboardArrowDownIcon from "@material-ui/icons/KeyboardArrowDown";
import { useCubeQuery } from "@cubejs-client/react";
import CircularProgress from "@material-ui/core/CircularProgress";

// ...

const useStyles = makeStyles(theme => ({
  // ...
  actions: {
    justifyContent: "flex-end"
  },
+ tableRow: {
+   padding: '0 5px',
+   cursor: "pointer",
+   '.MuiTableRow-root.MuiTableRow-hover&:hover': {
+     backgroundColor: palette.primary.action
+   }
+ },
+ hoverable: {
+   "&:hover": {
+     color: `${palette.primary.normal}`,
+     cursor: `pointer`
+   }
+ },
+ arrow: {
+   fontSize: 10,
+   position: "absolute"
+ }
}));

const statusColors = {
  completed: "success",
  processing: "info",
  shipped: "danger"
};

const TableComponent = props => {
-  const { className, query, cubejsApi, ...rest } = props;
+  const { className, sorting, setSorting, query, cubejsApi, ...rest } = props;

// ...

  if (resultSet) {

	   //...
+     const handleSetSorting = str => {
+       setSorting([str, sorting[1] === "desc" ? "asc" : "desc"]);
+     };

    return (

								// ...
                <TableHead className={classes.head}>
                  <TableRow>
                    {tableHeaders.map((item) => (
                      <TableCell key={item.value + Math.random()} className={classes.hoverable}
+                                 onClick={() => {
+                                 handleSetSorting(`${item.value}`);
+                                 }}
                      >
                        <span>{item.text}</span>
+                        <Typography
+                          className={classes.arrow}
+                          variant="body2"
+                          component="span"
+                        >
+                          {(sorting[0] === item.value) ? (sorting[1] === "desc" ? <KeyboardArrowUpIcon/> :
+                            <KeyboardArrowDownIcon/>) : null}
+                        </Typography>
                      </TableCell>
                    ))}
                  </TableRow>
                </TableHead>
						 // ...
```

Wonderful! 🎉 Now we have the data table that fully supports filtering and sorting: 

![](/images/image-7.gif)

## User Drill Down Page

The data table we've built allows to find informations about a particular order. However, our e-commerce business is quite successful and has a good return rate which means that users are highly likely to make multiple orders over time. So, let's add a drill down page to explore the complete order informations for a particular user.

As it's a new page, let's add a new route to the `src/index.js` file:

```diff
// ...

				<Switch>
          <Redirect exact from="/" to="/dashboard" />
          <Route key="index" exact path="/dashboard" component={DashboardPage} />
          <Route key="table" path="/orders" component={DataTablePage} />
+         <Route key="table" path="/user/:id" component={UsersPage} />
          <Redirect to="/dashboard" />
        </Switch>

// ...
```

For this route to work, we also need to add the `src/pages/UsersPage.js` file with these contents:

```diff
import React from 'react';
import { useParams } from 'react-router-dom';
import { makeStyles } from '@material-ui/styles';
import { useCubeQuery } from '@cubejs-client/react';
import { Grid } from '@material-ui/core';
import AccountProfile from '../components/AccountProfile';
import BarChart from '../components/BarChart';
import CircularProgress from '@material-ui/core/CircularProgress';
import UserSearch from '../components/UserSearch';
import KPIChart from '../components/KPIChart';

const useStyles = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(4),
  },
  row: {
    display: 'flex',
    margin: '0 -15px',
  },
  info: {
    paddingLeft: theme.spacing(2),
    paddingRight: theme.spacing(2),
  },
  sales: {
    marginTop: theme.spacing(4),
  },
  loaderWrap: {
    width: '100%',
    height: '100%',
    minHeight: 'calc(100vh - 64px)',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
  },
}));

const UsersPage = (props) => {
  const classes = useStyles();
  let { id } = useParams();
  const query = {
    measures: ['Users.count'],
    timeDimensions: [
      {
        dimension: 'Users.createdAt',
      },
    ],
    dimensions: [
      'Users.id',
      'Products.id',
      'Users.firstName',
      'Users.lastName',
      'Users.gender',
      'Users.age',
      'Users.city',
      'LineItems.itemPrice',
      'Orders.createdAt',
    ],
    filters: [
      {
        dimension: 'Users.id',
        operator: 'equals',
        values: [`${id}`],
      },
    ],
  };
  const barChartQuery = {
    measures: ['Orders.count'],
    timeDimensions: [
      {
        dimension: 'Orders.createdAt',
        granularity: 'month',
        dateRange: 'This week',
      },
    ],
    dimensions: ['Orders.status'],
    filters: [
      {
        dimension: 'Users.id',
        operator: 'equals',
        values: [id],
      },
    ],
  };
  const cards = [
    {
      title: 'ORDERS',
      query: {
        measures: ['Orders.count'],
        filters: [
          {
            dimension: 'Users.id',
            operator: 'equals',
            values: [`${id}`],
          },
        ],
      },
      duration: 1.25,
    },
    {
      title: 'TOTAL SALES',
      query: {
        measures: ['LineItems.price'],
        filters: [
          {
            dimension: 'Users.id',
            operator: 'equals',
            values: [`${id}`],
          },
        ],
      },
      duration: 1.5,
    },
  ];

  const { resultSet, error, isLoading } = useCubeQuery(query);
  if (isLoading) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <CircularProgress color="secondary" />
      </div>
    );
  }
  if (error) {
    return <pre>{error.toString()}</pre>;
  }
  if (!resultSet) {
    return null;
  }
  if (resultSet) {
    let data = resultSet.tablePivot();
    let userData = data[0];
    return (
      <div className={classes.root}>
        <Grid container spacing={4}>
          <Grid item lg={4} sm={6} xl={4} xs={12}>
            <UserSearch />
            <AccountProfile
              userFirstName={userData['Users.firstName']}
              userLastName={userData['Users.lastName']}
              gender={userData['Users.gender']}
              age={userData['Users.age']}
              city={userData['Users.city']}
              id={id}
            />
          </Grid>
          <Grid item lg={8} sm={6} xl={4} xs={12}>
            <div className={classes.row}>
              {cards.map((item, index) => {
                return (
                  <Grid className={classes.info} key={item.title + index} item lg={6} sm={6} xl={6} xs={12}>
                    <KPIChart {...item} />
                  </Grid>
                );
              })}
            </div>
            <div className={classes.sales}>
              <BarChart query={barChartQuery} dates={['This year', 'Last year']} />
            </div>
          </Grid>
        </Grid>
      </div>
    );
  }
};

export default UsersPage;
```

The last thing will be enable the data table to navigate to this page by clicking on a cell with a user's full name. Let's modify the `src/components/Table.js` like this:

```diff
// ...

import KeyboardArrowUpIcon from '@material-ui/icons/KeyboardArrowUp';
import KeyboardArrowDownIcon from '@material-ui/icons/KeyboardArrowDown';
+ import OpenInNewIcon from '@material-ui/icons/OpenInNew';
import { useCubeQuery } from '@cubejs-client/react';
import CircularProgress from '@material-ui/core/CircularProgress';

// ...

                      <TableCell>{obj['Orders.id']}</TableCell>
                      <TableCell>{obj['Orders.size']}</TableCell>
+                     <TableCell
+                       className={classes.hoverable}
+                       onClick={() => handleClick(`/user/${obj['Users.id']}`)}
+                     >
+                       {obj['Users.fullName']}
+                       &nbsp;
+                       <Typography className={classes.arrow} variant="body2" component="span">
+                         <OpenInNewIcon fontSize="small" />
+                       </Typography>
+                     </TableCell>
                      <TableCell>{obj['Users.city']}</TableCell>
                      <TableCell>{'$ ' + obj['Orders.price']}</TableCell>

// ...
```

Here's what we eventually got:

![](/images/image-202.gif)

And that's all! 😇 Congratulations on completing this guide! 🎉

Also, check the [live demo](https://material-ui-dashboard.cubecloudapp.dev/#/dashboard) and the [full source code](https://github.com/cube-js/cube.js/tree/master/examples/material-ui-dashboard) available on GitHub.

Now you should be able to create comprehensive analytical dashboards powered by Cube.js and using React and Material UI to display aggregate metrics and detailed information.

Feel free to explore [other examples](https://github.com/cube-js/cube.js/#examples) of what can be done with Cube.js such as the [Real-Time Dashboard Guide](https://real-time-dashboard.cube.dev) and the [Open Source Web Analytics Platform Guide](https://web-analytics.cube.dev).