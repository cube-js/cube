---
order: 8
title: "Data Table"
---

[Data Table](https://material-ui.com/components/tables/) is used for displaying tabular data. Features include sorting, searching, pagination, inline-editing, and row selection.

### Adding Side Bar

As we are adding a new page to out dashboard we want to create a sidebar for navigation...

For example we can download layout folder from github

```jsx
https://github.com/cube-js/cube.js/tree/master/examples/material-ui-dashboard/dashboard-app/src/layouts
https://github.com/cube-js/cube.js/tree/master/examples/material-ui-dashboard/dashboard-app/public/images
```

Now let's add this layout. Go to App.js and add

```diff
// ...
import 'typeface-roboto';
- import Header from "./components/Header";
+ import {Main} from './layouts'
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

![Result screen](/images/dashboard_result.png)

For create data table we need to customize schema

### Define new metrics in Data Schema

We will display order data in our data table.

We've already had definitions in the data schema for most of the metrics we want to display, but we have added definitions for orders size, orders price, and user's full name.

**Customizing Users schema**

Let's add the full name in Users schema by the path `schema/Users.js` 

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

**Customizing Orders schema**

Orders size and orders price we're going to use [subQuery](https://cube.dev/docs/subquery#top). 

You can use subQuery dimensions to reference measures from other cubes inside a dimension.

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

### Creating an Orders Page

Let's add a new page. Open index.js file and add new route and redirect.

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

Now we need to create this page.

`pages/DataTablePage.js`

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

This component contains a query. Later we can use this for adding new filters. The table component rendering query results. If we will change the query, the table component will be rendered. And we can use it!

But first you need to create  this table component file with the following content.

`components/Table.js`

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

Order status table cell uses simple component - StatusBullet. You can create this component by path `components/StatusBullet.js` with the following content.

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

Nice! We created a simple table. 🎉

![Simple table screen](/images/simple_table.png)

But will be better if we do this table with filters! I hide these filters in the toolbar component. Let's create this component and to our table interactive.

For the toolbar component we use some dependencies, to install dependencies run the command:

```jsx
// npm
npm i @date-io/date-fns@1.x date-fns @date-io/moment@1.x moment @material-ui/lab/Autocomplete
// yarn 
yarn add @date-io/date-fns@1.x date-fns @date-io/moment@1.x moment @material-ui/lab/Autocomplete
```

Now we can create our first filter. For this create Toolbar component by the path `components/toolbar.js`

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

If we look at this component we will see what we create custom Tab component with custom styles. This tab uses the setStatusFilter method. This method we get by the props. Now let's add this component, props, and filter in the parent component.

`pages/DataTablePage.js`

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

Yes, we got something similar to a data table.🎉

![Data table gif](/images/data_table.gif)

After that, it would be good to add new filters.

Open `components/toolbar.js` and add filters by date and price.

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

Nice, the next step is to edit our parent component. We need to add state, modify our query, and add new props to the toolbar component. Also, we will add sorting to our table. Open `pages/DataTablePage.js` and add

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

Nice, we added filters! 🎉 If you want [you can add more filters](https://cube.dev/docs/query-format#filters-format).

How you can see also we added sorting props to the table, but we don't release it.

Let's fix this. Open Table component `components/Table.js` and add this code

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

### Done

Congratulations on completing this guide! 🎉

You can check [the online demo here](https://material-ui-dashboard.cubecloudapp.dev/#/dashboard) and [the source code is available on Github](https://github.com/cube-js/cube.js/tree/master/examples/material-ui-dashboard)

![Result Dashboard Page screen](/images/dashboard_done.png)

![Result Table Page screen](/images/table_done.png)
