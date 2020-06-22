import React from "react";
import { makeStyles } from "@material-ui/styles";

import { OrdersToolbar, OrdersTable } from "./components";
import { QueryRenderer } from "@cubejs-client/react";
import cubejs from "@cubejs-client/core";
import CircularProgress from "@material-ui/core/CircularProgress";

const cubejsApi = cubejs(process.env.REACT_APP_CUBEJS_TOKEN, {
  apiUrl: process.env.REACT_APP_API_URL
});

const useStyles = makeStyles(theme => ({
  root: {
    padding: theme.spacing(4)
  },
  content: {
    marginTop: 15
  },
  loaderWrap: {
    width: '100%',
    height: '100%',
    minHeight: 'calc(100vh - 64px)',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center'
  }
}));

const OrderList = () => {
  const classes = useStyles();
  const tabs = ['All', 'Shipped', 'Processing', 'Completed'];
  const [startDate, setStartDate] = React.useState(new Date("2019-01-01T00:00:00"));
  const [finishDate, setFinishDate] = React.useState(new Date("2022-01-01T00:00:00"));
  const [priceFilter, setPriceFilter] = React.useState([0, 200]);
  const [statusFilter, setStatusFilter] = React.useState(0);
  const [sorting, setSorting] = React.useState(['Orders.createdAt', 'desc']);

  const query = {
    limit: 500,
    order: {
      [`${sorting[0]}`]: sorting[1]
    },
    "measures": [
      "Orders.count"
    ],
    "timeDimensions": [
      {
        "dimension": "Orders.createdAt",
        dateRange: [startDate, finishDate],
        "granularity": "day"
      }
    ],
    "dimensions": [
      "Orders.userId",
      "Orders.productId",
      "Orders.createdAt",
      "Orders.status",
      "LineItems.itemPrice",
      "Users.city",
      "Users.company"
    ],
    "filters": [
      {
        "dimension": "LineItems.itemPrice",
        "operator": "gt",
        "values": [
          `${priceFilter[0]}`
        ]
      },
      {
        "dimension": "LineItems.itemPrice",
        "operator": "lt",
        "values": [
          `${priceFilter[1]}`
        ]
      },
      {
        "dimension": "Orders.status",
        "operator": tabs[statusFilter] !== 'All' ? "equals" : "set",
        "values": [
          `${tabs[statusFilter].toLowerCase()}`
        ]
      }
    ]
  };

  return (
    <div className={classes.root}>
      <OrdersToolbar
        startDate={startDate}
        setStartDate={setStartDate}
        finishDate={finishDate}
        setFinishDate={setFinishDate}
        priceFilter={priceFilter}
        setPriceFilter={setPriceFilter}
        statusFilter={statusFilter}
        setStatusFilter={setStatusFilter}
        tabs={tabs}
      />
      <div className={classes.content}>
        <QueryRenderer
          query={query}
          cubejsApi={cubejsApi}
          render={({ resultSet }) => {
            if (!resultSet) {
              return <div className={classes.loaderWrap}><CircularProgress color="secondary" /></div>;
            }
            return (
              <OrdersTable sorting={sorting} setSorting={setSorting} orders={resultSet.tablePivot()}/>
            );
          }}
        />
      </div>
    </div>
  );
};

export default OrderList;
