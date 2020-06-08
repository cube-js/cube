import React, { useState } from "react";
import { makeStyles } from "@material-ui/styles";

import { OrdersToolbar, OrdersTable } from "./components";
import { QueryRenderer } from "@cubejs-client/react";
import cubejs from "@cubejs-client/core";

const cubejsApi = cubejs(process.env.REACT_APP_CUBEJS_TOKEN, {
  apiUrl: process.env.REACT_APP_API_URL
});

const useStyles = makeStyles(theme => ({
  root: {
    padding: theme.spacing(3)
  },
  content: {
    marginTop: theme.spacing(6)
  }
}));

const OrderList = () => {
  const classes = useStyles();
  const [userIdFilter, setUserIdFilter] = useState(null);
  const [orderFilter, setOrder] = useState("Orders.createdAt");
  const [limit, setLimit] = useState(100);

  const query = {
    limit: limit,
    order: {
      [`${orderFilter}`]: "desc"
    },
    "measures": [
      "Orders.count"
    ],
    "timeDimensions": [
      {
        "dimension": "Orders.createdAt",
        "granularity": "day"
      }
    ],
    "dimensions": [
      "Orders.user_id",
      "Orders.product_id",
      "Orders.createdAt",
      "Orders.status",
      "Orders.order_id",
      "Users.city",
      "Users.company"
    ],
    "filters": [
      {
        "dimension": "Orders.user_id",
        "operator": userIdFilter ? "equals" : "set",
        "values": [
          `${userIdFilter}`
        ]
      }
    ]
  };

  return (
    <QueryRenderer
      query={query}
      cubejsApi={cubejsApi}
      render={({ resultSet }) => {
        if (!resultSet) {
          return <div className="loader"/>;
        }
        return (
          <div className={classes.root}>
            <OrdersToolbar setUserIdFilter={setUserIdFilter}
                           setOrder={setOrder}
                           setLimit={setLimit}
                           limit={limit}
            />
            <div className={classes.content}>
              <OrdersTable orders={resultSet.tablePivot()}/>
            </div>
          </div>
        );
      }}
    />
  );
};

export default OrderList;
