import React from "react";
import clsx from "clsx";
import moment from "moment";
import PerfectScrollbar from "react-perfect-scrollbar";
import PropTypes from "prop-types";
import { makeStyles } from "@material-ui/styles";
import { Link, useHistory } from "react-router-dom";
import {
  Card,
  CardActions,
  CardHeader,
  CardContent,
  Button,
  Divider,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  Tooltip,
  TableSortLabel
} from "@material-ui/core";
import ArrowRightIcon from "@material-ui/icons/ArrowRight";
import StatusBullet from "../../../../components/StatusBullet/StatusBullet";
import { QueryRenderer } from "@cubejs-client/react";

const useStyles = makeStyles(theme => ({
  root: {},
  content: {
    padding: 0
  },
  inner: {
    minWidth: 800
  },
  statusContainer: {
    display: "flex",
    alignItems: "center"
  },
  status: {
    marginRight: theme.spacing(1)
  },
  actions: {
    justifyContent: "flex-end"
  },
  tableRow: {
    cursor: 'pointer'
  }
}));

const statusColors = {
  completed: "success",
  processing: "info",
  shipped: "danger"
};

const query = {
  limit: 6,
  order: {
    [`Orders.createdAt`]: "desc"
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
  "filters": []
};

const LatestOrders = props => {
  const history = useHistory();

  function handleClick(str) {
    history.push(str);
  }
  const { className, cubejsApi, ...rest } = props;

  const classes = useStyles();

  return (
    <QueryRenderer
      query={query}
      cubejsApi={cubejsApi}
      render={({ resultSet }) => {
        if (!resultSet) {
          return <div className="loader"/>;
        }
        let orders = resultSet.tablePivot();
        return (
          <div className={classes.root}>
            <Card
              {...rest}
              className={clsx(classes.root, className)}
            >
              <CardHeader
                title="Latest Orders"
              />
              <Divider/>
              <CardContent className={classes.content}>
                <PerfectScrollbar>
                  <div className={classes.inner}>
                    <Table>
                      <TableHead>
                        <TableRow>
                          <TableCell>Order Ref</TableCell>
                          <TableCell>Customer ID</TableCell>
                          <TableCell sortDirection="desc">
                            <Tooltip
                              enterDelay={300}
                              title="Sort"
                            >
                              <TableSortLabel
                                active
                                direction="desc"
                              >
                                Date
                              </TableSortLabel>
                            </Tooltip>
                          </TableCell>
                          <TableCell>Status</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {orders.map(order => (
                          <TableRow
                            hover
                            className={classes.tableRow}
                            key={order["Orders.order_id"]}
                            onClick={() => handleClick(`/user/${order["Orders.user_id"]}`)}
                          >
                            <TableCell>{order["Orders.order_id"]}</TableCell>
                            <TableCell>
                              {order["Orders.user_id"]}
                            </TableCell>
                            <TableCell>
                              {moment(order["Orders.createdAt"]).format("DD/MM/YYYY")}
                            </TableCell>
                            <TableCell>
                              <div className={classes.statusContainer}>
                                <StatusBullet
                                  className={classes.status}
                                  color={statusColors[order["Orders.status"]]}
                                  size="sm"
                                />
                                {order.status}
                              </div>
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                </PerfectScrollbar>
              </CardContent>
              <Divider/>
              <CardActions className={classes.actions}>
                <Button
                  color="primary"
                  size="small"
                  variant="text"
                  component={Link}
                  to={"/orders"}
                >
                  View all <ArrowRightIcon/>
                </Button>
              </CardActions>
            </Card>
          </div>
        );
      }}
    />
  );
};

LatestOrders.propTypes = {
  className: PropTypes.string
};

export default LatestOrders;
