import React, { useState } from "react";
import { useHistory } from "react-router-dom";
import clsx from "clsx";
import PropTypes from "prop-types";
import moment from "moment";
import PerfectScrollbar from "react-perfect-scrollbar";
import { makeStyles } from "@material-ui/styles";
import KeyboardArrowUpIcon from '@material-ui/icons/KeyboardArrowUp';
import KeyboardArrowDownIcon from '@material-ui/icons/KeyboardArrowDown';
import {
  CardActions,
  CardContent,
  Checkbox,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  TablePagination, Typography
} from "@material-ui/core";

import StatusBullet from "../../../../components/StatusBullet/StatusBullet";
import palette from "../../../../theme/palette";
import CustomCard from "../../../../components/CustomCard";

const useStyles = makeStyles(theme => ({
  root: {
    padding: 0
  },
  content: {
    padding: 0
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
  tableRow: {
    cursor: "pointer"
  },
  hoverable: {
    "&:hover": {
      color: `${palette.secondary.main}`,
      cursor: `pointer`
    }
  },
  arrow: {
    fontSize: 10,
    display: 'inline-block',
    padding: '0 4px'
  }
}));

const statusColors = {
  completed: "success",
  processing: "info",
  shipped: "danger"
};

const OrdersTable = props => {
  const history = useHistory();

  function handleClick(str) {
    history.push(str);
  }

  const { className, orders, sorting, setSorting, ...rest } = props;

  const classes = useStyles();

  const [selectedOrders, setSelectedOrders] = useState([]);
  const [rowsPerPage, setRowsPerPage] = useState(10);
  const [page, setPage] = useState(0);

  const tableHeaders = [
    {
      text: 'User id',
      value: 'Orders.user_id'
    },
    {
      text: 'User city',
      value: 'Users.city'
    },
    {
      text: 'User company',
      value: 'Users.company'
    },
    {
      text: 'Product id',
      value: 'Orders.product_id'
    },
    {
      text: 'Order price',
      value: 'LineItems.item_price'
    },
    {
      text: 'Status',
      value: 'Orders.status'
    },
    {
      text: 'Created at',
      value: 'Orders.createdAt'
    },
  ];

  const handleSelectAll = event => {
    const { orders } = props;

    let selectedOrders;

    if (event.target.checked) {
      selectedOrders = orders.map(order => order["Orders.order_id"]);
    } else {
      selectedOrders = [];
    }

    setSelectedOrders(selectedOrders);
  };

  const handleSelectOne = (event, id) => {
    const selectedIndex = selectedOrders.indexOf(id);
    let newSelectedOrders = [];

    if (selectedIndex === -1) {
      newSelectedOrders = newSelectedOrders.concat(selectedOrders, id);
    } else if (selectedIndex === 0) {
      newSelectedOrders = newSelectedOrders.concat(selectedOrders.slice(1));
    } else if (selectedIndex === selectedOrders.length - 1) {
      newSelectedOrders = newSelectedOrders.concat(selectedOrders.slice(0, -1));
    } else if (selectedIndex > 0) {
      newSelectedOrders = newSelectedOrders.concat(
        selectedOrders.slice(0, selectedIndex),
        selectedOrders.slice(selectedIndex + 1)
      );
    }

    setSelectedOrders(newSelectedOrders);
  };

  const handlePageChange = (event, page) => {
    setPage(page);
  };

  const handleRowsPerPageChange = event => {
    setRowsPerPage(event.target.value);
  };
  const handleSetSorting = str => {
    setSorting([str, sorting[1] === "desc" ? "asc" : "desc"]);
  };

  return (
    <CustomCard
      {...rest}
      padding={"0"}
      className={clsx(classes.root, className)}
    >
      <CardContent className={classes.content}>
        <PerfectScrollbar>
          <div className={classes.inner}>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell padding="checkbox">
                    <Checkbox
                      checked={selectedOrders.length === orders.length}
                      color="primary"
                      indeterminate={
                        selectedOrders.length > 0 &&
                        selectedOrders.length < orders.length
                      }
                      onChange={handleSelectAll}
                    />
                  </TableCell>
                  {tableHeaders.map((item) => (
                    <TableCell key={item.value} className={classes.hoverable}
                               onClick={() => {handleSetSorting(`${item.value}`);}}
                    >
                      <span>{item.text}</span>
                      <Typography
                        className={classes.arrow}
                        variant="body2"
                        component="span"
                      >
                        {(sorting[0] === item.value && sorting[1] === 'desc') ? <KeyboardArrowUpIcon/> : <KeyboardArrowDownIcon/>}
                      </Typography>
                    </TableCell>
                  ))}
                </TableRow>
              </TableHead>
              <TableBody>
                {orders.slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage).map(obj => (
                  <TableRow
                    className={classes.tableRow}
                    hover
                    onClick={() => handleClick(`/user/${obj["Orders.user_id"]}`)}
                    key={obj["Orders.user_id"] + obj["Orders.product_id"] + Math.random()}
                    selected={selectedOrders.indexOf(obj["Orders.order_id"]) !== -1}
                  >
                    <TableCell padding="checkbox">
                      <Checkbox
                        checked={selectedOrders.indexOf(obj["Orders.order_id"]) !== -1}
                        color="primary"
                        onChange={event => handleSelectOne(event, obj["Orders.order_id"])}
                        value="true"
                      />
                    </TableCell>
                    <TableCell>
                      {obj["Orders.user_id"]}
                    </TableCell>
                    <TableCell>
                      {obj["Users.city"]}
                    </TableCell>
                    <TableCell>
                      {obj["Users.company"]}
                    </TableCell>
                    <TableCell>
                      {obj["Orders.product_id"]}
                    </TableCell>
                    <TableCell>
                      {"$ " + obj["LineItems.item_price"]}
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
    </CustomCard>
  );
};

OrdersTable.propTypes = {
  className: PropTypes.string,
  orders: PropTypes.array.isRequired
};

export default OrdersTable;
