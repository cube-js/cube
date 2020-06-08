import React, { useState } from 'react';
import clsx from 'clsx';
import PropTypes from 'prop-types';
import moment from 'moment';
import PerfectScrollbar from 'react-perfect-scrollbar';
import { makeStyles } from '@material-ui/styles';
import {
  Card,
  CardActions,
  CardContent,
  Checkbox,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  Typography,
  TablePagination
} from '@material-ui/core';

import StatusBullet from '../../../../components/StatusBullet/StatusBullet';

const useStyles = makeStyles(theme => ({
  root: {},
  content: {
    padding: 0
  },
  inner: {
    minWidth: 1050
  },
  nameContainer: {
    display: 'flex',
    alignItems: 'center'
  },
  status: {
    marginRight: theme.spacing(2)
  },
  actions: {
    justifyContent: 'flex-end'
  }
}));

const statusColors = {
  completed: 'success',
  processing: 'info',
  shipped: 'danger'
};

const OrdersTable = props => {
  const { className, orders, ...rest } = props;

  const classes = useStyles();

  const [selectedOrders, setSelectedOrders] = useState([]);
  const [rowsPerPage, setRowsPerPage] = useState(10);
  const [page, setPage] = useState(0);

  const handleSelectAll = event => {
    const { orders } = props;

    let selectedOrders;

    if (event.target.checked) {
      selectedOrders = orders.map(order => order['Orders.order_id']);
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

  return (
    <Card
      {...rest}
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
                  <TableCell>User id</TableCell>
                  <TableCell>User city</TableCell>
                  <TableCell>User company</TableCell>
                  <TableCell>Product id</TableCell>
                  <TableCell>Order id</TableCell>
                  <TableCell>Status</TableCell>
                  <TableCell>Created at</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {orders.slice(page*rowsPerPage, page*rowsPerPage+rowsPerPage).map(obj => (
                  <TableRow
                    className={classes.tableRow}
                    hover
                    key={obj['Orders.order_id']}
                    selected={selectedOrders.indexOf(obj['Orders.order_id']) !== -1}
                  >
                    <TableCell padding="checkbox">
                      <Checkbox
                        checked={selectedOrders.indexOf(obj['Orders.order_id']) !== -1}
                        color="primary"
                        onChange={event => handleSelectOne(event, obj['Orders.order_id'])}
                        value="true"
                      />
                    </TableCell>
                    <TableCell>
                      <div className={classes.nameContainer}>
                        <Typography variant="body1">{obj['Orders.user_id']}</Typography>
                      </div>
                    </TableCell>
                    <TableCell>
                      {obj['Users.city']}
                    </TableCell>
                    <TableCell>
                      {obj['Users.company']}
                    </TableCell>
                    <TableCell>
                      {obj['Orders.product_id']}
                    </TableCell>
                    <TableCell>
                      {obj['Orders.order_id']}
                    </TableCell>
                    <TableCell>
                      <StatusBullet
                        className={classes.status}
                        color={statusColors[obj['Orders.status']]}
                        size="sm"
                      />
                      {obj['Orders.status']}
                    </TableCell>
                    <TableCell>
                      {moment(obj['Orders.createdAt']).format('DD/MM/YYYY')}
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
};

OrdersTable.propTypes = {
  className: PropTypes.string,
  orders: PropTypes.array.isRequired
};

export default OrdersTable;
