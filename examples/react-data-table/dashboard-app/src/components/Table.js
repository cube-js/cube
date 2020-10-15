import React, { useState } from "react";
import clsx from "clsx";
import PropTypes from "prop-types";
import moment from "moment";
import PerfectScrollbar from "react-perfect-scrollbar";
import { makeStyles } from "@material-ui/styles";
import Typography from "@material-ui/core/Typography";
import KeyboardArrowUpIcon from "@material-ui/icons/KeyboardArrowUp";
import KeyboardArrowDownIcon from "@material-ui/icons/KeyboardArrowDown";
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
  TablePagination,
} from "@material-ui/core";

import StatusBullet from "./StatusBullet";

const statusColors = {
  completed: "success",
  processing: "info",
  shipped: "danger",
};

const useStyles = makeStyles(theme => ({
  root: {
    padding: 0,
  },
  content: {
    padding: 0,
  },
  inner: {
    minWidth: 1050,
  },
  nameContainer: {
    display: "flex",
    alignItems: "baseline",
  },
  status: {
    marginRight: 15,
  },
  actions: {
    justifyContent: "flex-end",
  },
  tableRow: {
    padding: '0 5px',
    cursor: "pointer",
    '.MuiTableRow-root.MuiTableRow-hover&:hover': {},
  },
  hoverable: {
    "&:hover": {
      cursor: `pointer`,
    },
  },
  arrow: {
    fontSize: 10,
    position: "absolute",
  },
}));

const TableComponent = props => {
  const { className, sorting, setSorting, query, cubejsApi, ...rest } = props;

  const classes = useStyles();

  const [rowsPerPage, setRowsPerPage] = useState(10);
  const [page, setPage] = useState(0);

  const tableHeaders = [
    { text: "Order id", value: "Orders.id" },
    { text: "Orders size", value: "Orders.size" },
    { text: "Full Name", value: "Users.fullName" },
    { text: "User city", value: "Users.city" },
    { text: "Order price", value: "Orders.price" },
    { text: "Status", value: "Orders.status" },
    { text: "Created at", value: "Orders.createdAt" },
  ];
  const { resultSet, error, isLoading } = useCubeQuery(query, { cubejsApi });
  if (isLoading) {
    return <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center' }}><CircularProgress
      color="secondary" /></div>;
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
    const handleSetSorting = str => {
      setSorting([str, sorting[1] === "desc" ? "asc" : "desc"]);
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
                <TableHead>
                  <TableRow>
                    {tableHeaders.map((item) => (
                      <TableCell key={item.value + Math.random()}
                                 className={classes.hoverable}
                                 onClick={() => {
                                   handleSetSorting(`${item.value}`);
                                 }}
                      >
                        <span>{item.text}</span>
                        <Typography
                          className={classes.arrow}
                          variant="body2"
                          component="span"
                        >
                          {(sorting[0] === item.value) ? (sorting[1] === "desc" ? <KeyboardArrowUpIcon /> :
                            <KeyboardArrowDownIcon />) : null}
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
  query: PropTypes.object.isRequired,
};

export default TableComponent;