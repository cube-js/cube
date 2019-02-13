import React from 'react';
import PropTypes from 'prop-types';
import { withStyles } from '@material-ui/core/styles';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import Paper from '@material-ui/core/Paper';

const styles = {
  root: {
    width: '100%',
    overflowX: 'auto',
  }
};

function TableChart(props) {
  const { classes, resultSet } = props;

  return (
    <Paper className={classes.root}>
      <Table className={classes.table}>
        <TableHead>
          <TableRow>
            <TableCell>Event</TableCell>
            <TableCell align="right">Count</TableCell>
            <TableCell align="right">Uniq Count</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {resultSet.pivot().map((rows, i) => (
            <TableRow key={i}>
              { rows.xValues.map((r, i) => (
                <TableCell key={i}  component="th"> {r} </TableCell>
              ))}
              { rows.yValuesArray.map((r, i) => (
                <TableCell key={i} align="right">{r[1]}</TableCell>
              ))}
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </Paper>
  );
}

TableChart.propTypes = {
  classes: PropTypes.object.isRequired,
};

export default withStyles(styles)(TableChart);
