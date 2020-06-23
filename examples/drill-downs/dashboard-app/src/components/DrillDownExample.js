import React, { useEffect, useState } from "react";
import { makeStyles } from '@material-ui/core/styles';
import { useCubeQuery } from "@cubejs-client/react";
import moment from "moment";
import {
  BarChart,
  Bar,
  ResponsiveContainer,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip as RechartsTooltip,
  Legend
} from "recharts";
import Modal from '@material-ui/core/Modal';

import Fade from '@material-ui/core/Fade';
import Paper from '@material-ui/core/Paper';
import Table from '@material-ui/core/Table';
import TableCell from '@material-ui/core/TableCell';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import TableContainer from '@material-ui/core/TableContainer';

const dateFormatter = item => moment(item).format("MMM DD");

const useStyles = makeStyles((theme) => ({
  modal: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
  },
  paper: {
    backgroundColor: theme.palette.background.paper,
    border: '2px solid #000',
    boxShadow: theme.shadows[5],
    padding: theme.spacing(2, 4, 3),
    width: 700
  },
}));

const colors = ["#FF6492", "#141446", "#7A77FF"];

const query = {
  measures: ["Orders.count"],
  dimensions: ["Orders.status"],
  timeDimensions: [{
    dimension: "Orders.createdAt",
    granularity: "day",
    dateRange: "last 30 days"
  }]
};

const DrillDownExample = () => {
  const classes = useStyles();
  const [drillDownQuery, setDrillDownQuery] = useState();
  const [open, setOpen] = React.useState(false);
  const [activeId, setActiveId] = useState(null);

  const handleClose = () => {
    setOpen(false);
  };

  const { resultSet } = useCubeQuery(query);
  const drillDownResponse = useCubeQuery(
    drillDownQuery,
    {
      skip: !drillDownQuery
    }
  );

  const drillDownData = () => (
    (drillDownResponse.resultSet && drillDownResponse.resultSet.tablePivot()) || []
  )

  if (!resultSet) {
    return <p>Loading...</p>;
  }

  const handleBarClick = (event, yValues) => {
    if (event.xValues != null) {
      setDrillDownQuery(
        resultSet.drillDown(
          {
            xValues: event.xValues,
            yValues
          }
        )
      );
      setOpen(true);
    }
  };

  function Tooltip({ active, payload, label }) {
    if (active && activeId !== null) {
      return (
        <div className="tooltip" style={{ color: colors[activeId] }}>
          Drill down into {dateFormatter(label)}, {payload[activeId].name}
        </div>
      );
    }

    return null;
  }

  return (
    <>
      <ResponsiveContainer width="100%" height={300}>
        <BarChart data={resultSet.chartPivot()} cursor="pointer">
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="x" tickFormatter={dateFormatter} />
          <YAxis />
          <RechartsTooltip content={<Tooltip />} />
          <Legend />

          {resultSet.seriesNames().map(({ key, yValues }, index) => {
            return (
              <Bar
                key={key}
                dataKey={key}
                stackId="a"
                fill={colors[index]}
                onClick={event => handleBarClick(event, yValues)}
                onMouseOver={() => setActiveId(index)}
                onMouseOut={() => setActiveId(null)}
              />
            );
          })}
        </BarChart>
      </ResponsiveContainer>
      <Modal
         open={open}
         onClose={handleClose}
         className={classes.modal}
         aria-labelledby="simple-modal-title"
         aria-describedby="simple-modal-description"
       >
         <Fade in={open}>
          <div className={classes.paper}>
            { drillDownResponse.isLoading ?
              "Loading..." :
              <TableContainer component={Paper}>
                <Table>
                  <TableHead>
                    <TableRow>
                      { Object.keys(drillDownData()[0] || []).map((key) => (
                        <TableCell>{key}</TableCell>
                      ))}
                    </TableRow>
                  </TableHead>
                  { drillDownData().map((row) => (
                    <TableRow key={row["Orders.id"]}>
                      {Object.keys(row).map((key) => (
                        <TableCell> {row[key]} </TableCell>
                      ))}
                    </TableRow>
                  ))}
                </Table>
              </TableContainer>
              }
          </div>
        </Fade>
       </Modal>
    </>
  );
}

export default DrillDownExample;
