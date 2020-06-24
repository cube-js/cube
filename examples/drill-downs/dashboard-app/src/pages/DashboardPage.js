import React from "react";
import DrillDownExample from "../components/DrillDownExample";
import { makeStyles } from '@material-ui/core/styles';
import Alert from '@material-ui/lab/Alert';

const useStyles = makeStyles((theme) => ({
  container: {
    padding: theme.spacing(2, 4, 3),
  },
  paper: {
    backgroundColor: theme.palette.background.paper,
    boxShadow: theme.shadows[5],
    padding: theme.spacing(2, 4, 3),
    margin: theme.spacing(2, 0, 0)
  },
}));
const DashboardPage = () => {
  const classes = useStyles();
  return (
    <div className={classes.container}>
      <Alert severity="info">Click on the bar to open the drill down table.</Alert>
      <div className={classes.paper}>
        <DrillDownExample />
      </div>
    </div>
  )
}

export default DashboardPage;
