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
    padding: theme.spacing(4, 4, 3),
    margin: theme.spacing(2, 0, 0),
    borderRadius: 8
  },
  alertInfo: {
    color: "#7A77FF",
    border: "1px solid #CAC9FF",
    borderRadius: 8,
    background: "white"
  }
}));
const DashboardPage = () => {
  const classes = useStyles();
  return (
    <div className={classes.container}>
      <Alert className={classes.alertInfo} severity="info">Click on the bar to open the drill down table.</Alert>
      <div className={classes.paper}>
        <DrillDownExample />
      </div>
    </div>
  )
}

export default DashboardPage;
