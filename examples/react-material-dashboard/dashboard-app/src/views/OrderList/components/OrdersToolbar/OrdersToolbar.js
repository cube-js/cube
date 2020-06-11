import "date-fns";
import React from "react";
import PropTypes from "prop-types";
import clsx from "clsx";
import { makeStyles } from "@material-ui/styles";
import Grid from "@material-ui/core/Grid";
import FormControl from "@material-ui/core/FormControl";
import InputLabel from "@material-ui/core/InputLabel";
import Select from "@material-ui/core/Select";
import MenuItem from "@material-ui/core/MenuItem";
import Slider from "@material-ui/core/Slider";
import Typography from "@material-ui/core/Typography";
import DateFnsUtils from "@date-io/date-fns";
import {
  MuiPickersUtilsProvider,
  KeyboardDatePicker
} from "@material-ui/pickers";
import SearchInput from "../../../../components/SearchInput";

const useStyles = makeStyles(theme => ({
  root: {},
  row: {
    display: "flex",
    alignItems: "baseline",
    justifyContent: "space-between",
    // flexWrap: 'wrap',
    marginTop: theme.spacing(1)
  },
  spacer: {
    flexGrow: 1
  },
  importButton: {
    marginRight: theme.spacing(1)
  },
  exportButton: {
    marginRight: theme.spacing(1)
  },
  searchInput: {
    marginRight: theme.spacing(1)
  },
  formControl: {
    margin: 25,
    fullWidth: true,
    display: "flex",
    wrap: "nowrap"
  }
}));

function valuetext(value) {
  return `${value} orders`;
}

const OrdersToolbar = props => {
  const { className,
    orderFilter,
    setOrder,
    limit,
    setLimit,
    userIdFilter,
    setUserIdFilter,
    startDate,
    setStartDate,
    finishDate,
    setFinishDate,
    ...rest } = props;
  const [innerSearchState, setInnerSearchState] = React.useState("");

  const classes = useStyles();

  const changeSortingHandler = (event) => {
    setOrder(event.target.value);
  };
  const changeLimit = (event, value) => {
    setLimit(value);
  };
  const handleDateChange = (date) => {
    setStartDate(date);
  };
  const handleDateChangeFinish = (date) => {
    setFinishDate(date);
  };
  const handleKeyPress = (event) => {
    if (event.key === "Enter") {
      setUserIdFilter(innerSearchState);
    }
  };

  return (
    <div
      {...rest}
      className={clsx(classes.root, className)}
    >
      <div className={classes.row}>
        <Grid
          item
          lg={4}
          sm={6}
          xl={3}
          xs={12}
          m={2}
        >
          <SearchInput
            value={innerSearchState}
            type="number"
            className={classes.searchInput}
            placeholder="Search user by id"
            onChange={(e) => {
              setInnerSearchState(e.target.value);
            }}
            onKeyPress={(e) => {
              handleKeyPress(e);
            }}
          />
        </Grid>
        <Grid
          item
          lg={4}
          sm={6}
          xl={3}
          xs={12}
          m={2}
        >
          <FormControl className={classes.formControl}>
            <InputLabel id="sorting-select-label">Set sorting</InputLabel>
            <Select
              labelId="sorting-select-label"
              id="sorting-select"
              value={orderFilter}
              autoWidth
              onChange={(e) => {
                changeSortingHandler(e);
              }}
            >
              <MenuItem value={"Orders.createdAt"}>Sort by date</MenuItem>
              <MenuItem value={"Orders.order_id"}>Sort by order id</MenuItem>
              <MenuItem value={"Orders.user_id"}>Sort by user id</MenuItem>
            </Select>
          </FormControl>
        </Grid>
        <Grid
          item
          lg={3}
          sm={6}
          xl={3}
          xs={12}
          m={2}
        >
          <MuiPickersUtilsProvider utils={DateFnsUtils}>
            <Grid container justify="space-around">
              <KeyboardDatePicker
                margin="normal"
                id="date-picker-dialog"
                label="Start Date"
                format="MM/dd/yyyy"
                value={startDate}
                onChange={handleDateChange}
                KeyboardButtonProps={{
                  "aria-label": "change date"
                }}
              />
            </Grid>
          </MuiPickersUtilsProvider>
        </Grid>
        <Grid
          item
          lg={3}
          sm={6}
          xl={3}
          xs={12}
          m={2}
        >
          <MuiPickersUtilsProvider utils={DateFnsUtils}>
            <Grid container justify="space-around">
              <KeyboardDatePicker
                margin="normal"
                id="date-picker-dialog-finish"
                label="Finish Date"
                format="MM/dd/yyyy"
                value={finishDate}
                onChange={handleDateChangeFinish}
                KeyboardButtonProps={{
                  "aria-label": "change date"
                }}
              />
            </Grid>
          </MuiPickersUtilsProvider>
        </Grid>
      </div>
      <div className={classes.row}>
        <Grid
          item
          lg={3}
          sm={6}
          xl={3}
          xs={12}
          m={2}
        >
          <Typography id="discrete-slider" gutterBottom>
            Set orders limit
          </Typography>
          <Slider
            defaultValue={limit}
            getAriaValueText={valuetext}
            aria-labelledby="discrete-slider"
            valueLabelDisplay="auto"
            step={100}
            marks
            min={100}
            max={1000}
            onChangeCommitted={(e, value) => {
              changeLimit(e, value);
            }} // for example fetching new data=
          />
        </Grid>
      </div>
    </div>
  );
};

OrdersToolbar.propTypes = {
  className: PropTypes.string
};

export default OrdersToolbar;
