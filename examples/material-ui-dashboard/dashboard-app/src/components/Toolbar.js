import 'date-fns';
import React from 'react';
import PropTypes from 'prop-types';
import clsx from 'clsx';
import { makeStyles } from '@material-ui/styles';
import Grid from '@material-ui/core/Grid';
import Typography from '@material-ui/core/Typography';
import Tab from '@material-ui/core/Tab';
import Tabs from '@material-ui/core/Tabs';
import withStyles from '@material-ui/core/styles/withStyles';
import palette from '../theme/palette';
import Slider from '@material-ui/core/Slider';
import DateFnsUtils from '@date-io/date-fns';
import { MuiPickersUtilsProvider, KeyboardDatePicker } from '@material-ui/pickers';

const AntTabs = withStyles({
  root: {
    borderBottom: `1px solid ${palette.primary.main}`,
  },
  indicator: {
    backgroundColor: `${palette.primary.main}`,
  },
})(Tabs);
const AntTab = withStyles((theme) => ({
  root: {
    textTransform: 'none',
    minWidth: 25,
    fontSize: 12,
    fontWeight: theme.typography.fontWeightRegular,
    marginRight: theme.spacing(0),
    color: palette.primary.dark,
    opacity: 0.6,
    '&:hover': {
      color: `${palette.primary.main}`,
      opacity: 1,
    },
    '&$selected': {
      color: `${palette.primary.main}`,
      fontWeight: theme.typography.fontWeightMedium,
      outline: 'none',
    },
    '&:focus': {
      color: `${palette.primary.main}`,
      outline: 'none',
    },
  },
  selected: {},
}))((props) => <Tab disableRipple {...props} />);
const useStyles = makeStyles((theme) => ({
  root: {},
  row: {
    marginTop: theme.spacing(1),
  },
  spacer: {
    flexGrow: 1,
  },
  importButton: {
    marginRight: theme.spacing(1),
  },
  exportButton: {
    marginRight: theme.spacing(1),
  },
  searchInput: {
    marginRight: theme.spacing(1),
  },
  formControl: {
    margin: 25,
    fullWidth: true,
    display: 'flex',
    wrap: 'nowrap',
  },
  date: {
    marginTop: 3,
  },
  range: {
    marginTop: 13,
  },
}));

const Toolbar = (props) => {
  const {
    className,
    startDate,
    setStartDate,
    finishDate,
    setFinishDate,
    priceFilter,
    setPriceFilter,
    statusFilter,
    setStatusFilter,
    tabs,
    ...rest
  } = props;
  const [tabValue, setTabValue] = React.useState(statusFilter);
  const [rangeValue, rangeSetValue] = React.useState(priceFilter);

  const classes = useStyles();

  const handleChangeTab = (e, value) => {
    setTabValue(value);
    setStatusFilter(value);
  };
  const handleDateChange = (date) => {
    setStartDate(date);
  };
  const handleDateChangeFinish = (date) => {
    setFinishDate(date);
  };
  const handleChangeRange = (event, newValue) => {
    rangeSetValue(newValue);
  };
  const setRangeFilter = (event, newValue) => {
    setPriceFilter(newValue);
  };

  return (
    <div {...rest} className={clsx(classes.root, className)}>
      <Grid container spacing={4}>
        <Grid item lg={3} sm={6} xl={3} xs={12} m={2}>
          <div className={classes}>
            <AntTabs value={tabValue} onChange={handleChangeTab} aria-label="ant example">
              {tabs.map((item) => (
                <AntTab key={item} label={item} />
              ))}
            </AntTabs>
            <Typography className={classes.padding} />
          </div>
        </Grid>
        <Grid className={classes.date} item lg={3} sm={6} xl={3} xs={12} m={2}>
          <MuiPickersUtilsProvider utils={DateFnsUtils}>
            <Grid container justify="space-around">
              <KeyboardDatePicker
                id="date-picker-dialog"
                label={<span style={{ opacity: 0.6 }}>Start Date</span>}
                format="MM/dd/yyyy"
                value={startDate}
                onChange={handleDateChange}
                KeyboardButtonProps={{
                  'aria-label': 'change date',
                }}
              />
            </Grid>
          </MuiPickersUtilsProvider>
        </Grid>
        <Grid className={classes.date} item lg={3} sm={6} xl={3} xs={12} m={2}>
          <MuiPickersUtilsProvider utils={DateFnsUtils}>
            <Grid container justify="space-around">
              <KeyboardDatePicker
                id="date-picker-dialog-finish"
                label={<span style={{ opacity: 0.6 }}>Finish Date</span>}
                format="MM/dd/yyyy"
                value={finishDate}
                onChange={handleDateChangeFinish}
                KeyboardButtonProps={{
                  'aria-label': 'change date',
                }}
              />
            </Grid>
          </MuiPickersUtilsProvider>
        </Grid>
        <Grid className={classes.range} item lg={3} sm={6} xl={3} xs={12} m={2}>
          <Typography id="range-slider">Order price range</Typography>
          <Slider
            value={rangeValue}
            onChange={handleChangeRange}
            onChangeCommitted={setRangeFilter}
            aria-labelledby="range-slider"
            valueLabelDisplay="auto"
            min={0}
            max={2000}
          />
        </Grid>
      </Grid>
    </div>
  );
};

Toolbar.propTypes = {
  className: PropTypes.string,
};

export default Toolbar;
