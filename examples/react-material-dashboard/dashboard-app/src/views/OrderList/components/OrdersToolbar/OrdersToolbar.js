import React, {useState} from 'react';
import PropTypes from 'prop-types';
import clsx from 'clsx';
import { makeStyles } from '@material-ui/styles';
import Grid from "@material-ui/core/Grid";
import FormControl from "@material-ui/core/FormControl";
import InputLabel from "@material-ui/core/InputLabel";
import Select from "@material-ui/core/Select";
import MenuItem from "@material-ui/core/MenuItem";
import FormHelperText from "@material-ui/core/FormHelperText";
import Slider from "@material-ui/core/Slider";
import Typography from "@material-ui/core/Typography";

import SearchInput from '../../../../components/SearchInput/SearchInput';

const useStyles = makeStyles(theme => ({
  root: {},
  row: {
    height: '42px',
    display: 'flex',
    alignItems: 'center',
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
    display: 'flex',
    wrap: 'nowrap',
  },
}));
function valuetext(value) {
  return `${value}°C`;
}

const OrdersToolbar = props => {
  const { className, setUserIdFilter, setOrder, limit, setLimit, ...rest } = props;
  const [orderValue, setOrderValue] = useState('Orders.createdAt');

  const classes = useStyles();

  const searchInputHandler = (event) => {
    if (event.key === 'Enter') {
      let intValue = parseInt(event.target.value);
      if (intValue) {
        setUserIdFilter(intValue);
      }
    }
  };
  const changeSortingHandler = (event) => {
    setOrderValue(event.target.value);
    setOrder(event.target.value);
  };
  const changeLimit = (event, value) => {
    setLimit(value);
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
            className={classes.searchInput}
            placeholder="Search user by id"
            onKeyPress={(e) => (searchInputHandler(e))}
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
              value={orderValue}
              autoWidth
              onChange={(e) => {changeSortingHandler(e)}}
            >
              <MenuItem value={'Orders.createdAt'}>Sort by date</MenuItem>
              <MenuItem value={'Orders.order_id'}>Sort by order id</MenuItem>
              <MenuItem value={'Orders.user_id'}>Sort by user id</MenuItem>
            </Select>
            <FormHelperText>Please, choose the date</FormHelperText>
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
          <Typography id="discrete-slider" gutterBottom>
            Orders limit
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
            onChangeCommitted={(e, value) => {changeLimit(e, value)}} // for example fetching new data=
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
