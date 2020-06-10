import React from "react";
import clsx from "clsx";
import PropTypes from "prop-types";
import { Bar } from "react-chartjs-2";
import { makeStyles } from "@material-ui/styles";
import {
  Card,
  CardHeader,
  CardContent,
  CardActions,
  Divider,
  Button
} from "@material-ui/core";
import ArrowRightIcon from "@material-ui/icons/ArrowRight";
import ArrowDropDownIcon from "@material-ui/icons/ArrowDropDown"

import { options } from "./chart";
import { QueryRenderer } from "@cubejs-client/react";
import palette from "../../../../theme/palette";
import moment from "moment";
import Menu from "@material-ui/core/Menu";
import MenuItem from "@material-ui/core/MenuItem";

const useStyles = makeStyles(() => ({
  root: {},
  chartContainer: {
    height: 400,
    position: "relative"
  },
  actions: {
    justifyContent: "flex-end"
  }
}));


const LatestSales = props => {
  const { className, cubejsApi, ...rest } = props;

  const classes = useStyles();

  const [anchorEl, setAnchorEl] = React.useState(null);

  const [dateRange, setDateRange] = React.useState('This week');

  const query = {
    "measures": [
      "Orders.ordersCount"
    ],
    "timeDimensions": [
      {
        "dimension": "Orders.createdAt",
        "granularity": "day",
        "dateRange": `${dateRange}`
      }
    ],
    "dimensions": [
      "Orders.status"
    ],
    "filters": []
  };

  const handleClick = (event) => {
    setAnchorEl(event.currentTarget);
  };

  const handleClose = (date) => {
    setDateRange(date);
    setAnchorEl(null);
  };

  return (
    <QueryRenderer
      query={query}
      cubejsApi={cubejsApi}
      render={({ resultSet }) => {
        if (!resultSet) {
          return <div className="loader"/>;
        }
        let prepareData = resultSet.chartPivot();
        let data = {
          labels: prepareData.map((obj) => {
            return moment(obj.x).format('DD/MM/YYYY')
          }),
          datasets: [
            {
              label: "Shipped orders",
              backgroundColor: palette.primary.main,
              data: prepareData.map((obj) => {
                return obj['shipped, Orders.ordersCount'];
              }),
            },
            {
              label: "Processing orders",
              backgroundColor: palette.neutral,
              data: prepareData.map((obj) => {
                return obj['processing, Orders.ordersCount'];
              }),
            }
          ]
        };
        return (
          <Card
            {...rest}
            className={clsx(classes.root, className)}
          >
            <CardHeader
              action={
                <div>
                  <Button
                    size="small"
                    variant="text"
                    aria-controls="simple-menu"
                    aria-haspopup="true"
                    onClick={handleClick}
                  >
                    {dateRange} <ArrowDropDownIcon />
                  </Button>
                  <Menu
                    id="simple-menu"
                    anchorEl={anchorEl}
                    keepMounted
                    open={Boolean(anchorEl)}
                    onClose={handleClose}
                  >
                    <MenuItem onClick={() => handleClose('This week')}>This week</MenuItem>
                    <MenuItem onClick={() => handleClose('This month')}>This month</MenuItem>
                    <MenuItem onClick={() => handleClose('Last 7 days')}>Last 7 days</MenuItem>
                    <MenuItem onClick={() => handleClose('Last month')}>Last month</MenuItem>
                  </Menu>
                </div>
              }
              title="Latest Sales"
            />
            <Divider/>
            <CardContent>
              <div className={classes.chartContainer}>
                <Bar
                  data={data}
                  options={options}
                />
              </div>
            </CardContent>
            <Divider/>
            <CardActions className={classes.actions}>
              <Button
                color="primary"
                size="small"
                variant="text"
              >
                Overview <ArrowRightIcon/>
              </Button>
            </CardActions>
          </Card>
        );
        // {resultSet}
      }}
    />
  );
};

LatestSales.propTypes = {
  className: PropTypes.string
};

export default LatestSales;
