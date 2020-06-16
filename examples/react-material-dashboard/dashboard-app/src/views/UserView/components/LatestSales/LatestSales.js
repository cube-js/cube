import React from "react";
import clsx from "clsx";
import PropTypes from "prop-types";
import { Bar } from "react-chartjs-2";
import { makeStyles } from "@material-ui/styles";
import {
  Card,
  CardHeader,
  CardContent,
  Divider,
  Button
} from "@material-ui/core";
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
  const { className, cubejsApi, id, ...rest } = props;

  const classes = useStyles();

  const [anchorEl, setAnchorEl] = React.useState(null);

  const [dateRange, setDateRange] = React.useState('This year');

  const query = {
    "measures": [
      "Orders.count"
    ],
    "timeDimensions": [
      {
        "dimension": "Orders.createdAt",
        "granularity": "day",
        "dateRange": `${dateRange}`
      }
    ],
    "dimensions": [
      "LineItems.item_price"
    ],
    "filters": [
      {
        "dimension": "Orders.user_id",
        "operator": "equals",
        "values": [
          `${id}`
        ]
      }
    ]
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
        let prepareData = resultSet.tablePivot();
        let data = {
          labels: prepareData.map((obj) => {
            return moment(obj['Orders.createdAt.day']).format('DD/MM/YYYY')
          }),
          datasets: [
            {
              label: "Purchase price",
              backgroundColor: palette.secondary.main,
              data: prepareData.map((obj) => {
                return obj['LineItems.item_price'];
              }),
            },
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
                    onClose={() => handleClose(dateRange)}
                  >
                    <MenuItem onClick={() => handleClose('This year')}>This year</MenuItem>
                    <MenuItem onClick={() => handleClose('Last year')}>Last year</MenuItem>
                  </Menu>
                </div>
              }
              title="User's latest buys"
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
