import React from 'react';
import { Doughnut } from 'react-chartjs-2';
import clsx from 'clsx';
import PropTypes from 'prop-types';
import { makeStyles, useTheme } from '@material-ui/styles';
import {
  Card,
  CardHeader,
  CardContent,
  IconButton,
  Divider,
  Typography
} from '@material-ui/core';
import LaptopMacIcon from '@material-ui/icons/LaptopMac';
import PhoneIphoneIcon from '@material-ui/icons/PhoneIphone';
import RefreshIcon from '@material-ui/icons/Refresh';
import TabletMacIcon from '@material-ui/icons/TabletMac';
import cubejs from "@cubejs-client/core";
import { QueryRenderer } from "@cubejs-client/react";

const useStyles = makeStyles(theme => ({
  root: {
    height: '100%'
  },
  chartContainer: {
    position: 'relative',
    height: '300px'
  },
  stats: {
    marginTop: theme.spacing(2),
    display: 'flex',
    justifyContent: 'center'
  },
  device: {
    textAlign: 'center',
    padding: theme.spacing(1)
  },
  deviceIcon: {
    color: theme.palette.icon
  }
}));

const cubejsApi = cubejs(process.env.REACT_APP_CUBEJS_TOKEN, {
  apiUrl: process.env.REACT_APP_API_URL
});
const query = {
  "measures": [
    "Orders.count"
  ],
  "timeDimensions": [
    {
      "dimension": "Orders.createdAt"
    }
  ],
  "filters": [],
  "dimensions": [
    "Orders.status"
  ]
};

const UsersByDevice = props => {
  const { className, ...rest } = props;

  const classes = useStyles();
  const theme = useTheme();

  const data = {
    datasets: [
      {
        data: [63, 15, 22],
        backgroundColor: [
          theme.palette.primary.main,
          theme.palette.error.main,
          theme.palette.warning.main
        ],
        borderWidth: 8,
        borderColor: theme.palette.white,
        hoverBorderColor: theme.palette.white
      }
    ],
    labels: ['Desktop', 'Tablet', 'Mobile']
  };

  const options = {
    legend: {
      display: false
    },
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    cutoutPercentage: 80,
    layout: { padding: 0 },
    tooltips: {
      enabled: true,
      mode: 'index',
      intersect: false,
      borderWidth: 1,
      borderColor: theme.palette.divider,
      backgroundColor: theme.palette.white,
      titleFontColor: theme.palette.text.primary,
      bodyFontColor: theme.palette.text.secondary,
      footerFontColor: theme.palette.text.secondary
    }
  };

  const devices = [
    {
      title: 'Desktop',
      value: '63',
      icon: <LaptopMacIcon />,
      color: theme.palette.primary.main
    },
    {
      title: 'Tablet',
      value: '15',
      icon: <TabletMacIcon />,
      color: theme.palette.error.main
    },
    {
      title: 'Mobile',
      value: '23',
      icon: <PhoneIphoneIcon />,
      color: theme.palette.warning.main
    }
  ];

  return (
    <QueryRenderer
      query={query}
      cubejsApi={cubejsApi}
      render={({ resultSet }) => {
        if (!resultSet) {
          return <div className="loader" />;
        }
        let result = resultSet.tablePivot();
        console.log(result);
        return (
          <div>
            <Card
              {...rest}
              className={clsx(classes.root, className)}
            >
              <CardHeader
                action={
                  <IconButton size="small">
                    <RefreshIcon />
                  </IconButton>
                }
                title="Orders status"
              />
              <Divider />
              <CardContent>
                <div className={classes.chartContainer}>
                  <Doughnut
                    data={data}
                    options={options}
                  />
                </div>
                <div className={classes.stats}>
                  {devices.map(device => (
                    <div
                      className={classes.device}
                      key={device.title}
                    >
                      <span className={classes.deviceIcon}>{device.icon}</span>
                      <Typography variant="body1">{device.title}</Typography>
                      <Typography
                        style={{ color: device.color }}
                        variant="h2"
                      >
                        {device.value}%
                      </Typography>
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>
          </div>
        );
      }}
    />
  );
};

UsersByDevice.propTypes = {
  className: PropTypes.string
};

export default UsersByDevice;
