import React from 'react';
import { useParams } from 'react-router-dom';
import { makeStyles } from '@material-ui/styles';
import { useCubeQuery } from '@cubejs-client/react';
import { Grid } from '@material-ui/core';
import AccountProfile from '../components/AccountProfile';
import BarChart from '../components/BarChart';
import CircularProgress from '@material-ui/core/CircularProgress';
import UserSearch from '../components/UserSearch';
import KPIChart from '../components/KPIChart';

const useStyles = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(4),
  },
  row: {
    display: 'flex',
    margin: '0 -15px',
  },
  info: {
    paddingLeft: theme.spacing(2),
    paddingRight: theme.spacing(2),
  },
  sales: {
    marginTop: theme.spacing(4),
  },
  loaderWrap: {
    width: '100%',
    height: '100%',
    minHeight: 'calc(100vh - 64px)',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
  },
}));

const UsersPage = (props) => {
  const classes = useStyles();
  let { id } = useParams();
  const query = {
    measures: ['Users.count'],
    timeDimensions: [
      {
        dimension: 'Users.createdAt',
      },
    ],
    dimensions: [
      'Users.id',
      'Products.id',
      'Users.firstName',
      'Users.lastName',
      'Users.gender',
      'Users.age',
      'Users.city',
      'LineItems.itemPrice',
      'Orders.createdAt',
    ],
    filters: [
      {
        dimension: 'Users.id',
        operator: 'equals',
        values: [`${id}`],
      },
    ],
  };
  const barChartQuery = {
    measures: ['Orders.count'],
    timeDimensions: [
      {
        dimension: 'Orders.createdAt',
        granularity: 'month',
        dateRange: 'This week',
      },
    ],
    dimensions: ['Orders.status'],
    filters: [
      {
        dimension: 'Users.id',
        operator: 'equals',
        values: [id],
      },
    ],
  };
  const cards = [
    {
      title: 'ORDERS',
      query: {
        measures: ['Orders.count'],
        filters: [
          {
            dimension: 'Users.id',
            operator: 'equals',
            values: [`${id}`],
          },
        ],
      },
      duration: 1.25,
    },
    {
      title: 'TOTAL SALES',
      query: {
        measures: ['LineItems.price'],
        filters: [
          {
            dimension: 'Users.id',
            operator: 'equals',
            values: [`${id}`],
          },
        ],
      },
      duration: 1.5,
    },
  ];

  const { resultSet, error, isLoading } = useCubeQuery(query);
  if (isLoading) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <CircularProgress color="secondary" />
      </div>
    );
  }
  if (error) {
    return <pre>{error.toString()}</pre>;
  }
  if (!resultSet) {
    return null;
  }
  if (resultSet) {
    let data = resultSet.tablePivot();
    let userData = data[0];
    return (
      <div className={classes.root}>
        <Grid container spacing={4}>
          <Grid item lg={4} sm={6} xl={4} xs={12}>
            <UserSearch />
            <AccountProfile
              userFirstName={userData['Users.firstName']}
              userLastName={userData['Users.lastName']}
              gender={userData['Users.gender']}
              age={userData['Users.age']}
              city={userData['Users.city']}
              id={id}
            />
          </Grid>
          <Grid item lg={8} sm={6} xl={4} xs={12}>
            <div className={classes.row}>
              {cards.map((item, index) => {
                return (
                  <Grid className={classes.info} key={item.title + index} item lg={6} sm={6} xl={6} xs={12}>
                    <KPIChart {...item} />
                  </Grid>
                );
              })}
            </div>
            <div className={classes.sales}>
              <BarChart query={barChartQuery} dates={['This year', 'Last year']} />
            </div>
          </Grid>
        </Grid>
      </div>
    );
  }
};

export default UsersPage;
