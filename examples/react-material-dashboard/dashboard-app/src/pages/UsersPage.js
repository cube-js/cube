import React from 'react';
import { useParams } from 'react-router-dom';
import { makeStyles } from '@material-ui/styles';
import { useCubeQuery } from '@cubejs-client/react';
import { Grid } from '@material-ui/core';
import AccountProfile from '../components/AccountProfile';
import InfoCard from '../components/InfoCard';
import BarChart from '../components/BarChart';
import CircularProgress from '@material-ui/core/CircularProgress';
import UserSearch from '../components/UserSearch';

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
    return null
  }
  if (resultSet) {
    let data = resultSet.tablePivot();
    let userData = data[0];
    let totalSales = countSales(data, 'LineItems.itemPrice');
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
              <Grid className={classes.info} item lg={6} sm={6} xl={6} xs={12}>
                <InfoCard text={'ORDERS'} value={data.length} />
              </Grid>
              <Grid className={classes.info} item lg={6} sm={6} xl={6} xs={12}>
                <InfoCard text={'TOTAL SALES'} value={`$ ${totalSales.toLocaleString('ru')}`} />
              </Grid>
            </div>
            <div className={classes.sales}>
              <BarChart id={id} dates={['This year', 'Last year']} />
            </div>
          </Grid>
        </Grid>
      </div>
    );
  }
};

function countSales(array, key) {
  return array.reduce(function (sum, current) {
    return sum + current[key];
  }, 0);
}
export default UsersPage;
