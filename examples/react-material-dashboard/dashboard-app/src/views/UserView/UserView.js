import React from "react";
import { useParams } from "react-router-dom";
import { makeStyles } from "@material-ui/styles";
import { QueryRenderer } from "@cubejs-client/react";
import cubejs from "@cubejs-client/core";
import { Grid } from "@material-ui/core";
import AccountProfile from "./components/AccountProfile";
import InfoCard from "./components/InfoCard";
import LatestSales from "./components/LatestSales"
import CircularProgress from "@material-ui/core/CircularProgress";

const cubejsApi = cubejs(process.env.REACT_APP_CUBEJS_TOKEN, {
  apiUrl: process.env.REACT_APP_API_URL
});

const useStyles = makeStyles(theme => ({
  root: {
    padding: theme.spacing(4)
  },
  info: {
    paddingLeft: theme.spacing(2),
    paddingRight: theme.spacing(2)
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
    justifyContent: 'center'
  }
}));

const UserView = () => {
  const classes = useStyles();
  let { id } = useParams();

  const query = {
    "measures": [
      "Users.count"
    ],
    "timeDimensions": [
      {
        "dimension": "Users.createdAt"
      }
    ],
    "dimensions": [
      "Orders.user_id",
      "Orders.product_id",
      "Users.first_name",
      "Users.last_name",
      "Users.gender",
      "Users.age",
      "Users.city",
      "LineItems.item_price",
      "Orders.createdAt"
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

  return (
    <QueryRenderer
      query={query}
      cubejsApi={cubejsApi}
      render={({ resultSet }) => {
        if (!resultSet) {
          return <div className={classes.loaderWrap}><CircularProgress color="secondary" /></div>;
        }
        let data = resultSet.tablePivot();
        let userData = data[0];
        let totalSales = countSales(data, 'LineItems.item_price');
        return (
          <div className={classes.root}>
            <Grid
              container
              spacing={4}
            >
              <Grid
                item
                lg={4}
                sm={6}
                xl={4}
                xs={12}
              >
                <AccountProfile
                  userFirstName={userData['Users.first_name']}
                  userLastName={userData['Users.last_name']}
                  gender={userData['Users.gender']}
                  age={userData['Users.age']}
                  city={userData['Users.city']}
                  id={id}
                />
              </Grid>
              <Grid
                item
                lg={8}
                sm={6}
                xl={4}
                xs={12}
              >
                <div className="row">
                  <Grid
                    className={classes.info}
                    item
                    lg={6}
                    sm={6}
                    xl={6}
                    xs={12}
                  >
                    <InfoCard
                      text={'ORDERS'}
                      value={data.length}
                    />
                  </Grid>
                  <Grid
                    className={classes.info}
                    item
                    lg={6}
                    sm={6}
                    xl={6}
                    xs={12}
                  >
                    <InfoCard
                      text={'TOTAL SALES'}
                      value={`$ ${totalSales.toLocaleString('ru')}`}
                    />
                  </Grid>
                </div>
                <div className={classes.sales}>
                  <LatestSales cubejsApi={cubejsApi} id={id}/>
                </div>
              </Grid>
            </Grid>
          </div>
        );
      }}
    />
  );
};

function countSales(array, key) {
  return array.reduce(function(sum, current) {
    return sum + current[key];
  }, 0);
}
export default UserView;
