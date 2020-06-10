import React from "react";
import { useParams } from "react-router-dom";
import { makeStyles } from "@material-ui/styles";
import { QueryRenderer } from "@cubejs-client/react";
import cubejs from "@cubejs-client/core";
import { Grid } from "@material-ui/core";
import AccountProfile from "./components/AccountProfile";

const cubejsApi = cubejs(process.env.REACT_APP_CUBEJS_TOKEN, {
  apiUrl: process.env.REACT_APP_API_URL
});

const useStyles = makeStyles(theme => ({
  root: {
    padding: theme.spacing(3)
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
          return <div className="loader"/>;
        }
        console.log(resultSet.tablePivot());
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
                xl={3}
                xs={12}
              >
                <AccountProfile />
                {id}
              </Grid>
              <Grid
                item
                lg={4}
                sm={6}
                xl={3}
                xs={12}
              >

              </Grid>
              <Grid
                item
                lg={4}
                sm={6}
                xl={3}
                xs={12}
              >

              </Grid>
            </Grid>
          </div>
        );
      }}
    />
  );
};

export default UserView;
