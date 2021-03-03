import React from "react";
import { Grid, Typography, Card, CardContent, Container, CardActions, Button } from "@material-ui/core";
import copyToClipboard from 'copy-to-clipboard';

import ChartRenderer from "../components/ChartRenderer";
import Dashboard from "../components/Dashboard";
import DashboardItem from "../components/DashboardItem";
import { useAmplify } from '../libs/amplify';

const DashboardItems = [
  {
    id: 0,
    name: "New Chart",
    vizState: {
      query: {
        measures: ["LineItems.count"],
        timeDimensions: [
          {
            dimension: "Orders.completedAt",
            granularity: "day"
          }
        ],
        order: {},
        filters: []
      },
      chartType: "line"
    }
  }
];

const DashboardPage = () => {
  const { authData } = useAmplify();

  const dashboardItem = item => (
    <Grid item xs={12} lg={6} key={item.id}>
      <DashboardItem title={item.name}>
        <ChartRenderer vizState={item.vizState} />
      </DashboardItem>
    </Grid>
  );

  const Empty = () => (
    <div
      style={{
        textAlign: "center",
        padding: 12
      }}
    >
      <Typography variant="h5" color="inherit">
        There are no charts on this dashboard. Use Playground Build to add one.
      </Typography>
    </div>
  );

  return (
    <Container maxWidth="xl">
      <Grid container justify="center">
        {authData && (
          <Grid
            item
            xl={12}
          >
            <Card>
              <CardContent>
                <Typography component="pre">
                  {JSON.stringify(authData.signInUserSession.accessToken.payload, null, 2)}
                </Typography>
              </CardContent>
              <CardActions>
                <Button
                  size="small"
                  onClick={() => copyToClipboard(JSON.stringify(authData.signInUserSession.accessToken.payload))}
                >
                  Copy Payload
                </Button>
                <Button
                  size="small"
                  onClick={() => copyToClipboard(JSON.stringify(authData.signInUserSession.accessToken.getJwtToken()))}
                >
                  Copy Access Token
                </Button>
              </CardActions>
            </Card>
          </Grid>
        )}
        {DashboardItems.length ? (
          <Dashboard>{DashboardItems.map(dashboardItem)}</Dashboard>
        ) : (
          <Empty />
        )}
      </Grid>
    </Container>
  )
};

export default DashboardPage;
