import React, { useState } from "react";
import { useParams } from "react-router-dom";
import { useQuery } from "@apollo/react-hooks";
import Grid from "@material-ui/core/Grid";
import { GET_CUSTOM_REPORT } from "../graphql/queries";
import OverTimeChart from "../components/OverTimeChart";
import DataTable from "../components/DataTable";

const CustomReportPage = ({ withTime }) => {
  const { id } = useParams();
  const { loading, error, data } = useQuery(GET_CUSTOM_REPORT, {
    variables: {
      id: id
    },
  });
  if (loading || error) {
    return "Loading";
  }

  const query = JSON.parse(data.dashboardItem.query);
  return (
    <>
      <Grid item xs={12}>
        <OverTimeChart
          title="Users Over Time"
          vizState={withTime({ query, chartType: 'line' })}
        />
      </Grid>
      <Grid item xs={12}>
        <DataTable query={query} />
      </Grid>
    </>
  )
};

export default CustomReportPage;
