import React, { useState, useEffect, useContext } from "react";
import { useParams } from "react-router-dom";
import { useQuery } from "@apollo/react-hooks";
import Button from "@material-ui/core/Button";
import { CubeContext } from "@cubejs-client/react";
import Grid from "@material-ui/core/Grid";
import { GET_CUSTOM_REPORT } from "../graphql/queries";
import OverTimeChart from "../components/OverTimeChart";
import DataTable from "../components/DataTable";
import Dropdown from "../components/Dropdown";
import { Link } from "react-router-dom";

const CustomReportPage = ({ withTime }) => {
  const [activeMeasure, setActiveMeasure] = useState(null);
  const [meta, setMeta] = useState(null);
  const { id } = useParams();
  const cubejsApi = useContext(CubeContext).cubejsApi;
  const { loading, error, data } = useQuery(GET_CUSTOM_REPORT, {
    variables: {
      id: id
    },
  });
  useEffect(() => {
    (async function loadMeta() {
      const meta = await cubejsApi.meta();
      setMeta(meta);
    })();
  }, []);

  if (loading || error || !meta) {
    return "Loading";
  }

  const { measures, ...query } = JSON.parse(data.customReport.query);
  const finalActiveMeasure = activeMeasure || measures[0];
  const overTimeChartQuery = {
    measures: [finalActiveMeasure],
    timeDimensions: [{
      dimension: query.timeDimensions[0].dimension,
      granularity: 'day'
    }]
  };
  const dataTableQuery = {
    ...query,
    measures: measures,
    timeDimensions: [{
      dimension: query.timeDimensions[0].dimension,
    }]
  };
  return (
    <>
      <Grid item xs={12}>
        <Grid container justify="flex-end">
          <Button component={Link} to={`/custom-reports-builder/${id}`} variant="contained">
            Edit
          </Button>
        </Grid>
      </Grid>
      <Grid item xs={12}>
        <OverTimeChart
          title={
            measures.length > 1 ?
              <Dropdown
                value={meta.resolveMember(finalActiveMeasure, "measures").shortTitle}
                options={
                  measures.reduce((out, measure) => {
                    out[meta.resolveMember(measure, "measures").shortTitle] = () => setActiveMeasure(measure)
                    return out;
                  }, {})
                }
              /> : meta.resolveMember(finalActiveMeasure, "measures").shortTitle
          }
          vizState={withTime({ query: overTimeChartQuery, chartType: 'line' })}
        />
      </Grid>
      <Grid item xs={12}>
        <DataTable query={withTime({ query: dataTableQuery, chartType: 'table' })} />
      </Grid>
    </>
  )
};

export default CustomReportPage;
