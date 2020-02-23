import React from "react";
import Grid from "@material-ui/core/Grid";
import Paper from "@material-ui/core/Paper";
import Typography from "@material-ui/core/Typography";
import Table from "@material-ui/core/Table";
import TableBody from "@material-ui/core/TableBody";
import TableCell from "@material-ui/core/TableCell";
import TableHead from "@material-ui/core/TableHead";
import TableRow from "@material-ui/core/TableRow";
import Button from "@material-ui/core/Button";

import { Link } from "react-router-dom";
import { useQuery, useMutation } from "@apollo/react-hooks";
import { GET_DASHBOARD_ITEMS } from "../graphql/queries";
import { DELETE_CUSTOM_REPORT } from "../graphql/mutations";

import DotsMenu from "../components/DotsMenu";

const CustomReportsOverviewPage = () => {
  const [removeCustomReport] = useMutation(DELETE_CUSTOM_REPORT, {
    refetchQueries: [
      {
        query: GET_DASHBOARD_ITEMS
      }
    ]
  });
  const { loading, error, data } = useQuery(GET_DASHBOARD_ITEMS);
  return (
    <Grid container spacing={3}>
      <Grid item>
        <Typography variant="h6" id="tableTitle">
          Custom Reports
         </Typography>
      </Grid>
      <Grid item>
        <Button component={Link} to="/custom-reports-builder" variant="contained" color="primary">
          + New Custom Report
        </Button>
      </Grid>
     <Grid item xs={12}>
        <Paper>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell key="title">Title</TableCell>
                <TableCell key="creation-date">Creation Date</TableCell>
                <TableCell key="actions"></TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              { data && data.dashboardItems && data.dashboardItems.map(report => (
                <TableRow>
                  <TableCell key="title">
                    <Link to={`/custom-reports/${report.id}`}>
                      {report.name}
                    </Link>
                  </TableCell>
                  <TableCell key="creation-date">Creation Date</TableCell>
                  <TableCell key="actions">
                    <DotsMenu
                      options={{
                        "Delete": () => {
                          removeCustomReport({
                            variables: {
                              id: report.id
                            }
                          });
                        }
                      }}
                    />
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </Paper>
      </Grid>
    </Grid>
  )
};

export default CustomReportsOverviewPage;
