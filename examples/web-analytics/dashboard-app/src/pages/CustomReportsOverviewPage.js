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
import moment from "moment";

import { Link } from "react-router-dom";
import { useQuery, useMutation } from "@apollo/react-hooks";
import { GET_CUSTOM_REPORTS } from "../graphql/queries";
import { DELETE_CUSTOM_REPORT } from "../graphql/mutations";

import DotsMenu from "../components/DotsMenu";

const CustomReportsOverviewPage = ({ history }) => {
  const [removeCustomReport] = useMutation(DELETE_CUSTOM_REPORT, {
    refetchQueries: [
      {
        query: GET_CUSTOM_REPORTS
      }
    ]
  });
  const { loading, error, data } = useQuery(GET_CUSTOM_REPORTS);
  return (
    <Grid container spacing={3} justify="space-between">
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
          <Table aria-label="a dense table">
            <TableHead>
              <TableRow>
                <TableCell key="title">Title</TableCell>
                <TableCell align="right" key="creation-date">Creation Date</TableCell>
                <TableCell key="actions"></TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              { data && data.customReports && data.customReports.map(report => (
                <TableRow>
                  <TableCell key="title" component="th" scope="row">
                    <Link to={`/custom-reports/${report.id}`}>
                      {report.name}
                    </Link>
                  </TableCell>
                  <TableCell align="right" key="creation-date">
                    {moment(report.createdAt).format("MMM DD, YYYY")}
                  </TableCell>
                  <TableCell key="actions" align="right">
                    <DotsMenu
                      options={{
                        "Edit": () => history.push(`/custom-reports-builder/${report.id}`),
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
