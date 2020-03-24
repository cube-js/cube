import React, { useState } from "react";
import Typography from "@material-ui/core/Typography";
import TextField from '@material-ui/core/TextField';
import Button from "@material-ui/core/Button";
import FormControl from '@material-ui/core/FormControl';
import FormLabel from '@material-ui/core/FormLabel';
import { useParams } from "react-router-dom";

import { QueryBuilder } from "@cubejs-client/react";
import { Link } from "react-router-dom";
import { makeStyles } from "@material-ui/core/styles";
import { useMutation, useQuery } from "@apollo/react-hooks";
import * as Yup from "yup";
import { Formik } from "formik";

import MemberSelect from "../components/MemberSelect";
import { GET_DASHBOARD_ITEMS, GET_CUSTOM_REPORT } from "../graphql/queries";
import {
  CREATE_DASHBOARD_ITEM,
  UPDATE_DASHBOARD_ITEM
} from "../graphql/mutations";



const useStyles = makeStyles(theme => ({
  formControl: {
    marginBottom: theme.spacing(3),
    display: 'block'
  },
  formLabel: {
    marginBottom: theme.spacing(1)
  },
  button: {
    marginRight: theme.spacing(2)
  }
}));

const CustomReportsBuilderPage = ({ cubejsApi, history }) => {
  const { id } = useParams();
  const [addDashboardItem] = useMutation(CREATE_DASHBOARD_ITEM, {
    refetchQueries: [
      {
        query: GET_DASHBOARD_ITEMS
      }
    ]
  });
  const [updateDashboardItem] = useMutation(UPDATE_DASHBOARD_ITEM, {
    refetchQueries: [
      {
        query: GET_DASHBOARD_ITEMS
      }
    ]
  });
  const { loading, error, data } = useQuery(GET_CUSTOM_REPORT, {
    variables: {
      id: id
    },
    skip: !id
  });
  const classes = useStyles();
  const [title, setTitle] = useState(null);

  if (loading || error) {
    return "Loading";
  }

  return (
    <div>
      <Typography variant="h6" id="tableTitle">
        Create Custom Report
       </Typography>
        <QueryBuilder
          query={(data && data.dashboardItem.query && JSON.parse(data.dashboardItem.query)) || {}}
          wrapWithQueryRenderer={false}
          cubejsApi={cubejsApi}
          render={({
            measures, availableMeasures, updateMeasures,
            dimensions, availableDimensions, updateDimensions,
            query
          }) => (
             <Formik
              enableReinitialize
              initialValues={{ title: title || (data && data.dashboardItem.name) || "", query: query }}
              onSubmit={async values => {
                const { data } = await (id ? updateDashboardItem : addDashboardItem)({
                  variables: {
                    id: id,
                    input: {
                      query: JSON.stringify(values.query),
                      name: values.title
                    }
                  }
                });
                history.push(`/custom-reports/${id || data.createDashboardItem.id}`);
              }}
              validationSchema={Yup.object().shape({
                title: Yup.string().required("Required"),
                query: Yup.object().shape({
                  measures: Yup.array().min(1).required(),
                  dimensions: Yup.array().min(1).required()
                })
              })}
            >
              {props => {
                const {
                  values,
                  touched,
                  errors,
                  isSubmitting,
                  handleChange,
                  handleBlur,
                  handleSubmit,
                } = props;
                return (
                  <form>
                    <FormControl component="fieldset" className={classes.formControl}>
                      <TextField
                        id="title"
                        onChange={(event) => {
                          // because of the enableReinitialize
                          setTitle(event.target.value);
                          handleChange(event);
                        }}
                        onBlur={handleBlur}
                        label="Title"
                        value={values.title}
                        error={errors.title && touched.title}
                      />
                    </FormControl>
                    <FormControl component="fieldset" className={classes.formControl}>
                      <FormLabel
                        error={touched.query && errors.query && errors.query.measures }
                        component="legend" className={classes.formLabel}>Metrics</FormLabel>
                      {measures.map(measure =>
                        <MemberSelect
                          onSelect={updateMeasures.update}
                          member={measure}
                          availableMembers={availableMeasures}
                          onRemove={updateMeasures.remove}
                        />
                      )}
                      <MemberSelect
                        title="metric"
                        onSelect={updateMeasures.add}
                        availableMembers={availableMeasures}
                      />
                    </FormControl>
                    <FormControl component="fieldset" className={classes.formControl}>
                      <FormLabel
                        error={touched.query && errors.query && errors.query.dimensions }
                        component="legend"
                        className={classes.formLabel}
                      >
                          Dimensions
                      </FormLabel>
                      {dimensions.map(dimension =>
                        <MemberSelect
                          onSelect={updateDimensions.update}
                          member={dimension}
                          availableMembers={availableDimensions}
                          onRemove={updateDimensions.remove}
                        />
                      )}
                      <MemberSelect
                        title="dimension"
                        onSelect={updateDimensions.add}
                        availableMembers={availableDimensions}
                      />
                    </FormControl>
                    <div>
                      <Button
                        className={classes.button}
                        variant="contained"
                        color="primary"
                        onClick={handleSubmit}
                        disabled={isSubmitting}
                      >
                        Save
                      </Button>
                      <Button
                        className={classes.button}
                        variant="contained"
                        component={Link}
                        to="/custom-reports-overview">
                        Cancel
                      </Button>
                    </div>
                  </form>
                );
              }}
            </Formik>
          )}
        />
      </div>
  )
};

export default CustomReportsBuilderPage;
