import React, { useState } from "react";
import {
  Button
} from 'antd';
import ExploreQueryBuilder from "./QueryBuilder/ExploreQueryBuilder";
import { useMutation } from '@apollo/react-hooks';
import { GET_DASHBOARD_QUERY, ADD_DASHBOARD_ITEM } from './DashboardStore';

const ExplorePage = ({ cubejsApi }) => {
  const [addDashboardItem, { data }] = useMutation(ADD_DASHBOARD_ITEM, {
    refetchQueries: [{ query: GET_DASHBOARD_QUERY }]
  });
  const [vizState, setVizState] = useState({});

  return (
    <ExploreQueryBuilder
      cubejsApi={cubejsApi}
      vizState={vizState}
      setVizState={setVizState}
      chartExtra={
        <Button
          icon="plus"
          size="small"
          onClick={() => addDashboardItem({
            variables: {
              vizState
            }
          })}
        >
          Add to Dashboard
        </Button>
      }
    />
  )
};

export default ExplorePage;
