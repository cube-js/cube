import React, { useState } from "react";
import { Button } from "antd";
import { useMutation, useQuery } from "@apollo/react-hooks";
import { withRouter } from 'react-router-dom';
import ExploreQueryBuilder from "./QueryBuilder/ExploreQueryBuilder";
import {
  GET_DASHBOARD_QUERY,
  ADD_DASHBOARD_ITEM,
  UPDATE_DASHBOARD_ITEM,
  GET_DASHBOARD_ITEM_QUERY
} from "./DashboardStore";

const ExplorePage = withRouter(({ cubejsApi, history, location }) => {
  const [addDashboardItem] = useMutation(ADD_DASHBOARD_ITEM, {
    refetchQueries: [
      {
        query: GET_DASHBOARD_QUERY
      }
    ]
  });

  const [updateDashboardItem] = useMutation(UPDATE_DASHBOARD_ITEM, {
    refetchQueries: [
      {
        query: GET_DASHBOARD_QUERY
      }
    ]
  });

  const [addingToDashboard, setAddingToDashboard] = useState(false);

  const params = new URLSearchParams(location.search);
  const itemId = parseInt(params.get('itemId'), 10);

  const { loading, error, data } = useQuery(GET_DASHBOARD_ITEM_QUERY, { variables: { id: itemId } });

  const [vizState, setVizState] = useState(null);

  const finalVizState = vizState || itemId && data && data.dashboard.items[0].vizState || {};

  const addToDashboardButton = (
    <Button
      icon={itemId ? 'save' : 'plus'}
      size="small"
      loading={addingToDashboard}
      onClick={async () => {
        setAddingToDashboard(true);
        try {
          await (itemId ? updateDashboardItem : addDashboardItem)({
            variables: {
              id: itemId,
              vizState: finalVizState
            }
          });
          history.push('/');
        } finally {
          setAddingToDashboard(false);
        }
      }}
    >
      {itemId ? 'Update' : 'Add to Dashboard'}
    </Button>
  );

  return (
    <ExploreQueryBuilder
      cubejsApi={cubejsApi}
      vizState={finalVizState}
      setVizState={setVizState}
      chartExtra={addToDashboardButton}
    />
  );
});

export default ExplorePage;
