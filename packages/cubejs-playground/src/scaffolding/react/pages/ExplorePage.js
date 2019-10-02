import React, { useState } from "react";
import {
  Button, Modal, Input, Alert, Spin
} from "antd";
import { useMutation, useQuery } from "@apollo/react-hooks";
import { withRouter } from "react-router-dom";
import ExploreQueryBuilder from "../components/QueryBuilder/ExploreQueryBuilder";
import {
  GET_DASHBOARD_ITEMS,
  GET_DASHBOARD_ITEM
} from "../graphql/queries";
import {
  CREATE_DASHBOARD_ITEM,
  UPDATE_DASHBOARD_ITEM
} from "../graphql/mutations";

const ExplorePage = withRouter(({ cubejsApi, history, location }) => {
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
  const [addingToDashboard, setAddingToDashboard] = useState(false);
  const params = new URLSearchParams(location.search);
  const itemId = params.get("itemId");
  const { loading, error, data } = useQuery(GET_DASHBOARD_ITEM, {
    variables: {
      id: itemId
    },
    skip: !itemId
  });

  const [vizState, setVizState] = useState(null);
  const finalVizState =
    vizState ||
    (itemId && !loading && data && JSON.parse(data.dashboardItem.vizState)) ||
    {};
  const [titleModalVisible, setTitleModalVisible] = useState(false);
  const [title, setTitle] = useState(null);
  const finalTitle = title != null ? title
    : (itemId && !loading && data && data.dashboardItem.name) || "New Chart";

  if (loading) {
    return <Spin />;
  }

  if (error) {
    return <Alert type="error" message={error.toString()} />;
  }

  const titleModal = (
    <Modal
      key="modal"
      title="Save Chart"
      visible={titleModalVisible}
      onOk={async () => {
        setTitleModalVisible(false);
        setAddingToDashboard(true);

        try {
          await (itemId ? updateDashboardItem : addDashboardItem)({
            variables: {
              id: itemId,
              input: {
                vizState: JSON.stringify(finalVizState),
                name: finalTitle
              }
            }
          });
          history.push("/");
        } finally {
          setAddingToDashboard(false);
        }
      }}
      onCancel={() => setTitleModalVisible(false)}
    >
      <Input
        placeholder="Dashboard Item Name"
        value={finalTitle}
        onChange={e => setTitle(e.target.value)}
      />
    </Modal>
  );
  const addToDashboardButton = (
    <Button
      key="button"
      icon={itemId ? "save" : "plus"}
      size="small"
      loading={addingToDashboard}
      onClick={() => setTitleModalVisible(true)}
    >
      {itemId ? "Update" : "Add to Dashboard"}
    </Button>
  );
  return (
    <ExploreQueryBuilder
      cubejsApi={cubejsApi}
      vizState={finalVizState}
      setVizState={setVizState}
      chartExtra={[addToDashboardButton, titleModal]}
    />
  );
});
export default ExplorePage;
