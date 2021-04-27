import React, { useState } from 'react';
import { Alert, Button, Spin } from 'antd';
import { useQuery } from '@apollo/react-hooks';
import { withRouter } from 'react-router-dom';
import ExploreQueryBuilder from '../components/QueryBuilder/ExploreQueryBuilder';
import { GET_DASHBOARD_ITEM } from '../graphql/queries';
import TitleModal from '../components/TitleModal.js';
const ExplorePage = withRouter(({ history, location }) => {
  const [addingToDashboard, setAddingToDashboard] = useState(false);
  const params = new URLSearchParams(location.search);
  const itemId = params.get('itemId');
  const { loading, error, data } = useQuery(GET_DASHBOARD_ITEM, {
    variables: {
      id: itemId,
    },
    skip: !itemId,
  });
  const [vizState, setVizState] = useState(null);
  const finalVizState =
    vizState ||
    (itemId && !loading && data && JSON.parse(data.dashboardItem.vizState)) ||
    {};
  const [titleModalVisible, setTitleModalVisible] = useState(false);
  const [title, setTitle] = useState(null);
  const finalTitle =
    title != null
      ? title
      : (itemId && !loading && data && data.dashboardItem.name) || 'New Chart';

  if (loading) {
    return <Spin />;
  }

  if (error) {
    return <Alert type="error" message={error.toString()} />;
  }

  return (
    <div>
      <TitleModal
        history={history}
        itemId={itemId}
        titleModalVisible={titleModalVisible}
        setTitleModalVisible={setTitleModalVisible}
        setAddingToDashboard={setAddingToDashboard}
        finalVizState={finalVizState}
        setTitle={setTitle}
        finalTitle={finalTitle}
      />
      <ExploreQueryBuilder
        vizState={finalVizState}
        chartExtra={[
          <Button
            key="button"
            type="primary"
            loading={addingToDashboard}
            onClick={() => setTitleModalVisible(true)}
          >
            {itemId ? 'Update' : 'Add to Dashboard'}
          </Button>,
        ]}
        onVizStateChanged={setVizState}
      />
    </div>
  );
});
export default ExplorePage;
