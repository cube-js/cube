import React from 'react';
import { Modal, Input } from 'antd';
import { useMutation } from '@apollo/react-hooks';
import { GET_DASHBOARD_ITEMS } from '../graphql/queries';
import {
  CREATE_DASHBOARD_ITEM,
  UPDATE_DASHBOARD_ITEM,
} from '../graphql/mutations';

const TitleModal = ({
  history,
  itemId,
  titleModalVisible,
  setTitleModalVisible,
  setAddingToDashboard,
  finalVizState,
  setTitle,
  finalTitle,
}) => {
  const [addDashboardItem] = useMutation(CREATE_DASHBOARD_ITEM, {
    refetchQueries: [
      {
        query: GET_DASHBOARD_ITEMS,
      },
    ],
  });
  const [updateDashboardItem] = useMutation(UPDATE_DASHBOARD_ITEM, {
    refetchQueries: [
      {
        query: GET_DASHBOARD_ITEMS,
      },
    ],
  });
  return (
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
                name: finalTitle,
              },
            },
          });
          history.push('/');
        } finally {
          setAddingToDashboard(false);
        }
      }}
      onCancel={() => setTitleModalVisible(false)}
    >
      <Input
        placeholder="Dashboard Item Name"
        value={finalTitle}
        onChange={(e) => setTitle(e.target.value)}
      />
    </Modal>
  );
};

export default TitleModal;
