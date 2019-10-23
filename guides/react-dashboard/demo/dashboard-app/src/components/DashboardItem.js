import React from "react";
import { Card, Menu, Icon, Dropdown, Modal } from "antd";
import styled from 'styled-components';
import { useMutation } from "@apollo/react-hooks";
import { Link } from "react-router-dom";
import { GET_DASHBOARD_ITEMS } from "../graphql/queries";
import { DELETE_DASHBOARD_ITEM } from "../graphql/mutations";

const StyledCard = styled(Card)`
  box-shadow: 0px 2px 4px rgba(141, 149, 166, 0.1);
  border-radius: 4px;

  .ant-card-head {
    border: none;
  }
  .ant-card-body {
    padding-top: 12px;
  }
`


const DashboardItemDropdown = ({ itemId }) => {
  const [removeDashboardItem] = useMutation(DELETE_DASHBOARD_ITEM, {
    refetchQueries: [
      {
        query: GET_DASHBOARD_ITEMS
      }
    ]
  });
  const dashboardItemDropdownMenu = (
    <Menu>
      <Menu.Item>
        <Link to={`/explore?itemId=${itemId}`}>Edit</Link>
      </Menu.Item>
      <Menu.Item
        onClick={() =>
          Modal.confirm({
            title: "Are you sure you want to delete this item?",
            okText: "Yes",
            okType: "danger",
            cancelText: "No",

            onOk() {
              removeDashboardItem({
                variables: {
                  id: itemId
                }
              });
            }
          })
        }
      >
        Delete
      </Menu.Item>
    </Menu>
  );
  return (
    <Dropdown
      overlay={dashboardItemDropdownMenu}
      placement="bottomLeft"
      trigger={["click"]}
    >
      <Icon type="menu" />
    </Dropdown>
  );
};

const DashboardItem = ({ itemId, children, title }) => (
  <StyledCard
    title={title}
    bordered={false}
    style={{
      height: "100%",
      width: "100%"
    }}
    extra={<DashboardItemDropdown itemId={itemId} />}
  >
    {children}
  </StyledCard>
);

export default DashboardItem;
