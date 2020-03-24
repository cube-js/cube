import gql from "graphql-tag";

export const GET_DASHBOARD_ITEMS = gql`
  query GetDashboardItems {
    dashboardItems {
      id
      query
      name
      createdAt
    }
  }
`;

export const GET_CUSTOM_REPORT = gql`
  query GetDashboardItem($id: String!) {
    dashboardItem(id: $id) {
      id
      query
      name
      createdAt
    }
  }
`;
