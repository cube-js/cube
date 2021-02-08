import gql from 'graphql-tag';
export const GET_DASHBOARD_ITEMS = gql`
  query GetDashboardItems {
    dashboardItems {
      id
      layout
      vizState
      name
      type
    }
  }
`;
export const GET_DASHBOARD_ITEM = gql`
  query GetDashboardItem($id: String!) {
    dashboardItem(id: $id) {
      id
      layout
      vizState
      name
      type
    }
  }
`;
