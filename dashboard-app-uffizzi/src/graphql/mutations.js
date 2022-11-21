import gql from 'graphql-tag';
export const CREATE_DASHBOARD_ITEM = gql`
  mutation CreateDashboardItem($input: DashboardItemInput) {
    createDashboardItem(input: $input) {
      id
      layout
      vizState
      name
    }
  }
`;
export const UPDATE_DASHBOARD_ITEM = gql`
  mutation UpdateDashboardItem($id: String!, $input: DashboardItemInput) {
    updateDashboardItem(id: $id, input: $input) {
      id
      layout
      vizState
      name
    }
  }
`;
export const DELETE_DASHBOARD_ITEM = gql`
  mutation DeleteDashboardItem($id: String!) {
    deleteDashboardItem(id: $id) {
      id
      layout
      vizState
      name
    }
  }
`;
