import gql from "graphql-tag";

export const GET_CUSTOM_REPORTS = gql`
  query GetCustomReports {
    customReports {
      id
      query
      name
      createdAt
    }
  }
`;

export const GET_CUSTOM_REPORT = gql`
  query GetCustomReport($id: String!) {
    customReport(id: $id) {
      id
      query
      name
      createdAt
    }
  }
`;
