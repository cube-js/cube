import gql from "graphql-tag";

export const CREATE_CUSTOM_REPORT = gql`
  mutation createCustomReport($input: CustomReportInput) {
    createCustomReport(input: $input) {
      id
      query
      name
    }
  }
`;

export const UPDATE_CUSTOM_REPORT = gql`
  mutation UpdateCustomReport($id: String!, $input: CustomReportInput) {
    updateCustomReport(id: $id, input: $input) {
      id
      query
      name
    }
  }
`;

export const DELETE_CUSTOM_REPORT = gql`
  mutation DeleteCustomReport($id: String!) {
    deleteCustomReport(id: $id) {
      id
      query
      name
    }
  }
`;
