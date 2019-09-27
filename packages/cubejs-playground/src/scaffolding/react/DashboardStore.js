/* globals window */
import { ApolloClient } from "apollo-client";
import { InMemoryCache } from "apollo-cache-inmemory";
import gql from "graphql-tag";

const cache = new InMemoryCache();
const defaultDashboardItems = [];

const getDashboardItems = () => JSON.parse(window.localStorage.getItem("dashboardItems"))
  || defaultDashboardItems;

const setDashboardItems = items => window.localStorage.setItem("dashboardItems", JSON.stringify(items));

const nextId = () => {
  const currentId = parseInt(window.localStorage.getItem("dashboardIdCounter"), 10) || 1;
  window.localStorage.setItem("dashboardIdCounter", currentId + 1);
  return currentId;
};

const toApolloItem = (i) => ({
  ...i,
  __typename: "DashboardItem"
});

export const client = new ApolloClient({
  cache,
  resolvers: {
    Query: {
      dashboard() {
        return {
          id: 1,
          name: "Main",
          description: null,
          __typename: "Dashboard"
        };
      }
    },
    Mutation: {
      addDashboardItem: (_, item) => {
        const dashboardItems = getDashboardItems();
        item = {
          ...item,
          id: nextId(),
          layout: {}
        };
        dashboardItems.push(item);
        setDashboardItems(dashboardItems);
        return toApolloItem(item);
      },
      updateDashboardItem: (_, { id, ...item }) => {
        const dashboardItems = getDashboardItems();
        item = Object.keys(item)
          .filter(k => !!item[k])
          .map(k => ({
            [k]: item[k]
          }))
          .reduce((a, b) => ({ ...a, ...b }), {});
        const index = dashboardItems.findIndex(i => i.id === id);
        dashboardItems[index] = { ...dashboardItems[index], ...item };
        setDashboardItems(dashboardItems);
        return toApolloItem(dashboardItems[index]);
      },
      removeDashboardItem: (_, { id }) => {
        const dashboardItems = getDashboardItems();
        const index = dashboardItems.findIndex(i => i.id === id);
        const [removedItem] = dashboardItems.splice(index, 1);
        setDashboardItems(dashboardItems);
        return toApolloItem(removedItem);
      }
    },
    Dashboard: {
      items(dashboard, variables) {
        const { id } = variables || {};
        const dashboardItems = getDashboardItems();
        return dashboardItems
          .filter((i) => (id ? i.id === id : true))
          .map(toApolloItem);
      }
    }
  }
});
export const GET_DASHBOARD_QUERY = gql`
  query GetDashboard {
    dashboard @client {
      id
      name
      description
      items {
        id
        layout
        vizState
        title
      }
    }
  }
`;
export const GET_DASHBOARD_ITEM_QUERY = gql`
  query GetDashboardItem($id: Object!) {
    dashboard @client {
      items(id: $id) {
        id
        layout
        vizState
        title
      }
    }
  }
`;
export const ADD_DASHBOARD_ITEM = gql`
  mutation AddDashboardItem($vizState: Object!, $title: String!) {
    addDashboardItem(vizState: $vizState, title: $title) @client {
      id
      layout
      vizState
      title
    }
  }
`;
export const UPDATE_DASHBOARD_ITEM = gql`
  mutation UpdateDashboardItem($id: Object!, $title: String, $vizState: Object, $layout: Object) {
    updateDashboardItem(id: $id, vizState: $vizState, layout: $layout, title: $title) @client {
      id
      layout
      vizState
      title
    }
  }
`;
export const REMOVE_DASHBOARD_ITEM = gql`
  mutation AddDashboardItem($id: Object!) {
    removeDashboardItem(id: $id) @client {
      id
      layout
      vizState
      title
    }
  }
`;
