/* globals window */
import { ApolloClient } from "apollo-client";
import { InMemoryCache } from "apollo-cache-inmemory";
import gql from "graphql-tag";

const cache = new InMemoryCache();
const defaultDashboardItems = [];

const getDashboardItems = () =>
  JSON.parse(window.localStorage.getItem("dashboardItems")) ||
  defaultDashboardItems;

const setDashboardItems = items =>
  window.localStorage.setItem("dashboardItems", JSON.stringify(items));

const toApolloItem = (i, index) => ({
  ...i,
  id: index + 1,
  __typename: "DashboardItem",
  vizState: { ...i.vizState, __typename: "VizState" }
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
          layout: {
            x: 0,
            y: 0,
            w: 4,
            h: 8
          }
        };
        dashboardItems.push(item);
        setDashboardItems(dashboardItems);
        return toApolloItem(item, dashboardItems.length - 1);
      },
      updateDashboardItem: (_, { id, ...item }) => {
        const dashboardItems = getDashboardItems();
        item = Object.keys(item)
          .filter(k => !!item[k])
          .map(k => ({
            [k]: item[k]
          }))
          .reduce((a, b) => ({ ...a, ...b }), {});
        dashboardItems[id - 1] = { ...dashboardItems[id - 1], ...item };
        console.log(dashboardItems);
        setDashboardItems(dashboardItems);
        return toApolloItem(dashboardItems[id - 1], id - 1);
      },
      removeDashboardItem: (_, { id }) => {
        const dashboardItems = getDashboardItems();
        const [removedItem] = dashboardItems.splice(id - 1, 1);
        setDashboardItems(dashboardItems);
        return toApolloItem(removedItem, id - 1);
      }
    },
    Dashboard: {
      items(dashboard, variables) {
        const { id } = variables || {};
        const dashboardItems = getDashboardItems();
        return dashboardItems
          .filter((i, index) => (id ? index === id - 1 : true))
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
