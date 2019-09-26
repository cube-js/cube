/* globals window */
import { ApolloClient } from 'apollo-client';
import { InMemoryCache } from 'apollo-cache-inmemory';
import gql from 'graphql-tag';

const cache = new InMemoryCache();

const defaultDashboardItems = [];

const getDashboardItems = () => JSON.parse(window.localStorage.getItem('dashboardItems')) || defaultDashboardItems;
const setDashboardItems = (items) => window.localStorage.setItem('dashboardItems', JSON.stringify(items));

const toApolloItem = (i, index) => ({
  ...i,
  id: index + 1,
  __typename: 'DashboardItem',
  vizState: {
    ...i.vizState,
    __typename: 'VizState'
  }
});

export const client = new ApolloClient({
  cache,
  resolvers: {
    Query: {
      dashboard() {
        return {
          id: 1,
          name: 'Main',
          description: null,
          __typename: 'Dashboard'
        };
      }
    },
    Mutation: {
      addDashboardItem: (_, item, { cache }) => {
        const dashboardItems = getDashboardItems();
        item = { ...item, layout: {} };
        dashboardItems.push(item);
        setDashboardItems(dashboardItems);
        return toApolloItem(item, dashboardItems.length - 1);
      },
      updateDashboardItem: (_, { id, ...item }, { cache }) => {
        const dashboardItems = getDashboardItems();
        dashboardItems[id - 1] = {
          ...dashboardItems[id - 1],
          ...item
        };
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
      items(dashboard) {
        const dashboardItems = getDashboardItems();
        return dashboardItems.map(toApolloItem);
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
        vizState {
          chartType,
          query
        }
      }
    }
  }
`;

export const ADD_DASHBOARD_ITEM = gql`
  mutation AddDashboardItem($vizState: Object!) {
    addDashboardItem(vizState: $vizState) @client {
      id
      layout
      vizState
    }
  }
`;