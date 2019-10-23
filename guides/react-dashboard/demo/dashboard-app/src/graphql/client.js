/* globals window */
import { ApolloClient } from "apollo-client";
import { InMemoryCache } from "apollo-cache-inmemory";
import { SchemaLink } from "apollo-link-schema";
import { makeExecutableSchema } from "graphql-tools";
const cache = new InMemoryCache();
const defaultDashboardItems = [{"vizState":"{\"query\":{\"measures\":[\"Orders.count\"],\"timeDimensions\":[{\"dimension\":\"Orders.createdAt\",\"granularity\":\"month\"}],\"dimensions\":[\"Orders.status\"]},\"chartType\":\"bar\",\"sessionGranularity\":\"month\"}","name":"Orders by Status Over Time","id":"10","layout":"{\"x\":9,\"y\":8,\"w\":15,\"h\":8}"},{"vizState":"{\"query\":{\"measures\":[\"LineItems.cumulativeTotalRevenue\"],\"timeDimensions\":[{\"dimension\":\"LineItems.createdAt\",\"granularity\":\"month\",\"dateRange\":\"Last year\"}]},\"chartType\":\"area\",\"sessionGranularity\":\"month\"}","name":"Revenue Growth Last Year","id":"14","layout":"{\"x\":0,\"y\":0,\"w\":13,\"h\":8}"},{"vizState":"{\"query\":{\"measures\":[\"Orders.count\"],\"timeDimensions\":[{\"dimension\":\"Orders.completedAt\",\"granularity\":\"day\",\"dateRange\":\"Last 30 days\"}],\"filters\":[{\"dimension\":\"Orders.status\",\"operator\":\"equals\",\"values\":[\"completed\"]}]},\"chartType\":\"line\"}","name":"Completed Orders Last 30 days","id":"15","layout":"{\"x\":13,\"y\":0,\"w\":11,\"h\":8}"},{"vizState":"{\"query\":{\"measures\":[\"Orders.count\"],\"timeDimensions\":[{\"dimension\":\"Orders.completedAt\"}],\"dimensions\":[\"ProductCategories.name\"]},\"chartType\":\"bar\"}","name":"Orders by Product Category Name","id":"16","layout":"{\"x\":0,\"y\":16,\"w\":24,\"h\":8}"},{"vizState":"{\"query\":{\"dimensions\":[\"Orders.amountTier\"],\"timeDimensions\":[{\"dimension\":\"Orders.completedAt\"}],\"measures\":[\"Orders.count\"],\"filters\":[{\"dimension\":\"Orders.amountTier\",\"operator\":\"notEquals\",\"values\":[\"$0 - $100\"]}]},\"chartType\":\"pie\"}","name":"Orders by Price Tiers","id":"17","layout":"{\"x\":0,\"y\":8,\"w\":9,\"h\":8}"}];

export const getDashboardItems = () =>
  JSON.parse(window.localStorage.getItem("dashboardItems")) ||
  defaultDashboardItems;

export const setDashboardItems = items =>
  window.localStorage.setItem("dashboardItems", JSON.stringify(items));


const nextId = () => {
  const currentId =
    parseInt(window.localStorage.getItem("dashboardIdCounter"), 10) || 1;
  window.localStorage.setItem("dashboardIdCounter", currentId + 1);
  return currentId.toString();
};

const toApolloItem = i => ({ ...i, __typename: "DashboardItem" });

const typeDefs = `
  type DashboardItem {
    id: String!
    layout: String
    vizState: String
    name: String
  }

  input DashboardItemInput {
    layout: String
    vizState: String
    name: String
  }

  type Query {
    dashboardItems: [DashboardItem]
    dashboardItem(id: String!): DashboardItem
  }

  type Mutation {
    createDashboardItem(input: DashboardItemInput): DashboardItem
    updateDashboardItem(id: String!, input: DashboardItemInput): DashboardItem
    deleteDashboardItem(id: String!): DashboardItem
  }
`;
const schema = makeExecutableSchema({
  typeDefs,
  resolvers: {
    Query: {
      dashboardItems() {
        const dashboardItems = getDashboardItems();
        return dashboardItems.map(toApolloItem);
      },

      dashboardItem(_, { id }) {
        const dashboardItems = getDashboardItems();
        return toApolloItem(dashboardItems.find(i => i.id.toString() === id));
      }
    },
    Mutation: {
      createDashboardItem: (_, { input: { ...item } }) => {
        const dashboardItems = getDashboardItems();
        item = { ...item, id: nextId(), layout: JSON.stringify({}) };
        dashboardItems.push(item);
        setDashboardItems(dashboardItems);
        return toApolloItem(item);
      },
      updateDashboardItem: (_, { id, input: { ...item } }) => {
        const dashboardItems = getDashboardItems();
        item = Object.keys(item)
          .filter(k => !!item[k])
          .map(k => ({
            [k]: item[k]
          }))
          .reduce((a, b) => ({ ...a, ...b }), {});
        const index = dashboardItems.findIndex(i => i.id.toString() === id);
        dashboardItems[index] = { ...dashboardItems[index], ...item };
        setDashboardItems(dashboardItems);
        return toApolloItem(dashboardItems[index]);
      },
      deleteDashboardItem: (_, { id }) => {
        const dashboardItems = getDashboardItems();
        const index = dashboardItems.findIndex(i => i.id.toString() === id);
        const [removedItem] = dashboardItems.splice(index, 1);
        setDashboardItems(dashboardItems);
        return toApolloItem(removedItem);
      }
    }
  }
});
export default new ApolloClient({
  cache,
  link: new SchemaLink({
    schema
  })
});
