/* globals window */
import { ApolloClient } from "apollo-client";
import { InMemoryCache } from "apollo-cache-inmemory";
import { SchemaLink } from 'apollo-link-schema';
import { makeExecutableSchema } from 'graphql-tools';

const cache = new InMemoryCache();

const getCustomReports = () => JSON.parse(window.localStorage.getItem("customReports")) || [];
const setCustomReports = items => window.localStorage.setItem("customReports", JSON.stringify(items));

const nextId = () => {
  const currentId = parseInt(window.localStorage.getItem("customReportCounter"), 10) || 1;
  window.localStorage.setItem("customReportCounter", currentId + 1);
  return currentId.toString();
};

const toApolloItem = i => ({
  ...i,
  __typename: "CustomReport"
});

const typeDefs = `
  type CustomReport {
    id: String!
    query: String
    name: String
    createdAt: String
  }

  input CustomReportInput {
    query: String
    name: String
  }

  type Query {
    customReports: [CustomReport]
    customReport(id: String!): CustomReport
  }

  type Mutation {
    createCustomReport(input: CustomReportInput): CustomReport
    updateCustomReport(id: String!, input: CustomReportInput): CustomReport
    deleteCustomReport(id: String!): CustomReport
  }
`;

const schema = makeExecutableSchema({
  typeDefs,
  resolvers: {
    Query: {
      customReports() {
        const customReports = getCustomReports();
        return customReports.map(toApolloItem);
      },
      customReport(_, { id }) {
        const customReports = getCustomReports();
        return toApolloItem(customReports.find(i => i.id.toString() === id));
      }
    },
    Mutation: {
      createCustomReport: (_, { input: { ...item } }) => {
        const customReports = getCustomReports();
        item = { ...item, id: nextId(), createdAt: new Date(), layout: JSON.stringify({}) };
        customReports.push(item);
        setCustomReports(customReports);
        return toApolloItem(item);
      },
      updateCustomReport: (_, { id, input: { ...item } }) => {
        const customReports = getCustomReports();
        item = Object.keys(item)
          .filter(k => !!item[k])
          .map(k => ({
            [k]: item[k]
          }))
          .reduce((a, b) => ({ ...a, ...b }), {});
        const index = customReports.findIndex(i => i.id.toString() === id);
        customReports[index] = { ...customReports[index], ...item };
        setCustomReports(customReports);
        return toApolloItem(customReports[index]);
      },
      deleteCustomReport: (_, { id }) => {
        const customReports = getCustomReports();
        const index = customReports.findIndex(i => i.id.toString() === id);
        const [removedItem] = customReports.splice(index, 1);
        setCustomReports(customReports);
        return toApolloItem(removedItem);
      }
    }
  }
});

export default new ApolloClient({
  cache,
  link: new SchemaLink({ schema })
});
