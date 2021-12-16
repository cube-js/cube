import { ApolloClient, ApolloLink, InMemoryCache } from "@apollo/client";
import { HttpLink } from "apollo-link-http";
const uri = 'http://localhost:4000/cubejs-api/graphql'

const httpLink = new HttpLink({
  uri,
});

export const client = new ApolloClient({
  cache: new InMemoryCache(),
  link: ApolloLink.from([httpLink]),
});
