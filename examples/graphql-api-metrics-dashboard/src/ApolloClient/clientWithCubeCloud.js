import { ApolloClient, ApolloLink, InMemoryCache } from "@apollo/client";
import { HttpLink } from "apollo-link-http";

const uri = 'https://thirsty-raccoon.aws-eu-central-1.cubecloudapp.dev/cubejs-api/graphql'
const httpLink = new HttpLink({ uri });

let appJWTToken = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2NTk2MDg2ODV9.qmu1V0SUbW32aIt-AE_DDb99Cpb559r7L-Kky9hBcgc';
const authMiddleware = new ApolloLink((operation, forward)=> {
  if (appJWTToken) {
    operation.setContext({
      headers: {
        Authorization: `${appJWTToken}`
      }
    });
  } 
  return forward(operation);
});

export const clientWithCubeCloud = new ApolloClient({
  cache: new InMemoryCache(),
  link: authMiddleware.concat(httpLink),
});
