import { ApolloClient, ApolloLink, InMemoryCache } from "@apollo/client";
import { HttpLink } from "apollo-link-http";

const uri = 'https://tan-rooster.aws-eu-central-1.cubecloudapp.dev/cubejs-api/graphql'
const httpLink = new HttpLink({ uri });

let appJWTToken = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2Mzk1Nzc2NTh9.ARCF3pyi9rpNAPEF2rBoP-EKjzfJQX1q3X7A3qCDoYc';
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
