import { ApolloClient, ApolloLink, InMemoryCache } from "@apollo/client";
import { HttpLink } from "apollo-link-http";

const uri = 'http://localhost:4000/cubejs-api/graphql'
const httpLink = new HttpLink({ uri });

let appJWTToken = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoiYWRtaW4ifQ.FUewE3jySlmMD3DnOeDaMPBqTqirLQeuRG_--O5oPNw';
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

export const clientWithJwt = new ApolloClient({
  cache: new InMemoryCache(),
  link: authMiddleware.concat(httpLink),
});
