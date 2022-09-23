async function startGateway() {
  const express = require('express')
  const { ApolloServer } = require('apollo-server-express')
  const app = express()

  const { ApolloGateway, RemoteGraphQLDataSource } = require('@apollo/gateway')
  const { readFileSync } = require('fs')

  const supergraphSdl = readFileSync('./graphql/supergraph.graphql').toString();

  class AuthenticatedDataSource extends RemoteGraphQLDataSource {
    willSendRequest({ request, context }) {
      request.http.headers.set('Authorization', context.token);
    }
  }

  const gateway = new ApolloGateway({
    supergraphSdl,
    buildService({ name, url }) {
      return new AuthenticatedDataSource({ url });
    },
  });

  const server = new ApolloServer({
    gateway,
    subscriptions: false,
    context: ({ req }) => {
      const token = req.headers.authorization || '';
      return { token };
    },
  })
  await server.start()
  server.applyMiddleware({ app, path: "/gateway/graphql" })  

  app.get('/', (req, res) => res.send('🚀 Gateway is running!'))
  app.get('/gateway', (req, res) => res.send('🚀 Gateway is running!'))
  app.listen({ port: 4001 }, () =>
    console.log(`🚀 Gateway ready at http://localhost:4001`),
  )
}

module.exports = startGateway()
