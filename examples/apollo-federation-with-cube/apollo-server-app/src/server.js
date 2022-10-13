async function startServer() {
  const express = require('express')
  const { ApolloServer } = require('apollo-server-express')
  const app = express()

  const server = new ApolloServer({
    modules: [
      require('../graphql/fraud'),
    ],
  })
  await server.start()
  server.applyMiddleware({ app })  

  app.get('/', (req, res) => res.send('🚀 Server is running!'))
  app.listen({ port: 4000 }, () =>
    console.log(`🚀 Server ready at http://localhost:4000`),
  )
}
module.exports = startServer()
