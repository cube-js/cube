const express = require('express')
const bodyParser = require('body-parser')
const { ApolloServer } = require('apollo-server-express')
const cors = require('cors')
const app = express()
app.use(bodyParser.json())
app.use(bodyParser.urlencoded({ extended: true }))
app.use(cors())

async function startServer() {
  const server = new ApolloServer({
    modules: [
      require('./graphql/fraud'),
    ],
  })
  await server.start()
  server.applyMiddleware({ app })  

  app.get('/', (req, res) => res.send('🚀 Server is running!'))
  app.listen({ port: 4000 }, () =>
    console.log(`🚀 Server ready at http://localhost:4000`),
  )
}
startServer()
