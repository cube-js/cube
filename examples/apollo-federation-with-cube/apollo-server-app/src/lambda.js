const { ApolloServer } = require('apollo-server-lambda');
const express = require('express')
const bodyParser = require('body-parser')
const cors = require('cors')
const app = express()
const router = express.Router()
router.use(bodyParser.json())
router.use(bodyParser.urlencoded({ extended: true }))
router.use(cors())
router.get('/', (req, res) => res.send('🚀 Server is running!'))
app.use('/.netlify/functions/lambda', router)

const getHandler = (event, context) => {
  const server = new ApolloServer({
    modules: [ require('../graphql/fraud') ],
  })
  const graphqlHandler = server.createHandler({
    expressAppFromMiddleware(middleware) {
      app.use(middleware)
      return app
    }
  })

  if (!event.requestContext) {
    event.requestContext = context
  }
  return graphqlHandler(event, context)
}

exports.handler = getHandler;
