import dotenv from 'dotenv';
dotenv.config();

import express from 'express';
import cubejs from '@cubejs-client/core';

const port = process.env.RELAY_PORT

import queries from './queries.js';

function getNextQuery() {
  return queries[Math.floor(Math.random() * queries.length)]
}

function prepareQuery(type) {
  let query = getNextQuery()
  let data = query.data ? query.data() : undefined
  let generateQuery = query[type]

  return generateQuery ? generateQuery(data) : undefined
}

express()

  .get('/:endpoint', (req, res) => {
    let query = prepareQuery('query');

    const api = cubejs.default(
      process.env[`CUBEJS_${req.params.endpoint.toUpperCase()}_TOKEN`], 
      { apiUrl: process.env[`CUBEJS_${req.params.endpoint.toUpperCase()}_API_URL`] }
    );

    if (!query) {
      res.status(200).send()
      return
    }

    api
      .load(query)
      .then(() => res.status(200).send())
      .catch(e => {
        console.log(req.params.endpoint)
        console.log(JSON.stringify(query))
        console.log(e)
        res.status(400).send()
      })
  })

  .listen(port, () => {
    console.log(`Ready to relay at http://localhost:${port}`)
  })