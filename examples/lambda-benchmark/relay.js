import dotenv from 'dotenv';
dotenv.config();

import express from 'express';
import cubejs from '@cubejs-client/core';

const port = process.env.RELAY_PORT

import { queries } from './queries.js';

function prepareQuery(id) {
  const spec = queries[id];
  const data = spec.data()
  return spec.query(data)
}

function query(endpoint, id) {
  endpoint = endpoint.toUpperCase();
  const query = prepareQuery(id);
  const api = cubejs.default(
    process.env[`CUBEJS_${endpoint}_TOKEN`],
    { apiUrl: process.env[`CUBEJS_${endpoint}_API_URL`] }
  );

  if (!query) {
    res.status(200).send()
    return
  }

  return api.load(query)
}

express()
  .get('/:endpoint/:id', async (req, res) => {
    const endpoint = req.params.endpoint.toUpperCase();
    const id = req.params.id;
    try {
      await query(endpoint, id);
      res.status(200).send()
    } catch (e) {
      console.log(req.params.endpoint)
      console.log(JSON.stringify(query))
      console.log(e)
      res.status(400).send()
    }
  })

  .listen(port, () => {
    console.log(`Ready to relay at http://localhost:${port}`)
  })