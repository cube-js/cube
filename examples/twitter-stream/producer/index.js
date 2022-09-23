import * as dotenv from 'dotenv'
import express from 'express'
import { load } from './twitter.js'
import { wrap, produce } from './kafka.js'

dotenv.config()

const port = process.env.PORT || 8080

async function main() {
  express().listen(port, () => {
    console.log('Up and running.')
  })

  await loadAndProduce()

  setInterval(async () => {
    await loadAndProduce()
  }, process.env.FETCH_INTERVAL_SECONDS * 1000);
}

async function loadAndProduce() {
  const tweets = await load()
  const messages = wrap(tweets, tweet => tweet.id)
  await produce(messages)
}
  
main()
