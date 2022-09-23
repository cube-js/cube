import * as dotenv from 'dotenv'
import { Client } from 'twitter-api-sdk'

dotenv.config()

const clients = process.env.TWITTER_BEARER_TOKENS.split(',').map(token => new Client(token))

let requests = 0

export async function load() {
  requests++

  const keyIndex = Math.floor(Math.random() * clients.length)
  const client = clients[keyIndex]

  const response = await client.tweets.listsIdTweets(process.env.TWITTER_LISTS, {
    max_results: process.env.TWITTER_BATCH_SIZE || 10,
    "tweet.fields": [
      "attachments",
      "author_id",
      // "context_annotations",
      "conversation_id",
      "created_at",
      "entities",
      "geo",
      "id",
      "in_reply_to_user_id",
      "lang",
      // "possibly_sensitive",
      "public_metrics",
      // "referenced_tweets",
      "reply_settings",
      "source",
      "text",
      "withheld",
    ],
    "user.fields": [
      "created_at",
      "description",
      "entities",
      "id",
      "location",
      "name",
      "pinned_tweet_id",
      "profile_image_url",
      "protected",
      "public_metrics",
      "url",
      "username",
      "verified",
      "withheld",
    ],
    "expansions": [
      "attachments.media_keys",
      "attachments.poll_ids",
      "author_id",
      "entities.mentions.username",
      "geo.place_id",
      "in_reply_to_user_id",
      // "referenced_tweets.id",
      // "referenced_tweets.id.author_id",
    ]
  })

  const tweets = response.data.map(tweet => ({
    ...tweet,

    author: response.includes.users.find(user => user.id === tweet.author_id)
  }))

  log(tweets, keyIndex);

  return tweets
}

const previouslySeenIds = []

async function log(tweets, keyIndex = 0) {
  const ids = tweets.map(tweet => tweet.id)
  const newButPreviouslySeenIds = ids.filter(id => previouslySeenIds.includes(id))

  console.log(
    `Twitter API request #${requests} with key #${keyIndex}: ` +
    `${ids.length} tweets, ` +
    `${ids.length - newButPreviouslySeenIds.length} new`
  )

  previouslySeenIds.push(...ids)
}