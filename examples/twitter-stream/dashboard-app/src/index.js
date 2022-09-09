import ReactDOM from 'react-dom/client'
import { useState, useEffect } from 'react'
import cubejs from '@cubejs-client/core'
import { CubeProvider, useCubeQuery } from '@cubejs-client/react'
import WebSocketTransport from '@cubejs-client/ws-transport'

const cubejsApi = cubejs({
  transport: new WebSocketTransport({
    authorization: process.env.CUBE_TOKEN,
    apiUrl: process.env.CUBE_API_URL,
  }),
});

const tweetCountQuery = {
  measures: [ 'Tweets.count' ]
}

const tweetAvgTextLengthQuery = {
  measures: [ 'Tweets.avgTextLength' ]
}

const tweetAvgRetweetCountQuery = {
  measures: [ 'Tweets.avgRetweetCount' ]
}

const tweetAvgReplyCountQuery = {
  measures: [ 'Tweets.avgReplyCount' ]
}

const tweetAvgQuoteCountQuery = {
  measures: [ 'Tweets.avgQuoteCount' ]
}

const tweetAvgLikeCountQuery = {
  measures: [ 'Tweets.avgLikeCount' ]
}

const tweetAuthorCountQuery = {
  measures: [ 'Tweets.authorCount' ]
}

const topAuthorsQuery = {
  measures: [ 'Tweets.count' ],
  dimensions: [ 'Tweets.authorName' ],
  order: { 'Tweets.count': 'desc', 'Tweets.authorName': 'asc' },
  limit: 10
}

const verifiedAuthorsQuery = {
  measures: [ 'Tweets.authorCount' ],
  dimensions: [ 'Tweets.authorVerified' ]
}

const topHashtagsQuery = {
  measures: [ 'Hashtags.count' ],
  dimensions: [ 'Hashtags.name' ],
  order: { 'Hashtags.count': 'desc', 'Hashtags.name': 'asc' },
  limit: 10
}

const topMentionsQuery = {
  measures: [ 'Mentions.count' ],
  dimensions: [ 'Mentions.userName' ],
  order: { 'Mentions.count': 'desc', 'Mentions.userName': 'asc' },
  limit: 10
}

const firstLastTweetTimestampQuery = {
  measures: [
    'Tweets.minCreatedAt',
    'Tweets.maxCreatedAt'
  ]
}

const tweetsQuery = {
  dimensions: [
    'Tweets.id',
    'Tweets.authorUsername',
    'Tweets.text'
  ],
  filters: [ {
    member: 'Tweets.createdAt',
    operator: 'equals',
    values: []
  } ]
}

const queries = [
  tweetCountQuery,
  tweetAvgTextLengthQuery,
  tweetAvgRetweetCountQuery,
  tweetAvgReplyCountQuery,
  tweetAvgQuoteCountQuery,
  tweetAvgLikeCountQuery,
  tweetAuthorCountQuery,
  topAuthorsQuery,
  verifiedAuthorsQuery,
  topHashtagsQuery,
  topMentionsQuery
]

queries.forEach(query => {
  cubejsApi.subscribe(query, { subscribe: true }, (error, resultSet) => {
    if (!error) {
      console.log(resultSet.tablePivot())
    }
  })
})

cubejsApi.subscribe(firstLastTweetTimestampQuery, { subscribe: true }, (error, resultSet) => {
  if (!error) {
    const firstTweetTimestamp = resultSet.tablePivot()[0]['Tweets.minCreatedAt']
    const lastTweetTimestamp = resultSet.tablePivot()[0]['Tweets.maxCreatedAt']

    console.log(new Date(parseInt(firstTweetTimestamp)), 'to', new Date(parseInt(lastTweetTimestamp)))
  }
})

cubejsApi.subscribe(firstLastTweetTimestampQuery, { subscribe: true }, (error, resultSet) => {
  if (!error) {
    const lastTweetTimestamp = resultSet.tablePivot()[0]['Tweets.maxCreatedAt']

    const query = JSON.parse(JSON.stringify(tweetsQuery))
    query.filters[0].values.push(lastTweetTimestamp)

    cubejsApi.load(query, (error, resultSet) => {
      if (!error) {
        console.log(resultSet.tablePivot())
      }
    })
  }
})

ReactDOM
  .createRoot(document.getElementById('app'))
  .render(<CubeProvider cubejsApi={cubejsApi}><App /></CubeProvider>)

function App() {
  return <>
    <p style={{ textAlign: 'center' }}>UI pending. Please use your browser's console.</p>
  </>
}