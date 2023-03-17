import ReactDOM from 'react-dom/client'
import { useState, useEffect } from 'react'
import cubejs from '@cubejs-client/core'
import { CubeProvider, useCubeQuery } from '@cubejs-client/react'
import WebSocketTransport from '@cubejs-client/ws-transport'
import { getTimestamp } from './utils'
import {
  tweetCountQuery,
  tweetAvgTextLengthQuery,
  tweetAvgRetweetCountQuery,
  tweetAvgReplyCountQuery,
  tweetAvgQuoteCountQuery,
  tweetAvgLikeCountQuery,
  tweetAuthorCountQuery,
  top1AuthorQuery,
  top2AuthorQuery,
  top3AuthorQuery,
  verifiedAuthorsQuery,
  top1HashtagQuery,
  top2HashtagQuery,
  top3HashtagQuery,
  top1MentionQuery,
  top2MentionQuery,
  top3MentionQuery,
  firstLastTweetTimestampQuery,
  tweetsQuery
} from './queries'
import Cards from './components/Cards'
import Card from './components/Card'
import LiveIndicatorCard from './components/LiveIndicatorCard'

const cubejsApi = cubejs({
  transport: new WebSocketTransport({
    authorization: process.env.CUBE_TOKEN,
    apiUrl: process.env.CUBE_API_URL,
  }),
});

const entries = [
  tweetCountQuery,
  tweetAvgTextLengthQuery,
  tweetAvgRetweetCountQuery,
  tweetAvgReplyCountQuery,
  tweetAvgQuoteCountQuery,
  tweetAvgLikeCountQuery,
  tweetAuthorCountQuery,
  top1AuthorQuery,
  top2AuthorQuery,
  top3AuthorQuery,
  verifiedAuthorsQuery,
  top1HashtagQuery,
  top2HashtagQuery,
  top3HashtagQuery,
  top1MentionQuery,
  top2MentionQuery,
  top3MentionQuery
]

ReactDOM
  .createRoot(document.getElementById('app'))
  .render(<CubeProvider cubejsApi={cubejsApi}><App /></CubeProvider>)

function App() {
  const [ cards, setCards ] = useState([])

  function pushCard(entry, resultSet) {
    setCards(current => ([
      {
        title: entry.title,
        value: entry.value(entry.query, resultSet),
        timestamp: getTimestamp()
      },
      ...current.filter(card => card.title !== entry.title)
    ]))
  }

  useEffect(() => {
    entries.forEach(entry => {
      cubejsApi.subscribe(entry.query, { subscribe: true }, (error, resultSet) => {
        if (!error) {
          pushCard(entry, resultSet)
        }
      })
    })
    
    cubejsApi.subscribe(firstLastTweetTimestampQuery.query, { subscribe: true }, (error, resultSet) => {
      if (!error) {
        const firstTweetTimestamp = resultSet.tablePivot()[0]['Tweets.minCreatedAt']
        const lastTweetTimestamp = resultSet.tablePivot()[0]['Tweets.maxCreatedAt']
    
        pushCard(firstLastTweetTimestampQuery, resultSet)
      }
    })
    
    cubejsApi.subscribe(firstLastTweetTimestampQuery.query, { subscribe: true }, (error, resultSet) => {
      if (!error) {
        const lastTweetTimestamp = resultSet.tablePivot()[0]['Tweets.maxCreatedAt']
    
        const query = JSON.parse(JSON.stringify(tweetsQuery.query))
        query.filters[0].values.push(lastTweetTimestamp)
    
        cubejsApi.load(query, (error, resultSet) => {
          if (!error) {
            pushCard(tweetsQuery, resultSet)
          }
        })
      }
    })
  }, [])

  return <Cards>
    <LiveIndicatorCard />
    {cards.map((card, i) => (
      <Card key={i} {...card} />
    ))}
  </Cards>
}