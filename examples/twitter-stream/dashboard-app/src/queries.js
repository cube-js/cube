function round(value) {
  const floatValue = parseFloat(value)
  return floatValue.toFixed(floatValue > 10 ? 0 : floatValue > 1 ? 1 : 2)
}

function percent(floatValue) {
  return `${(floatValue * 100).toFixed(0)} %`
}

function extractSingleMeasure(query, resultSet) {
  return round(resultSet.tablePivot()[0][query.measures[0]])
}

const limit = 3

export const tweetCountQuery = {
  title: 'Tweets',
  query: {
    measures: [ 'Tweets.count' ]
  },
  value: extractSingleMeasure
}

export const tweetAvgTextLengthQuery = {
  title: 'Avg. tweet length',
  query: {
    measures: [ 'Tweets.avgTextLength' ]
  },
  value: extractSingleMeasure
}

export const tweetAvgRetweetCountQuery = {
  title: 'Avg. RT count',
  query: {
    measures: [ 'Tweets.avgRetweetCount' ]
  },
  value: extractSingleMeasure
}

export const tweetAvgReplyCountQuery = {
  title: 'Avg. reply count',
  query: {
    measures: [ 'Tweets.avgReplyCount' ]
  },
  value: extractSingleMeasure
}

export const tweetAvgQuoteCountQuery = {
  title: 'Avg. quote count',
  query: {
    measures: [ 'Tweets.avgQuoteCount' ]
  },
  value: extractSingleMeasure
}

export const tweetAvgLikeCountQuery = {
  title: 'Avg. like count',
  query: {
    measures: [ 'Tweets.avgLikeCount' ]
  },
  value: extractSingleMeasure
}

export const tweetAuthorCountQuery = {
  title: 'Author count',
  query: {
    measures: [ 'Tweets.authorCount' ]
  },
  value: extractSingleMeasure
}

function createTopAuthorQuery(offset) {
  return {
    title: `#${1 + offset} author`,
    query: {
      measures: [ 'Tweets.count' ],
      dimensions: [ 'Tweets.authorName', 'Tweets.authorUsername' ],
      order: { 'Tweets.count': 'desc', 'Tweets.authorName': 'asc' },
      limit: 1,
      offset
    },
    value: function(query, resultSet) {
      const row = resultSet.tablePivot()[0]

      return <span>
        <em></em>
        <a href={`https://twitter.com/${row['Tweets.authorUsername']}`} target='_blank'>
          {row['Tweets.authorName']}
        </a> ({row['Tweets.count']})
      </span>
    }
  }
}

export const top1AuthorQuery = createTopAuthorQuery(0)

export const top2AuthorQuery = createTopAuthorQuery(1)

export const top3AuthorQuery = createTopAuthorQuery(2)

export const verifiedAuthorsQuery = {
  title: 'Verified authors ratio',
  query: {
    measures: [ 'Tweets.authorCount' ],
    dimensions: [ 'Tweets.authorVerified' ]
  },
  value: function (query, resultSet) {
    const rows = resultSet.tablePivot()
    const verifiedCount = parseInt(rows.find(row => row['Tweets.authorVerified'] === 'true')['Tweets.authorCount'])
    const unverifiedCount = parseInt(rows.find(row => row['Tweets.authorVerified'] === 'false')['Tweets.authorCount'])
    return percent(verifiedCount / (verifiedCount + unverifiedCount))
  }
}

function createTopHashtagQuery(offset) {
  return {
    title: `#${1 + offset} hashtag`,
    query: {
      measures: [ 'Hashtags.count' ],
      dimensions: [ 'Hashtags.name' ],
      order: { 'Hashtags.count': 'desc', 'Hashtags.name': 'asc' },
      limit: 1,
      offset
    },
    value: function(query, resultSet) {
      const row = resultSet.tablePivot()[0]

      return <span>
        <em>#</em>
        <a href={`https://twitter.com/search?q=%23${row['Hashtags.name']}`} target='_blank'>
          {row['Hashtags.name']}
        </a> ({row['Hashtags.count']})
      </span>
    }
  }
}

export const top1HashtagQuery = createTopHashtagQuery(0)

export const top2HashtagQuery = createTopHashtagQuery(1)

export const top3HashtagQuery = createTopHashtagQuery(2)

function createTopMentionQuery(offset) {
  return {
    title: `#${1 + offset} mention`,
    query: {
      measures: [ 'Mentions.count' ],
      dimensions: [ 'Mentions.userName' ],
      order: { 'Mentions.count': 'desc', 'Mentions.userName': 'asc' },
      limit: 1,
      offset
    },
    value: function(query, resultSet) {
      const row = resultSet.tablePivot()[0]

      return <span>
        <em>@</em>
        <a href={`https://twitter.com/${row['Mentions.userName']}`} target='_blank'>
          {row['Mentions.userName']}
        </a> ({row['Mentions.count']})
      </span>
    }
  }
}

export const top1MentionQuery = createTopMentionQuery(0)

export const top2MentionQuery = createTopMentionQuery(1)

export const top3MentionQuery = createTopMentionQuery(2) 

export const firstLastTweetTimestampQuery = {
  title: 'Last tweet at',
  query: {
    measures: [
      'Tweets.minCreatedAt',
      'Tweets.maxCreatedAt'
    ]
  },
  value: function (query, resultSet) {
    const timestamp = parseInt(resultSet.tablePivot()[0]['Tweets.maxCreatedAt'])
    return new Date(timestamp).toLocaleDateString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
  }
}

export const tweetsQuery = {
  title: 'Last tweet',
  query: {
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
  },
  value: function (query, resultSet) {
    const row = resultSet.tablePivot()[0]

    return <span>
      <em>@</em>
      <a href={`https://twitter.com/${row['Tweets.authorUsername']}/status/${row['Tweets.id']}`} target='_blank'>
        {row['Tweets.authorUsername']}
      </a>: {row['Tweets.text'].substr(0, 50).trim()}...
    </span>
  }
}