import cubejs from '@cubejs-client/core'
import { format } from 'date-fns'

const cubejsApi = cubejs(
  process.env.CUBE_TOKEN,
  { apiUrl: process.env.CUBE_API }
)

function createQuery(word) {
  return ({ 
    measures: [ "Words.count" ],
    timeDimensions: [{
      dimension: "Words.timestamp",
      granularity: "month"
    }],
    filters: !word ? [] : [{
      member: "Words.word",
      operator: "equals",
      values: [ word.toLowerCase() ]
    }]
  })
}

function formatDate(date) {
  let month = format(date, 'MMM')
  let year = format(date, 'yyyy')
  return month == 'Jan' ? year : ''
}

function mapData(data, maxValues) {
  return maxValues.map(row => {
    let count = data.find(e => e["Words.timestamp"] === row["Words.timestamp"])

    return ({
      name: formatDate(new Date(row["Words.timestamp"])),
      data: !count ? 0 : parseInt(count["Words.count"]) / parseInt(row["Words.count"])
    })
  })
}

let maxValues = undefined

function fetchMaxValue() {
  return new Promise((resolve, reject) => {
    if (maxValues) {
      resolve(maxValues)
      return
    }

    cubejsApi
      .load(createQuery())
      .then(resultSet => {
        maxValues = resultSet.rawData()
        resolve(maxValues)
      })
      .catch(reject)
  })
}

export function fetchData(word) {
  return cubejsApi
    .load(createQuery(word))
    .then(resultSet => {
      return fetchMaxValue()
        .then(maxValues => {
          return mapData(resultSet.rawData(), maxValues)
        }) 
    })
}