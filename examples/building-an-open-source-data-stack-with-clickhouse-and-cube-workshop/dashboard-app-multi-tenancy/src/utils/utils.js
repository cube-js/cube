import BarChart from '../components/BarChart'
import LoadingIndicator from '../components/LoadingIndicator'
import jwt from 'jsonwebtoken'
import moment from "moment"
import numeral from "numeral"

export const ticksFormmater = (ticksCount, value, data, dateFormatter) => {
  const valueIndex = data.map(i => i.x).indexOf(value)
  if (valueIndex % Math.floor(data.length / ticksCount) === 0) {
    return dateFormatter(value)
  }

  return ""
}
export const numberFormatter = (item) => numeral(item/100).format("0%")
export const dateFormatter = (item) => moment(item).format("YYYY")
export const colors = ["#7DB3FF", "#49457B", "#FF7C78"]

const isEmpty = (obj) => Object.keys(obj).length === 0

const defaultJwtSecret = '1c7548fdc11622f711fd0113139feefc4cbd88826d3107b29b4950b0b1df159c'
export const defaultDataSourceId = 1
/** OSS Cube */
// const defaultApiUrl = 'http://localhost:4000/cubejs-api/v1'
/** Cube Cloud */
const defaultApiUrl = 'https://blue-stork.aws-us-east-1.cubecloudapp.dev/dev-mode/demo2/cubejs-api/v1'
const jwtSecret = defaultJwtSecret
export const apiUrl = defaultApiUrl

export const dataSources = [
  { id: 1, dataSource: 'ClickHouse' },
  { id: 2, dataSource: 'MySQL' }
].map(({ id, dataSource }) => ({
  id,
  dataSource,
  token: jwt.sign({
    exp: 5000000000,
    dataSource: dataSource.toLowerCase(),
  }, jwtSecret),
}))

export const jsonQuery = () => ({
  measures: [ `Ontime.avgDepDelayGreaterThanTenMinutesPercentage` ],
  timeDimensions: [ {
    dimension: `Ontime.flightdate`,
    granularity: 'year',
  } ],
})

export function DisplayBarChart({ chartData }) {
  if (!chartData || isEmpty(chartData)) {
    return <LoadingIndicator />
  }
  
  return (
    <BarChart
      data={chartData}
    />
  )
}
