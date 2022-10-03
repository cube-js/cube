import { ResponsiveBar } from '@nivo/bar'
import moment from "moment";
import numeral from "numeral";

const ticksFormmater = (ticksCount, value, data, dateFormatter) => {
  const valueIndex = data.map(i => i.x).indexOf(value)
  if (valueIndex % Math.floor(data.length / ticksCount) === 0) {
    return dateFormatter(value)
  }

  return ""
}
const numberFormatter = (item) => numeral(item/100).format("0%")
const dateFormatter = (item) => moment(item).format("MM")
const colors = ["#7DB3FF", "#49457B", "#FF7C78"]

const BarChart = ({ data /* see data tab */ }) => {
  const prodData = data.chartPivot()
  const prodKeys = data.seriesNames().map(seriesName => seriesName.key)
  const prodIndex = 'x'

  return (
    <ResponsiveBar
      enableLabel={false}
      colors={colors}
      data={prodData}
      keys={prodKeys}
      indexBy={prodIndex}
      enableGridY={false}
      padding={0.3}
      margin={{ top: 60, bottom: 60, left: 40 }}
      axisLeft={{
        format: numberFormatter
      }}
      axisBottom={{
        format: value =>
          ticksFormmater(12, value, prodData, dateFormatter)
      }}
      tooltip={({ id, value, color }) => (
        <strong style={{ color, backgroundColor: 'white', padding: '5px', borderRadius: '5px' }}>
          {id.split(",")[0]}: {numberFormatter(value)}
        </strong>
      )}
      legends={[
        {
          anchor: "bottom",
          direction: "row",
          translateY: 50,
          itemsSpacing: 2,
          itemWidth: 150,
          itemHeight: 20,
          itemDirection: "left-to-right"
        }
      ]}
    />
  )
}

export default BarChart;
