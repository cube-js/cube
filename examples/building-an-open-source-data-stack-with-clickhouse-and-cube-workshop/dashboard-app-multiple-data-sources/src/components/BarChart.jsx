import { ResponsiveBar } from '@nivo/bar'
import {
  colors,
  numberFormatter,
  dateFormatter,
  ticksFormmater,
} from '../utils/utils'

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
          ticksFormmater(15, value, prodData, dateFormatter)
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
