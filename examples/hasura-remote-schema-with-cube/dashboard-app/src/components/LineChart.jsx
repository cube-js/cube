import { ResponsiveLine } from '@nivo/line';

const LineChart = ({ data /* see data tab */ }) => (
  <ResponsiveLine
    data={data}
    colors={{ scheme: 'nivo' }}
    borderColor={{ from: 'color' }}
    margin={{ top: 50, right: 50, bottom: 100, left: 150 }}
    enableGridX={false}
    enableGridY={false}
    xScale={{ type: 'point' }}
    yScale={{
      type: 'linear',
      min: 'auto',
      max: 'auto',
      stacked: true,
      reverse: false
    }}
    yFormat=" >-$.2f"
    axisTop={null}
    axisRight={null}
    axisBottom={null}
    axisLeft={{
      orient: 'left',
      tickSize: 5,
      tickPadding: 5,
      tickRotation: 0,
      legendPosition: 'middle'
    }}
    pointSize={2}
    pointColor={{ theme: 'background' }}
    pointBorderWidth={2}
    pointBorderColor={{ from: 'serieColor' }}
    pointLabelYOffset={-12}
    useMesh={true}
    legends={[
      {
        anchor: 'bottom',
        direction: 'row',
        justify: false,
        translateX: 0,
        translateY: 40,
        itemsSpacing: 30,
        itemDirection: 'left-to-right',
        itemWidth: 80,
        itemHeight: 20,
        itemOpacity: 0.75,
        symbolSize: 12,
        symbolShape: 'circle',
        symbolBorderColor: 'rgba(0, 0, 0, .5)',
        effects: [
          {
            on: 'hover',
            style: {
              itemBackground: 'rgba(0, 0, 0, .03)',
              itemOpacity: 1
            }
          }
        ]
      }
    ]}
  />
)

export default LineChart;
