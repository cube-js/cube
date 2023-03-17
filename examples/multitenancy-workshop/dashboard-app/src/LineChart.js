import { ResponsiveLine } from '@nivo/line';

export const LineChart = ({ data, group, x, y }) => {
  const groups = Array.from(data.reduce((all, current) => {
    all.add(group(current));
    return all;
  }, new Set()));

  const transformedData = groups.map(status => ({
    id: status,
    data: data
      .filter(row => group(row) === status)
      .map(row => ({
        x: x(row),
        y: y(row),
      }))
  }));

  return (
    <ResponsiveLine
      data={transformedData}
      xScale={{
        type: 'time',
        format: '%Y-%m-%d',
      }}
      yScale={{
        type: 'linear',
      }}
      axisBottom={{
        format: '%b \'%y',
      }}
      curve='monotoneX'
      margin={{ top: 50, right: 50, bottom: 50, left: 50 }}
      pointColor={{ theme: 'background' }}
      pointBorderColor={{ from: 'serieColor' }}
      pointSize={1}
      pointBorderWidth={1}
      legends={[ {
        anchor: 'top-left',
        direction: 'row',
        justify: false,
        translateX: 5,
        translateY: -25,
        itemsSpacing: 0,
        itemDirection: 'left-to-right',
        itemWidth: 100,
        itemHeight: 20,
        symbolSize: 12,
        symbolShape: 'circle',
      } ] }
    />
  );
}