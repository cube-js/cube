import React from 'react';
import Chart from '../components/Charts';
import { Rectangle } from 'recharts';

const getPath = (x, y, width, height) => {
  const baseY = y + (height - 100);
  const baseX = x + (width + 5);
  const baseWidth = 50;
  const arrowWidth = 10;
  return `M${baseX},${baseY}
          L${baseX + baseWidth} ${baseY}
          L${baseX + arrowWidth + baseWidth} ${baseY + 13}
          L${baseX + baseWidth} ${baseY + 13*2}
          L${baseX} ${baseY + 13*2}
          Z`;
};


const BarwithSteps = (props) => {
  const {  x, y, width, height, index, resultSet } = props;
  const data = resultSet.chartPivot()
  const lastIndex = data.length - 1;
  const showStep = index !== lastIndex;
  let conversion = null
  if (showStep) {
    conversion = Math.round(100 * Object.values(data[index + 1])[2]/Object.values(data[index])[2]);
  }
  const baseY = y + (height - 100);
  const baseX = x + (width + 5);

  return (
    <g>
      <Rectangle {...props} />
      {showStep &&
        ([
          <path d={getPath(x, y, width, height)} stroke="none" fill="#50556C"/>,
          <text x={baseX + 25} y={baseY + 15} fill="#fff" textAnchor="middle" dominantBaseline="middle">
            {conversion}%
          </text>
        ])
      }
    </g>
  );
}

const Funnel = ({ query, dateRange }) => (
  <Chart
    type="bar"
    options={{
      label: { position: 'top' },
      margin: { top: 20 },
      shape: <BarwithSteps />
    }}
    query={query}
  />
)

export default Funnel;
