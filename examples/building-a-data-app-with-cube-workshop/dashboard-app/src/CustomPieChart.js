import { PieChart, Pie, Cell, Tooltip, ResponsiveContainer, Legend } from 'recharts';
import { Spinner } from 'react-bootstrap';
import { isEmpty } from './utils/isEmpty';
const colors = ["#7DB3FF", "#49457B", "#FF7C78"];

export const CustomPieChart = ({ data }) => {
  if (!data || isEmpty(data)) {
    return <Spinner animation="border" />;
  }

  return (
    <ResponsiveContainer width="100%" height={300}>
      <PieChart>
        <Pie
          isAnimationActive={false}
          data={data.chartPivot()}
          nameKey="x"
          dataKey={data.seriesNames()[0]?.key}
          fill="#8884d8"
        >
          {data.chartPivot().map((e, i) => (
            <Cell key={i} fill={colors[i % colors.length]} />
          ))}
        </Pie>
        <Legend />
        <Tooltip />
      </PieChart>
    </ResponsiveContainer>
  );
}


    