import { XAxis, YAxis, Tooltip, ResponsiveContainer, Legend, BarChart, Bar } from 'recharts';
import moment from "moment";
import numeral from "numeral";
import { Spinner } from 'react-bootstrap';
import { isEmpty } from './utils/isEmpty';

const numberFormatter = (item) => numeral(item).format("0,0");
const dateFormatter = (item) => moment(item).format("MMM YY");
const colors = ["#7DB3FF", "#49457B", "#FF7C78"];
    
export const CustomBarChart = ({ data }) => {
  if (!data || isEmpty(data)) {
    return <Spinner animation="border" />;
  }

  return (
    <ResponsiveContainer width="100%" height={300}>
      <BarChart data={data.chartPivot()}>
        <XAxis tickFormatter={dateFormatter} dataKey="x" />
        <YAxis tickFormatter={numberFormatter} />
          {data.seriesNames().map((seriesName, i) => (
            <Bar
              key={seriesName.key}
              stackId="a"
              dataKey={seriesName.key}
              name={seriesName.title.split(",")[0]}
              fill={colors[i]}
            />
          ))}
        <Legend />
        <Tooltip labelFormatter={dateFormatter} formatter={numberFormatter} />
      </BarChart>
    </ResponsiveContainer>
  );
}
