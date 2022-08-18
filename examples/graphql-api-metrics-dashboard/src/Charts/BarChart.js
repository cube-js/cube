import React from "react";
import { gql, useQuery } from '@apollo/client';
import { getRandomColor, formatDate } from './Helpers'
import { Bar } from 'react-chartjs-2';
import { Chart as ChartJS, BarElement, Title, CategoryScale, LinearScale, Tooltip, Legend } from 'chart.js';
ChartJS.register(BarElement, Title, CategoryScale, LinearScale, Tooltip, Legend);

const COMPLETEDORDERS = gql`
  query CubeQuery {
    cube(
      limit: 10
      where: {
        orders: {
          status: { equals: "completed" }
          createdAt: { inDateRange: "This year" }
        }
      }
    ) {
      orders(orderBy: { count: desc }) {
        count
        status
        createdAt {
          day
        }
      }
    }
  }
`;

const GenerateChart = () => {
  const { data, loading, error } = useQuery(COMPLETEDORDERS);

  if (loading) {
    return <div>loading</div>;
  }

  if (error) {
    return <div>{error.message}</div>;
  }

  if (!data) {
    return null;
  }

  const chartData = {
    labels: ['Daily Completed Orders in 2021'],
    datasets: data.cube
      .map(o => o.orders)
      .map(o => {
        return {
          data: [o.count],
          label: formatDate(new Date(o.createdAt.day)),
          backgroundColor: [getRandomColor()],
        };
      })
  }

  return (
    <Bar
      data={chartData}
    />
  );
}

const BarChart = () => {
  return (
    <div style={{ margin: "10px", paddingTop: "65px" }}>
      <h2 style={{ margin: "10px", textAlign: "center" }}>
        Bar Chart
      </h2>
      <div style={{ margin: "10px 100px", padding: "10px 100px" }}>
        <GenerateChart />
      </div>
    </div>
  );
};

export { BarChart };
