import { useState } from 'react';
import cubejs from "@cubejs-client/core";
import { Button } from 'antd';
import TableRenderer from './components/Table';
import PieChart from './components/PieChart';
import ChartRenderer from './components/BarChart';
import { CubeProvider } from '@cubejs-client/react';

const cubejsApi = cubejs(
  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2NTMyODIzNDQsImV4cCI6MTY1NTg3NDM0NH0.6__5oRpMmh8dEbBmhN-tkFOVc-B8CNU8IkxX7E_z5XI",
  {
    apiUrl: "https://inherent-lynx.aws-us-east-1.cubecloudapp.dev/cubejs-api/v1"
  }
);


function App() {
  const [showPieChart, setShowPieChart] = useState(false);

  return (
    <CubeProvider cubejsApi={cubejsApi}>
      <div className="App">
        <div>
          <Button onClick={() => setShowPieChart(false)}>Show Details Table</Button>
          <Button onClick={() => setShowPieChart(true)} >View by Frauds type</Button>
        </div>
        {
          showPieChart ? (
            <>
              <PieChart />
              <ChartRenderer />
            </>
          ) : <TableRenderer />
        }
      </div>
    </CubeProvider>
  );
}

export default App;
