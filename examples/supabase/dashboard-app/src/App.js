import { useState } from 'react';
import { Button } from 'antd';
import TableRenderer from './components/Table';
import PieChart from './components/PieChart';
import ChartRenderer from './components/BarChart';

function App() {
  const [showPieChart, setShowPieChart] = useState(false);

  return (
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
  );
}

export default App;
