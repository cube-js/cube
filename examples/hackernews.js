'use strict';

const QueryRenderer = cubejsReact.QueryRenderer;
const {LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend} = Recharts;

const TimeSeriesChart = ({ resultSet }) => {
  return (<LineChart width={600} height={300} data={resultSet.pivotedRows()}
                     margin={{top: 5, right: 30, left: 20, bottom: 5}}>
    <XAxis dataKey="category"
           tickFormatter={(v) => moment(v).format('MMM YY')}
    />
    <YAxis/>
    <CartesianGrid strokeDasharray="3 3"/>
    <Tooltip/>
    <Legend />
    {resultSet.seriesNames().map(({key}) => <Line type="monotone" dataKey={key} stroke="#8884d8" id={key}/>)}
  </LineChart>);
};

class HackerNewsExample extends React.Component {
  constructor(props) {
    super(props);
    this.state = {};
    this.api = cubejs('eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpIjozODU5NH0.5wEbQo-VG2DEjR2nBpRpoJeIcE_oJqnrm78yUo9lasw');
  }

  render() {
    const renderChart = (Chart) => ({ resultSet, error }) => {
      if (error) {
        return <div>{error.message}</div>;
      } else if (resultSet) {
        return (
          <Chart resultSet={resultSet} />
        )
      }
      return null;
    };

    return (<div>
      <QueryRenderer query={{
        measures: ["Stories.count", "Stories.averageScore"],
        dimensions: ['Stories.time.week']
      }} cubejsApi={this.api} render={renderChart(TimeSeriesChart)}
      />
    </div>);
  }
}

ReactDOM.render(<HackerNewsExample/>, document.getElementById("root"));