'use strict';

const QueryRenderer = cubejsReact.QueryRenderer;
const {Area, AreaChart, BarChart, Bar, PieChart, Pie, LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend} = Recharts;

const TimeSeriesRenderer = ({ resultSet }) => {
  return (<LineChart width={600} height={300} data={resultSet.pivotedRows()}
                     margin={{top: 5, right: 30, left: 20, bottom: 5}}>
    <XAxis dataKey="category"
           tickFormatter={(v) => moment(v).format('MMM YY')}
    />
    <YAxis/>
    <CartesianGrid strokeDasharray="3 3"/>
    <Tooltip/>
    <Legend />
    {resultSet.seriesNames().map(({ key, title }) => <Line type="monotone" dataKey={key} key={key} name={title}/>)}
  </LineChart>);
};

const PieRenderer = ({ resultSet }) => {
  return (
    <PieChart width={800} height={400}>
      {resultSet.seriesNames().map(({ key, title }) =>
        <Pie
          isAnimationActive={false}
          data={resultSet.pivotedRows()}
          nameKey="category"
          dataKey={key}
          key={key}
          cx={200}
          cy={200}
          outerRadius={80}
          fill="#8884d8"
          label
        />
      )}
      <Tooltip/>
    </PieChart>
  );
};

const BarRenderer = ({ resultSet }) => {
  return (
    <BarChart width={600} height={300} data={resultSet.pivotedRows()}
              margin={{top: 5, right: 30, left: 20, bottom: 5}}>
      <CartesianGrid strokeDasharray="3 3"/>
      <XAxis dataKey="category"/>
      <YAxis/>
      <Tooltip/>
      <Legend />
      {resultSet.seriesNames().map(({ key, title }) => <Bar dataKey={key} key={key} name={title} fill="#8884d8"/>)}
    </BarChart>
  );
};

const AreaRenderer = ({ resultSet }) => {
  return (
    <AreaChart width={600} height={400} data={resultSet.pivotedRows()}
               margin={{top: 10, right: 30, left: 0, bottom: 0}} >
      <XAxis dataKey="category"/>
      <YAxis/>
      <Tooltip />
      {resultSet.seriesNames().map(({ key, title }) =>
        <Area type='monotone' dataKey={key} key={key} stackId="1" name={title} />
      )}
    </AreaChart>
  );
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
      <QueryRenderer
        query={{
          measures: ["Stories.count", "Stories.averageScore"],
          dimensions: ['Stories.time.week'],
          filters: [{
            dimension: 'Stories.time',
            operator: 'set'
          }]
        }}
        cubejsApi={this.api}
        render={renderChart(TimeSeriesRenderer)}
      />
      <QueryRenderer
        query={{
          measures: ["Stories.count"],
          dimensions: ['Stories.deleted']
        }}
        cubejsApi={this.api}
        render={renderChart(PieRenderer)}
      />
      <QueryRenderer
        query={{
          measures: ["Stories.count"],
          dimensions: ['Stories.dead']
        }}
        cubejsApi={this.api}
        render={renderChart(PieRenderer)}
      />
      <QueryRenderer
        query={{
          measures: ["Stories.percentageOfDead"],
          dimensions: ['Stories.category']
        }}
        cubejsApi={this.api}
        render={renderChart(BarRenderer)}
      />
      <QueryRenderer
        query={{
          measures: ["Stories.count"],
          dimensions: ['Stories.time.month', 'Stories.category'],
          filters: [{
            dimension: 'Stories.time',
            operator: 'set'
          }]
        }}
        cubejsApi={this.api}
        render={renderChart(AreaRenderer)}
      />
    </div>);
  }
}

ReactDOM.render(<HackerNewsExample/>, document.getElementById("root"));