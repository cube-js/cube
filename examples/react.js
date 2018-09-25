'use strict';

const QueryRenderer = cubejsReact.QueryRenderer;
const {LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend} = Recharts;

class ReactExample extends React.Component {
  constructor(props) {
    super(props);
    this.state = {};
    this.api = cubejs('eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpIjozODU5NH0.5wEbQo-VG2DEjR2nBpRpoJeIcE_oJqnrm78yUo9lasw');
    this.chartRef = React.createRef();
  }

  render() {
    return (<div>
      <QueryRenderer query={{
        measures: ["Stories.count"],
        timeDimensions: [{
          dimension: "Stories.time",
          dateRange: ["2015-01-01", "2016-01-01"],
          granularity: 'month'
        }]
      }} cubejsApi={this.api} render={
        ({ resultSet, error }) => {
          if (error) {
            return <div>{error.stack}</div>;
          } else if (resultSet) {
            return (
              <LineChart width={600} height={300} data={resultSet.rawData()}
                               margin={{top: 5, right: 30, left: 20, bottom: 5}}>
                <XAxis dataKey="Stories.time"
                       tickFormatter={(v) => moment(v).format('MMM YY')}
                />
                <YAxis/>
                <CartesianGrid strokeDasharray="3 3"/>
                <Tooltip/>
                <Legend />
                <Line type="monotone" dataKey="Stories.count" stroke="#8884d8"/>
              </LineChart>
            )
          }
          return null;
        }
      }
      />
    </div>);
  }
}

ReactDOM.render(<ReactExample/>, document.getElementById("root"));