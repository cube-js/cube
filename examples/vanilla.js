'use strict';

class LikeButton extends React.Component {
  constructor(props) {
    super(props);
    this.state = { loading: false };
    this.api = cubejs('eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpIjoiMTQzIn0.6bn_WAIzJZzatu8H2XJcTyyNU9Qhj6WP2yM5Fw1nDUw');
  }

  render() {
    if (this.state.result) {
      return this.state.result;
    }
    if (this.state.loading) {
      return 'Loading...';
    }

    return <button onClick={() => this.load()}>Load</button>;
  }

  load() {
    this.setState({ loading: true });
    this.api.load('chart', {
      "timezone": "America/Los_Angeles",
      "measures": ["Bots.count"],
      "timeDimensions": [{
        "dimension": "Bots.createdAt",
        "granularity": "day",
        "dateRange": ["2018-01-01", "2018-02-01"]
      }]
    })
      .then(r => {
        const context = document.getElementById("myChart");
        const resultSet = cubejs.chartjs(r);
        this.setState({ result: JSON.stringify(resultSet.timeSeries()) });
        var myChart = new Chart(context, resultSet.timeSeries());
      })
  }
}

ReactDOM.render(<LikeButton/>, document.getElementById("root"));