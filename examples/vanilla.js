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
    this.api.load({
      measures: ["Bots.count"],
      dimensions: ["Integrations.kind"],
      timeDimensions: [{
        dimension: "Bots.createdAt",
        dateRange: ["2018-01-01", "2018-02-01"]
      }]
    })
      .then(r => {
        const context = document.getElementById("myChart");
        const config = cubejs.chartjsConfig(r, { type: 'pie' });
        this.setState({ result: JSON.stringify(config) });
        new Chart(context, config);
      })
  }
}

ReactDOM.render(<LikeButton/>, document.getElementById("root"));