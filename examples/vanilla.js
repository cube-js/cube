'use strict';

class LikeButton extends React.Component {
  constructor(props) {
    super(props);
    this.state = { loading: false };
    this.api = cubejs('eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpIjozODU5NH0.5wEbQo-VG2DEjR2nBpRpoJeIcE_oJqnrm78yUo9lasw');
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
      measures: ["Stories.count"],
      timeDimensions: [{
        dimension: "Stories.time",
        dateRange: ["2015-01-01", "2016-01-01"],
        granularity: 'month'
      }]
    })
      .then(r => {
        const context = document.getElementById("myChart");
        const config = cubejs.chartjsConfig(r);
        this.setState({ result: JSON.stringify(config) });
        new Chart(context, config);
      })
  }
}

ReactDOM.render(<LikeButton/>, document.getElementById("root"));