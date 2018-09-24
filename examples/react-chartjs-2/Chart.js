import React from 'react';
import { Row, Col, Tabs, Spin, Card } from 'antd';
import { QueryRenderer } from '@cubejs-client/react';
import JSONPretty from 'react-json-pretty';
import cubejs from 'cubejs-client';
import Prism from "prismjs";
import "./prism.css";


// Minimal Example:
//
// import { default as cubejs, toChartjsData } from 'cubejs-client';
// import { QueryRenderer } from '@cubejs-client/react';
// import { Pie } from 'react-chartjs-2';

// ReactDOM.render(
//   <QueryRenderer
//     query={{ measures: ['Stories.count'], dimensions: ['Stories.date.month'] }}
//     cubejsApi={cubejs('API_KEY')}
//     render={ ({ resultSet }) => (
//       <Line
//         data={toChartjsData('line', resultSet)}
//         options={{ scales: { xAxes: [{ type: `time`, time: { unit: 'month' }}] }}}
//       />
//     )
//   />
// )

const HACKER_NEWS_DATASET_API_KEY = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpIjozODU5NH0.5wEbQo-VG2DEjR2nBpRpoJeIcE_oJqnrm78yUo9lasw'

class PrismCode extends React.Component {
  componentDidMount() {
    Prism.highlightAll();
  }

  componentDidUpdate() {
    Prism.highlightAll();
  }

  render() {
    return (
      <pre>
        <code className='language-javascript'>
          { this.props.code }
        </code>
      </pre>
    )
  }
}

const tabList = [{
  key: 'code',
  tab: 'Code'
}, {
  key: 'response',
  tab: 'Response'
}]

class CodeExample extends React.Component {
  constructor(props) {
    super(props);

    this.state = { activeTabKey: 'code' };
  }

  onTabChange(key) {
    this.setState({ activeTabKey: key });
  }

  render() {
    const { codeExample, resultSet } = this.props;
    const contentList = {
      code: <PrismCode code={codeExample} />,
      response: <PrismCode code={JSON.stringify(resultSet, null, 2)} />
    };

    return (<Col span={12} style={{ "padding": 10 }}>
      <Card
        tabList={tabList}
        activeTabKey={this.state.activeTabKey}
        onTabChange={(key) => { this.onTabChange(key, 'key'); }}
      >
        { contentList[this.state.activeTabKey] }
      </Card>
    </Col>);
  }
}

const TabPane = Tabs.TabPane;
const Chart = ({ query, codeExample, render }) => (
  <Row gutter={24}>
    <QueryRenderer
      query={query}
      cubejsApi={cubejs(HACKER_NEWS_DATASET_API_KEY)}
      render={ ({ resultSet, error }) => {
        if (resultSet) {
          return [
            <Col span={12} style={{ "padding": 10 }}>
              <Card
                title="Line Chart"
              >
                {render({ resultSet, error })}
              </Card>
            </Col>,
            <CodeExample resultSet={resultSet} codeExample={codeExample} />
          ];
        }
        return <Spin style={{ textAlign: 'center' }} />;
      }}
    />
  </Row>
)

export default Chart;
