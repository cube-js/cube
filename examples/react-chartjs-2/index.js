import React from 'react';
import ReactDOM from 'react-dom';
import { Layout, Row, Col, Menu } from 'antd';
import chartsExamples from './chartsExamples';
import 'antd/dist/antd.css';

const { Header, Footer, Sider, Content } = Layout;
class App extends React.Component {
  constructor(props) {
    super(props);

    this.state = { activeChart: 'line' }
  }

  handleMenuChange(e) {
    this.setState({
      activeChart: e.key
    })
  }

  render() {
    return (
      <Layout>
        <Header style={{ background: "#fff" }}>
          <Menu
            mode="horizontal"
            onClick={this.handleMenuChange.bind(this)}
            defaultSelectedKeys={[this.state.activeChart]}
            style={{ lineHeight: '64px' }}
          >
            <Menu.Item key="line">Line</Menu.Item>
            <Menu.Item key="bar">Bar</Menu.Item>
            <Menu.Item key="pie">Pie</Menu.Item>
          </Menu>
        </Header>
        <Content style={{ padding: '0 50px', margin: '50px 0' }}>
          { chartsExamples[this.state.activeChart].render() }
        </Content>
      </Layout>
    );
  }
}

ReactDOM.render(<App />, document.getElementById('root'));
