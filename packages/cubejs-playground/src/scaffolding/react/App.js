import {
  Row, Col, Card, Layout
} from 'antd';
import 'antd/dist/antd.css';
import './index.css';

const AppLayout = ({ children }) => (
  <Layout>
    <Layout.Header>
      <div style={{ float: 'left' }}>
        <h2
          style={{
            color: "#fff",
            margin: 0,
            marginRight: '1em'
          }}
        >
          My Dashboard
        </h2>
      </div>
    </Layout.Header>
    <Layout.Content
      style={{
        padding: "0 25px 25px 25px",
        margin: "25px"
      }}
    >
      {children}
    </Layout.Content>
  </Layout>
);

const Dashboard = ({ children }) => (
  <Row type="flex" justify="space-around" align="top" gutter={24}>{children}</Row>
);

const DashboardItem = ({ children, title }) => (
  <Col span={24} lg={12}>
    <Card title={title} style={{ marginBottom: '24px' }}>
      {children}
    </Card>
  </Col>
);

const App = () => (
  <AppLayout>
    <Dashboard />
  </AppLayout>
);
