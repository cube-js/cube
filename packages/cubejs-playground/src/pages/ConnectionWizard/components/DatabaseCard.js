import {
  Row,
  Col,
  Typography,
  Card,
  Image,
} from 'antd';

export default function DatabaseCard({ db }) {
  return (
    <Card>
      <Row align="middle" justify="space-between">
        <Col flex="40px">
          <Image src={db.logo} preview={false} />
        </Col>

        <Col>
          <Typography.Text strong>{db.title}</Typography.Text>
        </Col>
      </Row>
    </Card>
  );
}
