import { Row, Col, Typography, Card, Image } from 'antd';
import styled from 'styled-components';

import { Database } from '../ConnectionWizardPage';

export function DatabaseCard({ db }) {
  return (
    <Card data-testid="wizard-db-card">
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

const Wrapper = styled.div`
  background-color: #f8f8f9;
  padding: 15px 20px;
  border-radius: 4px;
`;

type TSelectedDatabaseCardProps = {
  db: Database;
};

export function SelectedDatabaseCard({ db }: TSelectedDatabaseCardProps) {
  return (
    <Wrapper>
      <Row align="middle" justify="space-between">
        <Col flex="40px">
          <Image src={db.logo} preview={false} />
        </Col>

        <Col>
          <Typography.Text strong>{db.title}</Typography.Text>
        </Col>
      </Row>
    </Wrapper>
  );
}
