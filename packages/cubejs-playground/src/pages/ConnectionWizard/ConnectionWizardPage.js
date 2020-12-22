import { Col, PageHeader, Row, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import envVarsDatabaseMap from '../../shared/env-vars-db-map';
import DatabaseCard from './components/DatabaseCard';
import DatabaseForm from './components/DatabaseForm';

const DatabaseCardWrapper = styled.div`
  cursor: pointer;
`;

const databases = envVarsDatabaseMap.reduce(
  (memo, { databases: dbs, settings }) => [
    ...memo,
    ...dbs.map((db) => ({ ...db, settings })),
  ],
  []
);

const { Title, Paragraph } = Typography;

const Layout = styled.div`
  width: auto;
  max-width: 960px;
  padding: 48px 24px;
  margin: 0 auto;
  background-color: #fff;
`;

export default function ConnectionWizardPage() {
  const [db, selectDatabase] = useState(null);

  return (
    <Layout>
      <Title>Set Up a Database connection</Title>

      {db ? (
        <Row gutter={[12, 12]}>
          <Col span={24}>
            <PageHeader
              title={<DatabaseCard db={db} />}
              onBack={() => selectDatabase(null)}
            />
          </Col>

          <Col span={24}>
            <Typography>
              {db.instructions ? (
                <p>
                  <span dangerouslySetInnerHTML={{ __html: db.instructions }} />
                </p>
              ) : (
                <p>Enter database credentials to connect to your database</p>
              )}
            </Typography>
          </Col>

          <Col span={12}>
            <DatabaseForm
              db={db}
              deployment={{}}
              onSubmit={(values) => console.log('submit', values)}
              onCancel={() => console.log('cancel')}
            />
          </Col>
        </Row>
      ) : (
        <>
          <Paragraph>Select a database type</Paragraph>

          <Row gutter={[12, 12]}>
            {databases.map((db) => (
              <Col xl={8} lg={8} md={12} sm={24} xs={24} key={db.title}>
                <DatabaseCardWrapper onClick={() => selectDatabase(db)}>
                  <DatabaseCard db={db} />
                </DatabaseCardWrapper>
              </Col>
            ))}
          </Row>
        </>
      )}
    </Layout>
  );
}
