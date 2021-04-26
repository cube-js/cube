import { Col, Row, Space, Typography } from 'antd';
import { useState } from 'react';
import styled from 'styled-components';

import envVarsDatabaseMap from '../../shared/env-vars-db-map';
import { fetchWithTimeout } from '../../utils';
import ConnectionTest from './components/ConnectionTest';
import { DatabaseCard, SelectedDatabaseCard } from './components/DatabaseCard';
import DatabaseForm from './components/DatabaseForm';
import { Button } from '../../atoms';
import { LocalhostTipBox } from './components/LocalhostTipBox';

const { Title, Paragraph } = Typography;

const DatabaseCardWrapper = styled.div`
  cursor: pointer;
`;

const databases = envVarsDatabaseMap.reduce<any>(
  (memo, { databases: dbs, settings }) => [
    ...memo,
    ...(dbs as any).map((db) => ({ ...db, settings })),
  ],
  []
);

async function testConnection(variables: Record<string, string>) {
  const response = await fetchWithTimeout(
    '/playground/test-connection',
    {
      method: 'post',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        variables,
      }),
    },
    5 * 1000
  );

  const { error } = await response.json();
  if (error) {
    throw new Error(error);
  }
}

const Layout = styled.div`
  width: auto;
  max-width: 960px;
  padding: 48px 24px;
  margin: 0 auto;
  background-color: #fff;
`;

async function saveConnection(variables: Record<string, string>) {
  await fetch('/playground/env', {
    method: 'post',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      variables,
    }),
  });

  await fetch('/restart');
}

export type Database = {
  title: string;
  logo: string;
  instructions?: string;
};

export default function ConnectionWizardPage({ history }) {
  const [isLoading, setLoading] = useState(false);
  const [isTestConnectionLoading, setTestConnectionLoading] = useState(false);
  const [testConnectionResult, setTestConnectionResult] = useState<any>(null);
  const [db, selectDatabase] = useState<Database | null>(null);

  return (
    <Layout>
      <Title>Set Up a Database connection</Title>

      {db ? (
        <>
          <Space direction="vertical" size="large">
            <Space size="middle">
              <SelectedDatabaseCard db={db} />

              <Button type="link" onClick={() => selectDatabase(null)}>
                Change
              </Button>
            </Space>

            <Row gutter={[40, 12]}>
              <Col span={24}>
                <Typography>
                  {db.instructions ? (
                    <p>
                      <span
                        dangerouslySetInnerHTML={{ __html: db.instructions }}
                      />
                    </p>
                  ) : (
                    <p>
                      Enter database credentials to connect to your database.{' '}
                      <br />
                      Cube.js will store your credentials into the .env file for
                      future use.
                    </p>
                  )}
                </Typography>
              </Col>

              <Col span={12}>
                <DatabaseForm
                  db={db}
                  deployment={{}}
                  loading={isLoading}
                  disabled={isTestConnectionLoading}
                  onCancel={() => undefined}
                  onSubmit={async (variables) => {
                    try {
                      setTestConnectionResult(null);
                      setTestConnectionLoading(true);

                      await testConnection(variables);

                      setTestConnectionResult({
                        success: true,
                      });

                      setLoading(true);
                      await saveConnection(variables);
                      setLoading(false);

                      history.push('/schema');
                    } catch (error) {
                      setTestConnectionResult({
                        success: false,
                        error,
                      });
                    }

                    setTestConnectionLoading(false);
                  }}
                />
              </Col>

              {['MySQL', 'PostgreSQL', 'Druid', 'Clickhouse'].includes(
                db?.title || ''
              ) && (
                <Col span={12}>
                  <LocalhostTipBox />
                </Col>
              )}
            </Row>

            <Row>
              <Col span={12}>
                <ConnectionTest
                  loading={isTestConnectionLoading}
                  result={testConnectionResult}
                />
              </Col>
            </Row>
          </Space>
        </>
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
