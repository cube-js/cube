import { Alert, Col, Row, Space, Typography } from 'antd';
import { useEffect, useState } from 'react';
import styled from 'styled-components';

import envVarsDatabaseMap from '../../shared/env-vars-db-map';
import { fetchWithTimeout } from '../../utils';
import ConnectionTest from './components/ConnectionTest';
import { DatabaseCard, SelectedDatabaseCard } from './components/DatabaseCard';
import DatabaseForm from './components/DatabaseForm';
import { Button } from '../../atoms';
import { LocalhostTipBox } from './components/LocalhostTipBox';
import { event, playgroundAction } from '../../events';

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
}

export type Database = {
  title: string;
  logo: string;
  instructions?: string;
};

export default function ConnectionWizardPage({ history }) {
  const [hostname, setHostname] = useState<string>('');
  const [isLoading, setLoading] = useState(false);
  const [isTestConnectionLoading, setTestConnectionLoading] = useState(false);
  const [testConnectionResult, setTestConnectionResult] = useState<any>(null);
  const [db, selectDatabase] = useState<Database | null>(null);

  useEffect(() => {
    playgroundAction('connection_wizard_open');
  }, []);

  useEffect(() => {
    setTestConnectionLoading(false);
    setTestConnectionResult(null);
    setHostname('');
  }, [db?.title]);

  return (
    <Layout>
      <Title>Set Up a Database connection</Title>

      {db ? (
        <>
          <Space direction="vertical" size="large" style={{ width: '100%' }}>
            <Space size="middle">
              <SelectedDatabaseCard db={db} />

              <Button
                data-testid="wizard-change-db-btn"
                type="link"
                onClick={() => selectDatabase(null)}
              >
                Change
              </Button>
            </Space>

            <Typography>
              {db.instructions ? (
                <div dangerouslySetInnerHTML={{ __html: db.instructions }} />
              ) : (
                <Typography.Paragraph>
                  Enter database credentials to connect to your database. <br />
                  Cube.js will store your credentials into the <b><code>.env</code></b> file
                  for future use.
                </Typography.Paragraph>
              )}

              <Typography.Paragraph>
                For advanced configuration, use the <code>cube.js</code> configuration file inside
                mount volume or environment variables.<br />
                <Typography.Link href="https://cube.dev/docs/connecting-to-the-database" target="_blank">
                  Learn more about connecting to databases in the documentation.
                </Typography.Link>
              </Typography.Paragraph>

              {db.title === 'MongoDB' ? (
                <Alert
                  message="The MongoDB Connector for BI is required to connect to MongoDB."
                  type="info"
                />
              ) : null}
            </Typography>

            <Row gutter={[40, 12]}>
              <Col span={12}>
                <Space
                  direction="vertical"
                  size="large"
                  style={{ width: '100%' }}
                >
                  <DatabaseForm
                    db={db}
                    deployment={{}}
                    loading={isLoading}
                    disabled={isTestConnectionLoading}
                    hostname={hostname}
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

                        event('test_database_connection_success:frontend', {
                          database: db.title,
                        });

                        history.push('/schema');
                      } catch (error) {
                        setTestConnectionResult({
                          success: false,
                          error,
                        });

                        event('test_database_connection_error:frontend', {
                          error: error.message || error.toString(),
                          database: db.title,
                        });
                      }

                      setTestConnectionLoading(false);
                    }}
                  />

                  <ConnectionTest
                    loading={isTestConnectionLoading}
                    result={testConnectionResult}
                  />
                </Space>
              </Col>

              {['MySQL', 'PostgreSQL', 'Druid', 'ClickHouse'].includes(
                db?.title || ''
              ) && (
                <Col span={12}>
                  <LocalhostTipBox onHostnameCopy={setHostname} />
                </Col>
              )}
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
