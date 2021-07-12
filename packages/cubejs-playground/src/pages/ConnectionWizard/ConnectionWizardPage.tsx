import { Alert, Col, Row, Space, Spin, Typography } from 'antd';
import { useEffect, useLayoutEffect, useState } from 'react';
import styled from 'styled-components';

import envVarsDatabaseMap from '../../shared/env-vars-db-map';
import { fetchPoll, fetchWithTimeout } from '../../utils';
import ConnectionTest from './components/ConnectionTest';
import { DatabaseCard, SelectedDatabaseCard } from './components/DatabaseCard';
import DatabaseForm from './components/DatabaseForm';
import { Button, FatalError } from '../../atoms';
import { LocalhostTipBox } from './components/LocalhostTipBox';
import { event, playgroundAction } from '../../events';
import { useAppContext } from '../../components/AppContext';

const { Title, Paragraph } = Typography;

const STATUS = {
  INSTALLING: 'installing',
  INSTALLED: 'installed',
};

const DatabaseCardWrapper = styled.div`
  cursor: pointer;
`;

const Layout = styled.div`
  width: auto;
  min-height: 100vh;
  max-width: 960px;
  padding: 48px 24px;
  margin: 0 auto;
  background-color: #fff;
`;

const databases: Database[] = envVarsDatabaseMap.reduce<any>(
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
  driver: string;
  logo: string;
  instructions?: string;
};

export function ConnectionWizardPage({ history }) {
  const { playgroundContext } = useAppContext();

  const [hostname, setHostname] = useState<string>('');
  const [isLoading, setLoading] = useState(false);
  const [isTestConnectionLoading, setTestConnectionLoading] = useState(false);
  const [testConnectionResult, setTestConnectionResult] = useState<any>(null);
  const [db, selectDatabase] = useState<Database | null>(null);
  const [isDriverInstallationInProgress, setDriverInstallationInProgress] =
    useState<boolean>(false);
  const [dependencyName, setDependencyName] = useState<string | null>(null);
  const [installationError, setInstallationError] = useState<string | null>(
    null
  );

  useEffect(() => {
    playgroundAction('connection_wizard_open');
  }, []);

  useLayoutEffect(() => {
    if (playgroundContext?.dbType && !playgroundContext?.isDocker) {
      selectDatabase(
        databases.find(
          (currentDb) =>
            currentDb.driver.toLowerCase() === playgroundContext.dbType
        ) || null
      );
    }
  }, [playgroundContext]);

  useEffect(() => {
    let fetchResult;

    if (isDriverInstallationInProgress && db) {
      fetchResult = fetchPoll(
        `/playground/driver?driver=${db.driver}`,
        1000,
        async ({ response, cancel }) => {
          const { status, error } = await response.json();

          if (response.ok && status === STATUS.INSTALLED) {
            cancel();
            setDriverInstallationInProgress(false);
          }

          if (!response.ok) {
            cancel();
            setDriverInstallationInProgress(false);
            setInstallationError(error);
          }
        }
      );
    }

    return () => {
      if (db) {
        fetchResult?.cancel();
      }
    };
  }, [db, isDriverInstallationInProgress]);

  useEffect(() => {
    setTestConnectionLoading(false);
    setTestConnectionResult(null);
    setInstallationError(null);
    setHostname('');
  }, [db?.driver]);

  function handleDatabaseSelect(db: Database) {
    return async () => {
      if (playgroundContext?.isDocker) {
        return selectDatabase(db);
      }

      {
        const response = await fetch(
          `/playground/driver?driver=${db.driver || ''}`
        );
        const { status, error } = await response.json();

        if (response.ok) {
          if (status === STATUS.INSTALLED) {
            return selectDatabase(db);
          } else if (status === STATUS.INSTALLING) {
            setDriverInstallationInProgress(true);
            selectDatabase(db);
          }
        } else {
          setInstallationError(error);
        }
      }

      {
        const response = await fetch('/playground/driver', {
          method: 'post',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            driver: db.driver,
          }),
        });

        const { dependency, error } = await response.json();

        if (response.ok) {
          setDependencyName(dependency);
          setDriverInstallationInProgress(true);
          selectDatabase(db);
        } else {
          setInstallationError(error);
        }
      }
    };
  }

  if (installationError) {
    return (
      <Layout>
        <FatalError error={installationError} />
      </Layout>
    );
  }

  if (isDriverInstallationInProgress && dependencyName) {
    return (
      <Layout>
        <Title>Set Up a Database connection</Title>

        <Space align="center" size="middle">
          <Spin />
          <Typography.Text>
            Installing <b>{dependencyName}</b>
          </Typography.Text>
        </Space>
      </Layout>
    );
  }

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
                  Cube.js will store your credentials into the <code>
                    .env
                  </code>{' '}
                  file for future use.
                </Typography.Paragraph>
              )}

              <Alert
                type="info"
                message={
                  <>
                    For advanced configuration, use the <b>cube.js</b> or <b>.env</b>{' '}
                    configuration file inside mount volume or environment
                    variables.
                    <br />
                    <Typography.Link
                      href="https://cube.dev/docs/connecting-to-the-database"
                      target="_blank"
                    >
                      Learn more about connecting to databases in the
                      documentation.
                    </Typography.Link>
                  </>
                }
              />

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
              ) && playgroundContext?.isDocker ? (
                <Col span={12}>
                  <LocalhostTipBox onHostnameCopy={setHostname} />
                </Col>
              ) : null}
            </Row>
          </Space>
        </>
      ) : (
        <>
          <Paragraph>Select a database type</Paragraph>

          <Row gutter={[12, 12]}>
            {databases.map((db) => (
              <Col xl={8} lg={8} md={12} sm={24} xs={24} key={db.title}>
                <DatabaseCardWrapper onClick={handleDatabaseSelect(db)}>
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
