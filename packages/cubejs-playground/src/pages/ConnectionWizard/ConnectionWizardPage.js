import { Col, PageHeader, Row, Typography } from 'antd';
import { useState } from 'react';
import styled from 'styled-components';

import envVarsDatabaseMap from '../../shared/env-vars-db-map';
import { fetchWithTimeout } from '../../utils';
import ConnectionTest from './components/ConnectionTest';
import DatabaseCard from './components/DatabaseCard';
import DatabaseForm from './components/DatabaseForm';

const { Title, Paragraph } = Typography;

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

function testConnection() {
  const wait = (delay = 2000) =>
    new Promise((resolve) => setTimeout(resolve, delay));
  let retries = 0;

  return new Promise((resolve, reject) => {
    async function retryFetch(url, options = {}, timeout = 1000) {
      try {
        const { error } = await (
          await fetchWithTimeout(url, options, timeout)
        ).json();
        error ? reject(error) : resolve();
      } catch (error) {
        if (retries >= 2) {
          reject(error);
        } else {
          await wait();
          retryFetch(url, options, timeout);
        }
      }
      retries++;
    }

    retryFetch('/playground/test-connection');
  });
}

const Layout = styled.div`
  width: auto;
  max-width: 960px;
  padding: 48px 24px;
  margin: 0 auto;
  background-color: #fff;
`;

async function saveConnection(variables) {
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

export default function ConnectionWizardPage({ history }) {
  const [isLoading, setLoading] = useState(false);
  const [isTestConnectionLoading, setTestConnectionLoading] = useState(false);
  const [testConnectionResult, setTestConnectionResult] = useState(null);
  const [db, selectDatabase] = useState(null);

  return (
    <Layout>
      <Title>Set Up a Database connection</Title>

      {db ? (
        <>
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
                    <span
                      dangerouslySetInnerHTML={{ __html: db.instructions }}
                    />
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
                loading={isLoading}
                disabled={isTestConnectionLoading}
                onCancel={() => undefined}
                onSubmit={async (variables) => {
                  setLoading(true);
                  await saveConnection(variables);
                  setLoading(false);

                  try {
                    setTestConnectionResult(null);
                    setTestConnectionLoading(true);
                    await testConnection();
                    setTestConnectionResult({
                      success: true,
                    });
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
          </Row>

          <Row>
            <Col span={12}>
              <ConnectionTest
                loading={isTestConnectionLoading}
                result={testConnectionResult}
              />
            </Col>
          </Row>
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
