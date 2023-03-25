import { Card, Layout, Space, Tabs, Typography } from 'antd';
import { CodeSnippet } from '../../atoms';
import { Content, Header } from '../components/Ui';

const { Paragraph, Link, Title } = Typography;

export function FrontendIntegrationsPage() {
  const token = 'token';
  const apiUrl = 'http://localhost:4000/cubejs-api';

  return (
    <Layout>
      <Header>
        <Title>Frontend Integrations</Title>
      </Header>

      <Content>
        <Space direction="vertical" size="large">
          <Paragraph>
            Learn more about{' '}
            <Link href="https://cube.dev/docs/rest-api" target="_blank">
              REST
            </Link>
            ,{' '}
            <Link href="https://cube.dev/docs/backend/graphql" target="_blank">
              GraphQL
            </Link>{' '}
            APIs and{' '}
            <Link
              href="https://cube.dev/docs/frontend-introduction"
              target="_blank"
            >
              integration with frontend frameworks
            </Link>
            .
          </Paragraph>

          <Tabs defaultActiveKey="1" size="small">
            <Tabs.TabPane key="terminal" tab="Terminal">
              <Space direction="vertical" size="large">
                <Card>
                  <Paragraph>REST API</Paragraph>

                  <CodeSnippet
                    theme="light"
                    code={`curl \\ 
  -H "Authorization: ${token}" \\ 
  -G \\ 
  --data-urlencode 'query={"measures":["LineItems.count"]}' \\ 
  ${apiUrl}/v1/load

`}
                  />
                </Card>

                <Card>
                  <Paragraph>GraphQL API</Paragraph>

                  <CodeSnippet
                    theme="light"
                    code={`curl \\ 
  -H "Authorization: ${token}" \\ 
  -G \\ 
  --data-urlencode 'query={"measures":["LineItems.count"]}' \\ 
  ${apiUrl}/v1/graphql

`}
                  />
                </Card>
              </Space>
            </Tabs.TabPane>

            <Tabs.TabPane key="vanilla-js" tab="Vanilla JS">
              <Space direction="vertical" size="large">
                <div>
                  <Paragraph>Init Cube API</Paragraph>

                  <CodeSnippet
                    theme="light"
                    code={`import cube from '@cubejs-client/core';
const cubeApi = cube(
  '${token}',
  { apiUrl: '${apiUrl}/v1' }
);`}
                  />
                </div>

                <div>
                  <Paragraph>Get the result set</Paragraph>

                  <CodeSnippet
                    theme="light"
                    code={`const resultSet = await cubejsApi.load({
  "measures":["LineItems.count"]
});`}
                  />
                </div>
              </Space>
            </Tabs.TabPane>

            <Tabs.TabPane key="react" tab="React">
              <Space direction="vertical" size="large">
                <div>
                  <Paragraph>Init Cube API</Paragraph>

                  <CodeSnippet
                    theme="light"
                    code={`import cube from '@cubejs-client/core';
const cubeApi = cube(
  '${token}',
  { apiUrl: '${apiUrl}/v1' }
);`}
                  />
                </div>

                <div>
                  <Paragraph>Declare CubeProvider</Paragraph>

                  <CodeSnippet
                    theme="light"
                    code={`import { CubeProvider } from '@cubejs-client/react';
// ...
<CubeProvider cubejsApi={cubejsApi}>...`}
                  />
                </div>

                <div>
                  <Paragraph>Get the result set</Paragraph>

                  <CodeSnippet
                    theme="light"
                    code={`import { useCubeQuery } from '@cubejs-client/react'; 
// ... 
const { resultSet, isLoading, error, progress } = useCubeQuery({ 
  "measures":["LineItems.count"] 
});`}
                  />
                </div>
              </Space>
            </Tabs.TabPane>

            <Tabs.TabPane key="angular" tab="Angular">
              <Space direction="vertical" size="large">
                <div>
                  <Paragraph>Set Cube options</Paragraph>

                  <CodeSnippet
                    theme="light"
                    code={`const cubejsOptions = { 
  token: '${token}', 
  options: { apiUrl: '${apiUrl}/v1' } 
}; `}
                  />
                </div>

                <Paragraph>
                  You can find full Angular tutorial and examples in{' '}
                  <Link
                    href="https://cube.dev/docs/@cubejs-client-ngx/"
                    target="_blank"
                  >
                    this documentation guide
                  </Link>
                  .
                </Paragraph>
              </Space>
            </Tabs.TabPane>

            <Tabs.TabPane key="vue" tab="Vue">
              <Space direction="vertical" size="large">
                <div>
                  <Paragraph>Init Cube API</Paragraph>

                  <CodeSnippet
                    theme="light"
                    code={`import cube from '@cubejs-client/core';
const cubeApi = cube(
  '${token}',
  { apiUrl: '${apiUrl}/v1' }
);`}
                  />
                </div>

                <Paragraph>
                  You can find full Angular tutorial and examples in{' '}
                  <Link
                    href="https://cube.dev/docs/@cubejs-client-ngx/"
                    target="_blank"
                  >
                    this documentation guide
                  </Link>
                  .
                </Paragraph>
              </Space>
            </Tabs.TabPane>
          </Tabs>
        </Space>
      </Content>
    </Layout>
  );
}
