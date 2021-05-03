import { Space, Typography } from 'antd';
import styled from 'styled-components';

import { Alert } from './Alert';

const { Text, Paragraph } = Typography;

const Code = styled.pre`
  padding: 0.4em 0.8em;
  font-size: 13px;
  white-space: pre-wrap;
`;

type TFatalErrorProps = {
  error: Error;
};

export function FatalError({ error }: TFatalErrorProps) {
  return (
    <Space direction="vertical">
      <Text strong style={{ fontSize: 18 }}>
        Error 😢
      </Text>

      <Paragraph>
        Ask about it in{' '}
        <a
          href="https://slack.cube.dev"
          target="_blank"
          rel="noopener noreferrer"
        >
          Slack
        </a>
        . These guys know how to fix this for sure!
      </Paragraph>

      <Alert
        type="error"
        message={
          <Code
            dangerouslySetInnerHTML={{
              __html: error.toString(),
            }}
          />
        }
      />
    </Space>
  );
}
