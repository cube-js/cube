import { Space, Typography, Button } from 'antd';
import { useState, useMemo } from 'react';
import styled from 'styled-components';

import { Alert } from '../../atoms/Alert';
import { generateAnsiHTML } from './utils';

const { Text, Paragraph } = Typography;

export const Code = styled.pre`
  padding: 0.4em 0.8em;
  font-size: 13px;
  white-space: pre-wrap;
  margin: 0;
`;

type FatalErrorProps = {
  error: Error | string;
  stack?: string | null;
};

export function FatalError({ error, stack }: FatalErrorProps) {
  const [visible, setVisible] = useState(false);
  
  const ansiHtmlError = useMemo(() => {
    return generateAnsiHTML(error.toString()).replace(/(Error:\s)/g, '');
  }, [error])

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
          <Space direction="vertical">
            <Code
              dangerouslySetInnerHTML={{
                __html: ansiHtmlError,
              }}
            />

            {stack ? (
              <>
                {!visible ? (
                  <Button danger ghost onClick={() => setVisible(true)}>
                    Show stack trace
                  </Button>
                ) : null}

                {visible && <pre>{stack}</pre>}
              </>
            ) : null}
          </Space>
        }
      />
    </Space>
  );
}
