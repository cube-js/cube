import { Alert, Form, Space, Typography } from 'antd';
import styled from 'styled-components';

import { CopiableInput } from '../../../components/CopiableInput';
import { copyToClipboard } from '../../../utils';

const StyledAlert: typeof Alert = styled(Alert)`
  border: none;
  padding: 16px;
`;

const StyledForm: typeof Form = styled(Form)`
  .ant-form-item {
    margin-bottom: 20px;
  }

  .ant-form-item:last-child {
    margin-bottom: 0;
  }

  .ant-form-item-label {
    padding-bottom: 4px;
  }
`;

type TLocalhostTipBoxProps = {
  onHostnameCopy: (value: string) => void;
};

export function LocalhostTipBox({ onHostnameCopy }: TLocalhostTipBoxProps) {
  return (
    <StyledAlert
      data-testid="wizard-localhost-tipbox"
      type="warning"
      message={
        <Space direction="vertical" size="middle">
          <Typography.Text>
            To connect to the database running on the localhost use the
            following <b>hostname</b> value
          </Typography.Text>

          <StyledForm>
            <CopiableInput
              label="Mac"
              value="host.docker.internal"
              onCopyClick={onHostnameCopy}
            />

            <CopiableInput
              label="Windows"
              value="host.docker.internal"
              onCopyClick={onHostnameCopy}
            />

            <CopiableInput
              label="Linux"
              value="localhost"
              onCopyClick={onHostnameCopy}
            />

            <Space direction="vertical" size="middle">
              <Typography.Text>
                Please note, for Linux, you need to run Cube Docker container
                in the{' '}
                <Typography.Link
                  href="https://docs.docker.com/network/host/"
                  target="_blank"
                >
                  network mode "host"
                </Typography.Link>{' '}
                to be able to connect to the database running on localhost.
              </Typography.Text>

              <CopiableInput
                value="docker run --network host"
                onCopyClick={(value) => copyToClipboard(value)}
              />
            </Space>
          </StyledForm>
        </Space>
      }
    />
  );
}
