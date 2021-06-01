import { Alert, Button, Form, Input, Space, Typography } from 'antd';
import { CopyOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { InputProps } from 'antd/lib/input/Input';
import { copyToClipboard } from '../../../utils';

const IconButton: typeof Button = styled(Button)`
  border: none;
  color: var(--primary-1);
`;

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

type TCopiableInputProps = {
  label?: string;
  onCopyClick: (value: string) => void;
} & InputProps;

function CopiableInput({
  label,
  value,
  onCopyClick,
  ...props
}: TCopiableInputProps) {
  const suffix = (
    <IconButton
      data-testid={`localhost-tipbox-${label?.toLowerCase()}-copy-btn`}
      icon={<CopyOutlined />}
      onClick={() => value && onCopyClick(value.toString())}
    />
  );

  return (
    <Form.Item
      label={label ? <b>{label}</b> : null}
      labelCol={{ span: 24 }}
      wrapperCol={{ span: 24 }}
    >
      <Input
        data-testid={`localhost-tipbox-${label?.toLowerCase()}-input`}
        value={value}
        suffix={suffix}
        {...props}
      />
    </Form.Item>
  );
}

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
                Please note, for Linux, you need to run Cube.js Docker container
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
