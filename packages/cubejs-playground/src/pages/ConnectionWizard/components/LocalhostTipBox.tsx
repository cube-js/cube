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
} & InputProps;

function CopiableInput({ label, value, ...props }: TCopiableInputProps) {
  const suffix = (
    <IconButton
      icon={<CopyOutlined />}
      onClick={() => copyToClipboard(value)}
    />
  );

  return (
    <Form.Item
      label={label ? <b>{label}</b> : null}
      labelCol={{ span: 24 }}
      wrapperCol={{ span: 24 }}
    >
      <Input value={value} suffix={suffix} {...props} />
    </Form.Item>
  );
}

export function LocalhostTipBox() {
  return (
    <StyledAlert
      type="warning"
      message={
        <Space direction="vertical" size="middle">
          <Typography.Text>
            To connect to the database running on the localhost use the
            following <b>hostname</b> value
          </Typography.Text>

          <StyledForm>
            <CopiableInput label="Mac" value="host.docker.internal" />

            <CopiableInput label="Windows" value="docker.for.win.localhost" />

            <CopiableInput label="Linux" value="localhost" />

            <Space direction="vertical" size="middle">
              <Typography.Text>
                Please note, for Linux, you need to run Cube.js Docker container
                in the{' '}
                <Typography.Link href="https://docs.docker.com/network/host/" target="_blank">
                  network mode "host"
                </Typography.Link>{' '}
                to be able to connect to the database running on localhost.
              </Typography.Text>

              <CopiableInput value="docker run --network host" />
            </Space>
          </StyledForm>
        </Space>
      }
    />
  );
}
