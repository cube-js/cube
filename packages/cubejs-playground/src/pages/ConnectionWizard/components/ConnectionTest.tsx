import { Spin, Typography, Alert, Space } from 'antd';
import { CheckCircleFilled } from '@ant-design/icons';

type TConnectionTestResult = {
  success: boolean;
  error: Error | null;
};

type TConnectionTestProps = {
  loading: boolean;
  result: TConnectionTestResult | null;
};

export default function ConnectionTest({
  loading,
  result,
}: TConnectionTestProps) {
  if (loading) {
    return (
      <Space align="center" size="middle">
        <Spin data-testid="wizard-test-connection-spinner" />
        <Typography.Text>Testing database connection</Typography.Text>
      </Space>
    );
  }

  if (result?.success) {
    return (
      <Typography.Text type="success">
        <CheckCircleFilled />
        &nbsp;&nbsp;Connection successful
      </Typography.Text>
    );
  }

  if (result?.error) {
    return (
      <>
        <Typography.Text type="danger">
          We couldn’t connect. Please double check the provided data and try
          again
        </Typography.Text>

        <Alert
          data-testid="wizard-connection-error"
          style={{ marginTop: 20 }}
          message="Runtime Error"
          type="error"
          description={(() => (
            <>
              <p style={{ paddingLeft: 20 }}>
                Cube was unable to connect to the specified database.
              </p>
              <p style={{ paddingLeft: 20 }}>
                The database returned the following error:
              </p>
              <br />
              <p style={{ paddingLeft: 40 }}>&gt;Database Error</p>
              <p style={{ paddingLeft: 40 }}>{result.error.toString()}</p>
              <br />
              <p>
                Check your database credentials and try again. For more
                information, visit:
              </p>
              {/* eslint-disable-next-line */}
              <a
                href="https://cube.dev/cube-core/getting-started/create-a-project"
                target="_blank"
              >
                https://cube.dev/cube-core/getting-started/create-a-project
              </a>
            </>
          ))()}
        />
      </>
    );
  }

  return null;
}
