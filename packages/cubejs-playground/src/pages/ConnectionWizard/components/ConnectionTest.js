import { Spin, Typography, Alert } from 'antd';
import { CheckCircleFilled } from '@ant-design/icons';

export default function ConnectionTest({ loading, result }) {
  if (loading) {
    return (
      <>
        <Spin />
        &nbsp;&nbsp;Testing database connection
      </>
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
          style={{ marginTop: 20 }}
          message="Runtime Error"
          type="error"
          description={(() => (
            <>
              <p style={{ paddingLeft: 20 }}>
                Cube.js was unable to connect to the specified database.
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
                href="https://cube.dev/docs/connecting-to-the-database"
                target="_blank"
              >
                https://cube.dev/docs/connecting-to-the-database
              </a>
            </>
          ))()}
        />
      </>
    );
  }

  return null;
}
