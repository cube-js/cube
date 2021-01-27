import { useState } from 'react';
import { Button, Typography, Upload, Space } from 'antd';
import { UploadOutlined } from '@ant-design/icons';

const { Text } = Typography;

export default function Base64Upload({ onInput, ...props }) {
  const [file, setFile] = useState('');
  const [error, setError] = useState('');

  const uploadProps = {
    name: 'file',
    headers: {
      authorization: 'authorization-text',
    },
    accept: 'application/json, .json',
    beforeUpload(file) {
      const reader = new FileReader();

      reader.onload = (event) => {
        let base64text = '';
        const fileContent = event.target.result.toString();

        try {
          JSON.parse(fileContent);

          base64text = btoa(fileContent);
        } catch (e) {
          setError('Invalid JSON file');

          console.error(e);
        }

        onInput &&
          onInput({
            encoded: base64text,
            raw: JSON.parse(fileContent),
          });
      };

      reader.readAsText(file);
      return false;
    },
    onChange(info) {
      if (info.file.status !== 'uploading') {
        console.log(info.file, info.fileList);
      }
      if (info.file.status === 'done') {
        setFile(info.file.name);
      } else if (info.file.status === 'error') {
        console.log(`${info.file.name} file upload failed.`);
        setError('Invalid file');
      }
    },
    onRemove() {
      setError('');
    }
  };

  return (
    <Space direction="vertical">
      <Upload {...uploadProps}>
        <Button icon={<UploadOutlined />}>Choose file</Button>
      </Upload>

      {!file && !error ? <Text type="secondary">No file selected</Text> : null}
      {error && <Text type="danger">{error}</Text>}
    </Space>
  );
}
