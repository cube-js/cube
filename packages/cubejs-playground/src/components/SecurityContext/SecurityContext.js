import { createContext, useState } from 'react';
import { Modal, Tabs, Input, Button, Space, Typography } from 'antd';

import { useSecurityContext } from '../../hooks';
import { CopyOutlined, EditOutlined } from '@ant-design/icons';
import CubejsIcon from '../../shared/icons/CubejsIcon';

const { TabPane } = Tabs;
const { TextArea } = Input;
const { Text, Link } = Typography;

export const SecurityContextContext = createContext({
  claims: null,
  token: null,
  isValid: false,
  isModalOpen: false,
});

export function SecurityContextProvider({ children }) {
  const [token, setToken] = useState('eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2MTIzNTg3NTcsImV4cCI6MTYxMjQ0NTE1N30.Esx8sSxb16bJYJ-GKcC-fZ7PgvfzPJg8BjVwxXLlw6Y');
  const [claims, setClaims] = useState(null);
  const [isModalOpen, setIsModalOpen] = useState(true);

  return (
    <SecurityContextContext.Provider
      value={{
        claims,
        setClaims,
        token,
        setToken,
        isValid: false,
        isModalOpen,
        setIsModalOpen,
      }}
    >
      {children}
      <SecurityContext />
    </SecurityContextContext.Provider>
  );
}

export default function SecurityContext() {
  const {
    claims,
    token,
    isModalOpen,
    setIsModalOpen,
    setClaims,
    setToken,
  } = useSecurityContext();

  return (
    <Modal
      title="Security Context"
      visible={isModalOpen}
      footer={null}
      onCancel={() => setIsModalOpen(false)}
    >
      <Space direction="vertical" size={24} style={{ width: '100%' }}>
        <Tabs defaultActiveKey="json">
          <TabPane tab="JSON" key="json">
            <Space direction="vertical" size={16} style={{ width: '100%' }}>
              <TextArea cols={20} style={{ width: '100%' }} />

              <Button type="primary">Save</Button>
            </Space>
          </TabPane>

          <TabPane tab="Token" key="token">
            <Space direction="vertical" size={16} style={{ width: '100%' }}>
              <Text type="secondary">
                Edit or copy the generated token from below
              </Text>

              <Space style={{ display: 'flex', width: '100%' }}>
                <Input prefix={<CubejsIcon />} value={token} disabled style={{ width: '100%' }} />

                <Button ghost type="primary" icon={<EditOutlined />} />

                <Button type="primary" icon={<CopyOutlined />}>
                  Copy
                </Button>
              </Space>
            </Space>
          </TabPane>
        </Tabs>

        <Text type="secondary">
          Learn more about Security Context in{' '}
          <Link
            href="https://cube.dev/docs/security#security-context"
            target="_blank"
          >
            docs
          </Link>
        </Text>
      </Space>
    </Modal>
  );
}
