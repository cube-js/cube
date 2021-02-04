import { useEffect, useRef, useState } from 'react';
import { Modal, Tabs, Input, Button, Space, Typography } from 'antd';
import { CheckOutlined, CopyOutlined, EditOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { fetch } from 'whatwg-fetch';

import { useSecurityContext } from '../../hooks';
import CubejsIcon from '../../shared/icons/CubejsIcon';

const { TabPane } = Tabs;
const { TextArea } = Input;
const { Text, Link } = Typography;

const FlexBox = styled.div`
  display: flex;
  gap: 8px;
`;

export default function SecurityContext() {
  const {
    claims,
    token,
    isModalOpen,
    setIsModalOpen,
    saveToken,
  } = useSecurityContext();

  const [tmpToken, setToken] = useState(token || '');
  const [editingToken, setEditingToken] = useState(!token);
  const [isJsonValid, setIsJsonValid] = useState(true);
  const [tmpClaims, setClaims] = useState(claims);
  const inputRef = useRef(null);

  useEffect(() => {
    if (editingToken) {
      inputRef.current?.focus();
    }
  }, [editingToken]);

  useEffect(() => {
    setToken(token);
    setEditingToken(!token);
    setClaims(claims);
  }, [token, claims]);

  function handleTokenSave() {
    setEditingToken(false);
    saveToken(tmpToken);
  }

  function handleClaimsChange(event) {
    const { value } = event.target;
    setClaims(value);

    try {
      JSON.parse(value);
      setIsJsonValid(true);
    } catch (error) {
      setIsJsonValid(false);
    }
  }

  async function handleClaimsSave() {
    if (isJsonValid) {
      try {
        const response = await fetch('/playground/token', {
          method: 'post',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            claims: JSON.parse(tmpClaims),
          }),
        });
        const { token } = await response.json();
        saveToken(token);
      } catch (error) {
        console.error(error);
      }
    }
  }

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
              <TextArea
                value={tmpClaims}
                rows={10}
                style={{ width: '100%' }}
                onChange={handleClaimsChange}
              />

              <Button
                type="primary"
                disabled={!isJsonValid}
                onClick={handleClaimsSave}
              >
                Save
              </Button>
            </Space>
          </TabPane>

          <TabPane tab="Token" key="token">
            <Space direction="vertical" size={16} style={{ width: '100%' }}>
              <Text type="secondary">
                Edit or copy the generated token from below
              </Text>

              <FlexBox>
                <Input
                  ref={inputRef}
                  prefix={<CubejsIcon />}
                  value={tmpToken}
                  disabled={!editingToken}
                  style={{
                    width: 'auto',
                    flexGrow: 1,
                  }}
                  onChange={(event) => setToken(event.target.value)}
                />

                {!editingToken ? (
                  <>
                    <Button
                      ghost
                      type="primary"
                      icon={<EditOutlined />}
                      onClick={() => {
                        setEditingToken(true);
                      }}
                    />
                    <Button
                      type="primary"
                      icon={<CopyOutlined />}
                      disabled={!token}
                    >
                      Copy
                    </Button>
                  </>
                ) : (
                  <Button
                    type="primary"
                    icon={<CheckOutlined />}
                    onClick={handleTokenSave}
                  />
                )}
              </FlexBox>
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
