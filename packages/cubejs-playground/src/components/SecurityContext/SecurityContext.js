import { useEffect, useRef, useState } from 'react';
import { Modal, Tabs, Input, Button, Space, Typography, Form } from 'antd';
import { CheckOutlined, CopyOutlined, EditOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { fetch } from 'whatwg-fetch';

import { useSecurityContext } from '../../hooks';
import CubejsIcon from '../../shared/icons/CubejsIcon';
import { copyToClipboard } from '../../utils';

const { TabPane } = Tabs;
const { TextArea } = Input;
const { Text, Link } = Typography;

const FlexBox = styled.div`
  display: flex;
  gap: 8px;

  input {
    text-overflow: ${(props) => (props.editing ? 'unset' : 'ellipsis')};
  }
`;

export default function SecurityContext() {
  const {
    payload,
    token,
    isModalOpen,
    setIsModalOpen,
    saveToken,
  } = useSecurityContext();

  const [form] = Form.useForm();
  const [editingToken, setEditingToken] = useState(!token);
  const [isJsonValid, setIsJsonValid] = useState(true);
  const [tmpPayload, setPayload] = useState(payload);
  const inputRef = useRef(null);

  useEffect(() => {
    if (editingToken) {
      inputRef.current?.focus();
    }
  }, [editingToken]);

  useEffect(() => {
    setEditingToken(!token);
    setPayload(payload);

    form.resetFields();
  }, [form, token, payload]);

  function handleTokenSave(values) {
    setEditingToken(false);
    saveToken(values.token);
  }

  function handlepayloadChange(event) {
    const { value } = event.target;
    setPayload(value);

    try {
      JSON.parse(value);
      setIsJsonValid(true);
    } catch (error) {
      setIsJsonValid(false);
    }
  }

  async function handlepayloadSave() {
    if (isJsonValid) {
      try {
        const response = await fetch('/playground/token', {
          method: 'post',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            payload: JSON.parse(tmpPayload),
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
      bodyStyle={{
        paddingTop: 16,
      }}
      onCancel={() => setIsModalOpen(false)}
    >
      <Space direction="vertical" size={24} style={{ width: '100%' }}>
        <Tabs
          defaultActiveKey="json"
          style={{ minHeight: 200 }}
          onChange={(tabKey) => {
            if (tabKey !== 'token' && editingToken) {
              setEditingToken(false);
              form.resetFields();
            }
          }}
        >
          <TabPane tab="JSON" key="json">
            <Space direction="vertical" size={16} style={{ width: '100%' }}>
              <TextArea
                value={tmpPayload}
                rows={10}
                style={{ width: '100%' }}
                onChange={handlepayloadChange}
              />

              <Button
                type="primary"
                disabled={!isJsonValid}
                onClick={handlepayloadSave}
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

              <Form
                form={form}
                initialValues={{
                  token,
                }}
                onFinish={handleTokenSave}
              >
                <FlexBox editing={editingToken}>
                  <Form.Item
                    name="token"
                    style={{
                      width: 'auto',
                      flexGrow: 1,
                    }}
                  >
                    <Input
                      ref={inputRef}
                      prefix={<CubejsIcon />}
                      disabled={!editingToken}
                    />
                  </Form.Item>

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
                        onClick={() => copyToClipboard(token)}
                      >
                        Copy
                      </Button>
                    </>
                  ) : (
                    <Button
                      type="primary"
                      icon={<CheckOutlined />}
                      htmlType="submit"
                    />
                  )}
                </FlexBox>
              </Form>
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
