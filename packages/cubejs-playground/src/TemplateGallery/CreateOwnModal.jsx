import React from 'react';
import { CaretDownOutlined } from '@ant-design/icons';
import { Form } from '@ant-design/compatible';
import '@ant-design/compatible/assets/index.css';
import { Switch, Menu, Dropdown, Modal, Alert } from 'antd';
import styled from 'styled-components';
import { playgroundAction } from '../events';
import Button from '../components/Button';

const StyledModal = styled(Modal)`
  && {
    .ant-modal-header {
      border-bottom: none;
      padding: 40px 32px 0 32px;

      .ant-modal-title {
        font-size: 20px;
      }
    }

    .ant-modal-footer {
      border-top: none;
      padding: 0 32px 34px 32px;
      text-align: left;
    }
  }
`;

const StyledForm = styled(Form)`
  && {
    .ant-form-item-label {
      line-height: 16px;
      margin-bottom: 7px;
      label {
        font-weight: 500;
        font-size: 12px;
        line-height: 16px;
      }
    }

    .ant-dropdown-trigger {
      border-color: #ececf0;
      padding: 13px 16px;
      line-height: 13px;
    }
  }
`;

const DropdownIcon = () => <CaretDownOutlined style={{ color: '#727290' }} />;

const CreateOwnModal = ({
  visible,
  onOk,
  onCancel,
  onChange,
  chartLibraries,
  currentLibraryItem,
  frameworks,
  framework,
  frameworkItem,
  templatePackages,
  templatePackage,
  enableWebSocketTransport,
}) => {
  const chartLibrariesMenu = (
    <Menu
      onClick={(e) => {
        playgroundAction('Set Chart Library', { chartLibrary: e.key });
        onChange('chartLibrary', e.key);
      }}
    >
      {chartLibraries.map((library) => (
        <Menu.Item key={library.value}>{library.title}</Menu.Item>
      ))}
    </Menu>
  );

  const frameworkMenu = (
    <Menu
      onClick={(e) => {
        playgroundAction('Set Framework', { framework: e.key });
        onChange('framework', e.key);
      }}
    >
      {frameworks.map((f) => (
        <Menu.Item key={f.id}>{f.title}</Menu.Item>
      ))}
    </Menu>
  );

  const templatePackagesMenu = (
    <Menu
      onClick={(e) => {
        playgroundAction('Set Template Package', {
          templatePackageName: e.key,
        });
        onChange('templatePackageName', e.key);
      }}
    >
      {(templatePackages || []).map((f) => (
        <Menu.Item key={f.name}>{f.description}</Menu.Item>
      ))}
    </Menu>
  );

  return (
    <StyledModal
      title="Create your own Dashboard App"
      visible={visible}
      onOk={onOk}
      onCancel={onCancel}
      footer={[
        <Button key="submit" type="primary" onClick={onOk}>
          Ok
        </Button>,
        <Button key="back" onClick={onCancel}>
          Cancel
        </Button>,
      ]}
    >
      <StyledForm>
        <Form.Item label="Framework">
          <Dropdown overlay={frameworkMenu}>
            <Button>
              {frameworkItem && frameworkItem.title}
              <DropdownIcon />
            </Button>
          </Dropdown>
        </Form.Item>
        {frameworkItem && frameworkItem.docsLink && (
          <Alert
            style={{ marginBottom: 23 }}
            type="info"
            message={
              <span>
                We do not support&nbsp;
                {frameworkItem.title}
                &nbsp;dashboard scaffolding generation yet. Please refer
                to&nbsp;
                <a
                  href={frameworkItem.docsLink}
                  target="_blank"
                  rel="noopener noreferrer"
                  onClick={() =>
                    playgroundAction('Unsupported Dashboard Framework Docs', {
                      framework,
                    })
                  }
                >
                  {frameworkItem.title}
                  &nbsp;docs
                </a>
                &nbsp;to see on how to use it with Cube.js.
              </span>
            }
          />
        )}
        <Form.Item label="Main Template">
          <Dropdown
            overlay={templatePackagesMenu}
            disabled={!!frameworkItem.docsLink}
          >
            <Button>
              {templatePackage && templatePackage.description}
              <DropdownIcon />
            </Button>
          </Dropdown>
        </Form.Item>
        <Form.Item label="Charting Library">
          <Dropdown
            overlay={chartLibrariesMenu}
            disabled={!!frameworkItem.docsLink}
          >
            <Button>
              {currentLibraryItem && currentLibraryItem.title}
              <DropdownIcon />
            </Button>
          </Dropdown>
        </Form.Item>
        <Form.Item label="Web Socket Transport (Real-time)">
          <Switch
            checked={enableWebSocketTransport}
            onChange={(checked) =>
              onChange('enableWebSocketTransport', checked)
            }
          />
        </Form.Item>
      </StyledForm>
    </StyledModal>
  );
};

export default CreateOwnModal;
