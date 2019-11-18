import React, { Component } from 'react';
import { playgroundAction } from "../events";
import styled from 'styled-components';
import { Switch, Button, Menu, Dropdown, Icon, Form, Row, Col, Card, Modal, Typography } from 'antd';

const StyledModal = styled(Modal)`
`

const CreateOwnDashboardForm = styled(Form)`
  && {
    .ant-modal-header {
      border-bottom: none;
    }
  }
`;

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
  enableWebSocketTransport
}) => {
  const chartLibrariesMenu = (
    <Menu
      onClick={(e) => {
        playgroundAction('Set Chart Library', { chartLibrary: e.key });
        onChange('chartLibrary', e.key);
      }}
    >
      {
        chartLibraries.map(library => (
          <Menu.Item key={library.value}>
            {library.title}
          </Menu.Item>
        ))
      }
    </Menu>
  );

  const frameworkMenu = (
    <Menu
      onClick={(e) => {
        playgroundAction('Set Framework', { framework: e.key });
        onChange('framework', e.key);
      }}
    >
      {
        frameworks.map(f => (
          <Menu.Item key={f.id}>
            {f.title}
          </Menu.Item>
        ))
      }
    </Menu>
  );

  const templatePackagesMenu = (
    <Menu
      onClick={(e) => {
        playgroundAction('Set Template Package', { templatePackageName: e.key });
        onChange('templatePackageName', e.key);
      }}
    >
      {
        (templatePackages || []).map(f => (
          <Menu.Item key={f.name}>
            {f.description}
          </Menu.Item>
        ))
      }
    </Menu>
  );
  return (
    <Modal
      title="Create your own Dashboard App"
      visible={visible}
      onOk={onOk}
      onCancel={onCancel}
    >
      <CreateOwnDashboardForm>
        <Form.Item label="Framework">
          <Dropdown overlay={frameworkMenu}>
            <Button>
              {frameworkItem && frameworkItem.title}
              <Icon type="down" />
            </Button>
          </Dropdown>
        </Form.Item>
        {
          frameworkItem && frameworkItem.docsLink && (
            <p style={{ paddingTop: 24 }}>
              We do not support&nbsp;
              {frameworkItem.title}
              &nbsp;dashboard scaffolding generation yet.
              Please refer to&nbsp;
              <a
                href={frameworkItem.docsLink}
                target="_blank"
                rel="noopener noreferrer"
                onClick={() => playgroundAction('Unsupported Dashboard Framework Docs', { framework })}
              >
                {frameworkItem.title}
                &nbsp;docs
              </a>
              &nbsp;to see on how to use it with Cube.js.
            </p>
          )
        }
        <Form.Item label="Main Template">
          <Dropdown
            overlay={templatePackagesMenu}
            disabled={!!frameworkItem.docsLink}
          >
            <Button>
              {templatePackage && templatePackage.description}
              <Icon type="down" />
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
              <Icon type="down" />
            </Button>
          </Dropdown>
        </Form.Item>
        <Form.Item label="Web Socket Transport (Real-time)">
          <Switch
            checked={enableWebSocketTransport}
            onChange={(checked) => onChange('enableWebSocketTransport', checked)}
          />
        </Form.Item>
      </CreateOwnDashboardForm>
    </Modal>
  );
};

export default CreateOwnModal;
