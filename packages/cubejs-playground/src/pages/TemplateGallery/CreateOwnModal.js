import { CaretDownOutlined } from '@ant-design/icons';
import { Form } from '@ant-design/compatible';
import '@ant-design/compatible/assets/index.css';
import { Switch, Menu, Dropdown, Modal, Alert } from 'antd';
import styled from 'styled-components';

import { playgroundAction } from '../../events';
import Button from '../../components/Button';

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
      padding: 5px 12px;
    }
  }
`;

const StyledFormItem = styled(Form.Item)`
  && {
    &:not(:last-child) {
      margin-bottom: 16px;
    }
    
    &:last-child {
      margin-bottom: 0;
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
        <StyledFormItem label="Framework">
          <Dropdown overlay={frameworkMenu}>
            <Button>
              {frameworkItem && frameworkItem.title}
              <DropdownIcon />
            </Button>
          </Dropdown>
        </StyledFormItem>
        {!frameworkItem?.scaffoldingSupported && (
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
        
        <StyledFormItem label="Main Template">
          <Dropdown
            overlay={templatePackagesMenu}
            disabled={!frameworkItem.scaffoldingSupported}
          >
            <Button>
              {templatePackage && templatePackage.description}
              <DropdownIcon />
            </Button>
          </Dropdown>
        </StyledFormItem>
        
        <StyledFormItem label="Charting Library">
          <Dropdown
            overlay={chartLibrariesMenu}
            disabled={!frameworkItem.scaffoldingSupported}
          >
            <Button>
              {currentLibraryItem && currentLibraryItem.title}
              <DropdownIcon />
            </Button>
          </Dropdown>
        </StyledFormItem>
        
        <StyledFormItem label="Web Socket Transport (Real-time)">
          <Switch
            disabled={framework.toLowerCase() === 'angular'}
            checked={enableWebSocketTransport}
            onChange={(checked) =>
              onChange('enableWebSocketTransport', checked)
            }
          />
        </StyledFormItem>
      </StyledForm>
    </StyledModal>
  );
};

export default CreateOwnModal;
