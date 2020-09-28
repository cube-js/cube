import React, { Component } from 'react';
import * as PropTypes from 'prop-types';
import cubejs from '@cubejs-client/core';
import {
  Layout,
  Menu,
  Button,
  Tree,
  Tabs,
  Spin,
  Alert,
  Modal,
  Empty,
} from 'antd';
import PrismCode from './PrismCode';
import { playgroundAction } from './events';

import fetch from './playgroundFetch';

const { Content, Sider } = Layout;

const { TreeNode } = Tree;
const { TabPane } = Tabs;

const schemasMap = {};
const schemaToTreeData = (schemas) =>
  Object.keys(schemas).map((schemaName) => ({
    title: schemaName,
    key: schemaName,
    children: Object.keys(schemas[schemaName]).map((tableName) => {
      const key = `${schemaName}.${tableName}`
      schemasMap[key] = [schemaName, tableName]
      return {
        title: tableName,
        key,
      }
    }),
  }));

class SchemaPage extends Component {
  constructor(props) {
    super(props);
    this.state = {
      expandedKeys: [],
      autoExpandParent: true,
      checkedKeys: [],
      selectedKeys: [],
      activeTab: 'schema',
      files: [],
    };
  }

  async componentDidMount() {
    await this.loadDBSchema();
    await this.loadFiles();
  }

  onExpand(expandedKeys) {
    playgroundAction('Expand Tables');
    this.setState({
      expandedKeys,
      autoExpandParent: false,
    });
  }

  onCheck(checkedKeys) {
    playgroundAction('Check Tables');
    this.setState({ checkedKeys });
  }

  onSelect(selectedKeys) {
    this.setState({ selectedKeys });
  }

  cubejsApi() {
    const { cubejsToken, apiUrl } = this.state;
    if (!this.cubejsApiInstance && cubejsToken) {
      this.cubejsApiInstance = cubejs(cubejsToken, {
        apiUrl: `${apiUrl}/cubejs-api/v1`,
      });
    }
    return this.cubejsApiInstance;
  }

  async loadDBSchema() {
    this.setState({ schemaLoading: true });
    try {
      const res = await fetch('/playground/db-schema');
      const result = await res.json();
      this.setState({
        tablesSchema: result.tablesSchema,
      });
    } catch (e) {
      this.setState({ schemaLoadingError: e });
    } finally {
      this.setState({ schemaLoading: false });
    }
  }

  async loadFiles() {
    const res = await fetch('/playground/files');
    const result = await res.json();
    this.setState({
      files: result.files,
    });
  }

  async generateSchema() {
    const { checkedKeys, tablesSchema } = this.state;
    const { history } = this.props;
    playgroundAction('Generate Schema');
    const res = await fetch('/playground/generate-schema', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        tables: checkedKeys.filter((k) => !!schemasMap[k]).map(e => schemasMap[e]),
        tablesSchema,
      }),
    });
    if (res.status === 200) {
      playgroundAction('Generate Schema Success');
      await this.loadFiles();
      this.setState({ checkedKeys: [], activeTab: 'files' });
      Modal.success({
        title: 'Schema files successfully generated!',
        content: 'You can start building the charts',
        okText: 'Build',
        cancelText: 'Close',
        okCancel: true,
        onOk() {
          history.push('/build');
        },
      });
    } else {
      playgroundAction('Generate Schema Fail', { error: await res.text() });
    }
  }

  selectedFileContent() {
    const file = this.selectedFile();
    return file && file.content;
  }

  selectedFile() {
    const { files, selectedFile } = this.state;
    return files.find((f) => f.fileName === selectedFile);
  }

  renderFilesMenu() {
    const { selectedFile, files } = this.state;
    return (
      <Menu
        mode="inline"
        onClick={({ key }) => {
          playgroundAction('Select File');
          this.setState({ selectedFile: key });
        }}
        selectedKeys={selectedFile ? [selectedFile] : []}
      >
        {files.map((f) => (
          <Menu.Item key={f.fileName}>{f.fileName}</Menu.Item>
        ))}
      </Menu>
    );
  }

  render() {
    const {
      schemaLoading,
      schemaLoadingError,
      tablesSchema,
      selectedFile,
      expandedKeys,
      autoExpandParent,
      checkedKeys,
      selectedKeys,
      activeTab,
    } = this.state;
    const renderTreeNodes = (data) =>
      data.map((item) => {
        if (item.children) {
          return (
            <TreeNode title={item.title} key={item.key} dataRef={item}>
              {renderTreeNodes(item.children)}
            </TreeNode>
          );
        }
        return <TreeNode {...item} />;
      });

    const renderTree = () =>
      Object.keys(tablesSchema || {}).length > 0 ? (
        <Tree
          checkable
          onExpand={this.onExpand.bind(this)}
          expandedKeys={expandedKeys}
          autoExpandParent={autoExpandParent}
          onCheck={this.onCheck.bind(this)}
          checkedKeys={checkedKeys}
          onSelect={this.onSelect.bind(this)}
          selectedKeys={selectedKeys}
        >
          {renderTreeNodes(schemaToTreeData(tablesSchema || {}))}
        </Tree>
      ) : (
        <Alert
          message="Empty DB Schema"
          description="Please check connection settings"
          type="warning"
        />
      );

    const renderTreeOrError = () =>
      schemaLoadingError ? (
        <Alert
          message="Error while loading DB schema"
          description={schemaLoadingError.toString()}
          type="error"
        />
      ) : (
        renderTree()
      );

    return (
      <Layout style={{ height: '100%' }}>
        <Sider width={320} className="schema-sidebar">
          <Tabs
            activeKey={activeTab}
            onChange={(tab) => this.setState({ activeTab: tab })}
            tabBarExtraContent={
              <Button
                disabled={!checkedKeys.length}
                type="primary"
                onClick={() => this.generateSchema()}
              >
                Generate Schema
              </Button>
            }
          >
            <TabPane tab="Tables" key="schema">
              {schemaLoading ? (
                <Spin style={{ width: '100%' }} />
              ) : (
                renderTreeOrError()
              )}
            </TabPane>
            <TabPane tab="Files" key="files">
              {this.renderFilesMenu()}
            </TabPane>
          </Tabs>
        </Sider>
        <Content style={{ minHeight: 280 }}>
          {selectedFile && (
            <Alert
              message={
                <span>
                  This file can be edited at&nbsp;
                  <b>{this.selectedFile().absPath}</b>
                </span>
              }
              type="info"
              style={{ paddingTop: 10, paddingBottom: 11 }}
            />
          )}
          {selectedFile ? (
            <PrismCode
              code={this.selectedFileContent()}
              style={{ padding: 12 }}
            />
          ) : (
            <Empty
              style={{ marginTop: 50 }}
              description="Select tables to generate Cube.js schema"
            />
          )}
        </Content>
      </Layout>
    );
  }
}

SchemaPage.propTypes = {
  history: PropTypes.object.isRequired,
};

export default SchemaPage;
