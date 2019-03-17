import React, { Component } from 'react';
import cubejs from '@cubejs-client/core';
import { fetch } from 'whatwg-fetch';
import {
  Layout, Menu, Button, Tree, Tabs, Dropdown
} from 'antd';
import PrismCode from './PrismCode';
import { playgroundAction } from './events';

const {
  Content, Sider,
} = Layout;

const { TreeNode } = Tree;
const { TabPane } = Tabs;

const schemaToTreeData = (schemas) => Object.keys(schemas).map(schemaName => ({
  title: schemaName,
  key: schemaName,
  children: Object.keys(schemas[schemaName]).map(tableName => ({
    title: tableName,
    key: `${schemaName}.${tableName}`
  }))
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
      files: []
    };
  }

  cubejsApi() {
    if (!this.cubejsApiInstance && this.state.cubejsToken) {
      this.cubejsApiInstance = cubejs(this.state.cubejsToken, {
        apiUrl: `${this.state.apiUrl}/cubejs-api/v1`
      });
    }
    return this.cubejsApiInstance;
  }

  async componentDidMount() {
    await this.loadDBSchema();
    await this.loadFiles();
  }

  async loadDBSchema() {
    const res = await fetch('/playground/db-schema');
    const result = await res.json();
    this.setState({
      tablesSchema: result.tablesSchema
    });
  }

  async loadFiles() {
    const res = await fetch('/playground/files');
    const result = await res.json();
    this.setState({
      files: result.files
    });
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

  onSelect(selectedKeys, info) {
    this.setState({ selectedKeys });
  }

  async generateSchema() {
    playgroundAction('Generate Schema');
    const res = await fetch('/playground/generate-schema', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ tables: this.state.checkedKeys.filter(k => k.split('.').length === 2) })
    });
    if (res.status === 200) {
      playgroundAction('Generate Schema Success');
      await this.loadFiles();
      this.setState({ checkedKeys: [], activeTab: 'files' });
    } else {
      playgroundAction('Generate Schema Fail', { error: await res.text() });
    }
  }

  renderFilesMenu() {
    return (
      <Menu
      mode="inline"
      onClick={({ key }) => {
        playgroundAction('Select File');
        this.setState({ selectedFile: key });
      }}
      selectedKeys={this.state.selectedFile ? [this.state.selectedFile] : []}
      >
        {this.state.files.map(f => <Menu.Item key={f.fileName}>{f.fileName}</Menu.Item>)}
      </Menu>
    );
  }

  render() {
    const renderTreeNodes = data => data.map((item) => {
      if (item.children) {
        return (
          <TreeNode title={item.title} key={item.key} dataRef={item}>
            {renderTreeNodes(item.children)}
          </TreeNode>
        );
      }
      return <TreeNode {...item} />;
    });

    const menu = (
      <Menu>
        <Menu.Item onClick={() => this.generateSchema()}>
          Generate Schema
        </Menu.Item>
      </Menu>
    );

    return (
      <Layout style={{ height: '100%' }}>
        <Sider width={300} style={{ background: '#fff' }} className="schema-sidebar">
          <Tabs
            activeKey={this.state.activeTab}
             onChange={(activeTab) => this.setState({ activeTab })}
            tabBarExtraContent={(
              <Dropdown overlay={menu} placement="bottomRight" disabled={!this.state.checkedKeys.length}>
                <Button
                shape="circle"
                icon="plus"
                type="primary"
                style={{ marginRight: 8 }}
                />
              </Dropdown>
)}
          >
            <TabPane tab="Tables" key="schema">
              <Tree
                checkable
                onExpand={this.onExpand.bind(this)}
                expandedKeys={this.state.expandedKeys}
                autoExpandParent={this.state.autoExpandParent}
                onCheck={this.onCheck.bind(this)}
                checkedKeys={this.state.checkedKeys}
                onSelect={this.onSelect.bind(this)}
                selectedKeys={this.state.selectedKeys}
              >
                {renderTreeNodes(schemaToTreeData(this.state.tablesSchema || {}))}
              </Tree>
            </TabPane>
            <TabPane tab="Files" key="files">
              {this.renderFilesMenu()}
            </TabPane>
          </Tabs>
        </Sider>
        <Content style={{ minHeight: 280 }}>
          {this.state.selectedFile
            ? <PrismCode code={this.selectedFileContent()} style={{ padding: 12 }}/>
            : <h2 style={{ padding: 24, textAlign: 'center' }}>Select tables to generate Cube.js schema</h2>
          }

        </Content>
      </Layout>
    );
  }

  selectedFileContent() {
    const file = this.state.files.find(f => f.fileName === this.state.selectedFile);
    return file && file.content;
  }
}

export default SchemaPage;
