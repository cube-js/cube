import { Component } from 'react';
import { Layout, Button, Modal, Empty, Typography } from 'antd';
import { RouterProps } from 'react-router-dom';

import PrismCode from '../../PrismCode';
import { playgroundAction } from '../../events';
import { Menu, Tabs, Tree } from '../../components';
import { Alert, CubeLoader } from '../../atoms';
import { playgroundFetch } from '../../shared/helpers';
import { AppContextConsumer } from '../../components/AppContext';

const { Content, Sider } = Layout;

const { TreeNode } = Tree;
const { TabPane } = Tabs;

const schemasMap = {};
const schemaToTreeData = (schemas) =>
  Object.keys(schemas).map((schemaName) => ({
    title: schemaName,
    key: schemaName,
    treeData: Object.keys(schemas[schemaName]).map((tableName) => {
      const key = `${schemaName}.${tableName}`;
      schemasMap[key] = [schemaName, tableName];
      return {
        title: tableName,
        key,
      };
    }),
  }));

type SchemaPageProps = RouterProps;

export default class SchemaPage extends Component<SchemaPageProps, any> {
  constructor(props) {
    super(props);

    this.state = {
      expandedKeys: [],
      autoExpandParent: true,
      checkedKeys: [],
      selectedKeys: [],
      activeTab: 'schema',
      files: [],
      isDocker: null,
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

  async loadDBSchema() {
    this.setState({ schemaLoading: true });
    try {
      const res = await playgroundFetch('/playground/db-schema');
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
    const res = await playgroundFetch('/playground/files');
    const result = await res.json();
    this.setState({
      files: result.files,
    });
  }

  async generateSchema() {
    const { checkedKeys, tablesSchema } = this.state;
    const { history } = this.props;
    playgroundAction('Generate Schema');
    const res = await playgroundFetch('/playground/generate-schema', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        tables: checkedKeys
          .filter((k) => !!schemasMap[k])
          .map((e) => schemasMap[e]),
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
      isDocker,
    } = this.state;
    const renderTreeNodes = (data) =>
      data.map((item) => {
        if (item.treeData) {
          return (
            // @ts-ignore
            <TreeNode title={item.title} key={item.key} dataRef={item}>
              {renderTreeNodes(item.treeData)}
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
          data-testid="schema-error"
          message="Error while loading DB schema"
          description={schemaLoadingError.toString()}
          type="error"
        />
      ) : (
        renderTree()
      );

    return (
      <Layout style={{ height: '100%' }}>
        <Sider width={340} className="schema-sidebar">
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
              {schemaLoading ? <CubeLoader /> : renderTreeOrError()}
            </TabPane>

            <TabPane tab="Files" key="files">
              {this.renderFilesMenu()}
            </TabPane>
          </Tabs>
        </Sider>

        <Content
          style={{
            minHeight: 280,
            padding: 24,
          }}
        >
          {selectedFile && (
            <Alert
              message={
                isDocker ? (
                  <span>
                    Schema files are located and can be edited in the mount
                    volume directory.{' '}
                    <Typography.Link
                      href="https://cube.dev/docs/schema/getting-started"
                      target="_blank"
                    >
                      Learn more about working with Cube data schema in the
                      docs
                    </Typography.Link>
                  </span>
                ) : (
                  <span>
                    This file can be edited at&nbsp;
                    <b>{this.selectedFile().absPath}</b>
                  </span>
                )
              }
              type="info"
              style={{ paddingTop: 10, paddingBottom: 11 }}
            />
          )}
          {selectedFile ? (
            <PrismCode
              code={this.selectedFileContent()}
              style={{
                padding: 0,
                marginTop: 24,
              }}
            />
          ) : (
            <Empty
              style={{ marginTop: 50 }}
              description="Select tables to generate Cube schema"
            />
          )}

          <AppContextConsumer
            onReady={({ playgroundContext }) =>
              this.setState({ isDocker: playgroundContext?.isDocker })
            }
          />
        </Content>
      </Layout>
    );
  }
}
