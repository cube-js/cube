import React from 'react';
import { Card, Button } from 'antd';
import { getParameters } from 'codesandbox-import-utils/lib/api/define';
import { fetch } from 'whatwg-fetch';
import { map } from 'ramda';
import { Redirect } from 'react-router-dom';
import PrismCode from './PrismCode';
import { playgroundAction } from './events';

class ChartContainer extends React.Component {
  constructor(props) {
    super(props);
    this.state = { showCode: false };
  }

  async componentDidMount() {
    const {
      codeSandboxSource,
      dependencies,
      sandboxId
    } = this.props;
    const codeSandboxRes = await fetch("https://codesandbox.io/api/v1/sandboxes/define?json=1", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json"
      },
      body: JSON.stringify(this.codeSandboxDefinition(codeSandboxSource, dependencies))
    });
    const codeSandboxJson = await codeSandboxRes.json();
    this.setState({ sandboxId: codeSandboxJson.sandbox_id });
  }

  codeSandboxDefinition(codeSandboxSource, dependencies) {
    return {
      files: {
        ...(typeof codeSandboxSource === 'string' ? {
          'index.js': {
            content: codeSandboxSource,
          }
        } : codeSandboxSource),
        'package.json': {
          content: {
            dependencies: {
              'react-dom': 'latest',
              ...map(() => 'latest', dependencies)
            }
          },
        },
      },
      template: 'create-react-app'
    };
  }

  render() {
    const { redirectToDashboard, showCode, sandboxId, addingToDashboard } = this.state;
    const {
      resultSet,
      error,
      codeExample,
      render,
      title,
      codeSandboxSource,
      dependencies,
      dashboardSource,
      hideActions
    } = this.props;

    if (redirectToDashboard) {
      return <Redirect to="/dashboard" />;
    }

    const parameters = getParameters(this.codeSandboxDefinition(codeSandboxSource, dependencies));

    const extra = (
      <form action="https://codesandbox.io/api/v1/sandboxes/define" method="POST" target="_blank">
        <input type="hidden" name="parameters" value={parameters} />
        <Button.Group>
          {dashboardSource && (
            <Button
              onClick={async () => {
                playgroundAction('Add to Dashboard');
                this.setState({ addingToDashboard: true });
                await dashboardSource.addChart(codeExample);
                this.setState({ redirectToDashboard: true, addingToDashboard: false });
              }}
              icon="plus"
              size="small"
              loading={addingToDashboard}
            >
              {addingToDashboard ? 'Creating app and installing modules...' : 'Add to Dashboard'}
            </Button>
          )}
          <Button
            onClick={() => {
              playgroundAction('Show Code');
              this.setState({ showCode: !showCode });
            }}
            icon="code"
            size="small"
            type={showCode ? 'primary' : 'default'}
          >
            {showCode ? 'Hide Code' : 'Show Code'}
          </Button>
          <Button
            icon="code-sandbox"
            size="small"
            onClick={() => playgroundAction('Open Code Sandbox')}
            htmlType="submit"
          >
            Edit
          </Button>
        </Button.Group>
      </form>
    );

    return hideActions ? render({ resultSet, error, sandboxId }) : (
      <Card
        title={title}
        style={{ minHeight: 420 }}
        extra={extra}
      >
        {showCode ? <PrismCode code={codeExample} /> : render({ resultSet, error, sandboxId })}
      </Card>
    );
  }
}

export default ChartContainer;
