import React from 'react';
import { Card, Button } from 'antd';
import { getParameters } from 'codesandbox-import-utils/lib/api/define';
import { map } from 'ramda';
import PrismCode from './PrismCode';
import { playgroundAction } from './events';

class ChartContainer extends React.Component {
  constructor(props) {
    super(props);
    this.state = { showCode: false };
  }

  render() {
    const { resultSet, error, codeExample, render, title, codeSandboxSource, dependencies } = this.props;

    const { getParameters } = require('codesandbox-import-utils/lib/api/define');

    const parameters = getParameters({
      files: {
        'index.js': {
          content: codeSandboxSource,
        },
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
    });

    const codeSandboxLink = `https://codesandbox.io/api/v1/sandboxes/define?parameters=${parameters}`;

    const extra =
      (<Button.Group>
        <Button
          onClick={() => {
            playgroundAction('Show Code');
            this.setState({ showCode: !this.state.showCode });
          }}
          icon="code"
          size="small"
          type={this.state.showCode ? 'primary' : 'default'}
        >
          {this.state.showCode ? 'Hide Code' : 'Show Code'}
        </Button>
        <Button
          href={codeSandboxLink}
          target="_blank"
          icon="code-sandbox"
          size="small"
          onClick={() => playgroundAction('Open Code Sandbox')}
        >
          Edit
        </Button>
      </Button.Group>);

    return (<Card
      title={title}
      style={{ minHeight: 420 }}
      extra={extra}
    >
      {this.state.showCode ? <PrismCode code={codeExample} /> : render({ resultSet, error })}
    </Card>);
  }
}

export default ChartContainer;
