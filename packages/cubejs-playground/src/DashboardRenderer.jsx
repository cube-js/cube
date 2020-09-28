import React from 'react';
import cubejs from '@cubejs-client/core';
import * as cubejsReact from '@cubejs-client/react';
import * as antd from 'antd';
import { Spin } from 'antd';
import ChartContainer from './ChartContainer';
import { libraryToTemplate } from './ChartRenderer';

const DashboardRenderer = (props) => {
  const { source, sourceFiles } = props;
  const dependencies = {
    '@cubejs-client/core': cubejs,
    '@cubejs-client/react': cubejsReact,
    antd,
    react: React,
    ...Object.keys(libraryToTemplate)
      .map((k) => libraryToTemplate[k].imports)
      .reduce((a, b) => ({ ...a, ...b })),
  };
  return (
    <ChartContainer
      codeExample={source}
      codeSandboxSource={sourceFiles
        .map((f) => ({
          [f.fileName.split('/')[f.fileName.split('/').length - 1]]: {
            content: f.content,
          },
        }))
        .reduce((a, b) => ({ ...a, ...b }))}
      dependencies={dependencies}
      hideActions
      render={({ sandboxId }) =>
        (sandboxId && (
          <iframe
            src={`https://codesandbox.io/embed/${sandboxId}?fontsize=12&hidenavigation=1&editorsize=0`}
            style={{
              width: '100%',
              height: '100%',
              border: 0,
              borderRadius: 4,
              overflow: 'hidden',
            }}
            sandbox="allow-modals allow-forms allow-popups allow-scripts allow-same-origin"
            title="Dashboard"
          />
        )) || <Spin />
      }
    />
  );
};

export default DashboardRenderer;
