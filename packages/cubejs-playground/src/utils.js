export function codeSandboxDefinition(template, files, dependencies = []) {
  return {
    files: {
      // 'test1.ts': {
      //   content: 'const x = 1;'
      // },
      // 'src/test1.ts': {
      //   content: 'const x = 1;'
      // },
      // 'src/app/test1.ts': {
      //   content: 'const x = 1;'
      // },
      ...Object.entries(files)
        .map(([fileName, content]) => ({ [fileName]: { content } }))
        .reduce((a, b) => ({ ...a, ...b }), {}),
      'package.json': {
        content: {
          dependencies: {
            // 'react-dom': 'latest',
            ...dependencies.reduce(
              (memo, d) => ({ ...memo, [d]: 'latest' }),
              {}
            ),
          },
        },
      },
    },
    template: 'angular-cli',
  };
}
