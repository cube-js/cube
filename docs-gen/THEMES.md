# Markdown Themes

By default, the Markdown theme will attempt to render standard CommonMark, suitable for the majority of Markdown engines.
It follows the same structure and file patterns as the default HTML theme (see [typedoc-default-themes](https://github.com/TypeStrong/typedoc-default-themes)).

The plugin also comes packaged with some additional built-in themes and can also be extended with a custom theme.

- [Built-in themes](#built-in-themes)
- [Writing a custom Markdown theme](#writing-a-custom-markdown-theme)

## Writing a custom markdown theme

The Markdown theme packaged with the plugin can also be extended with a custom Markdown theme using the standard TypeDoc theming pattern as per https://typedoc.org/guides/themes/.

### Create a theme.js class

As per the theme docs create a `theme.js` file which TypeDoc will then attempt to load from a given location.

_mytheme/custom-theme.js_

```js
const MarkdownTheme = require('typedoc-plugin-markdown/dist/theme');

class CustomMarkdownTheme extends MarkdownTheme.default {
  constructor(renderer, basePath) {
    super(renderer, basePath);
  }
}

exports.default = CustomMarkdownTheme;
```

### Theme resources

By default the theme will inherit the resources of the Markdown theme (https://github.com/tgreyuk/typedoc-plugin-markdown/tree/master/src/resources).

These can be replaced and updated as required.

### Building the theme

#### CLI

```
npx typedoc ./src --plugin typedoc-plugin-markdown --theme ./mytheme/custom-theme --out docs
```

#### API

```js
const { Application } = require('typedoc');
const path = require('path');

const app = new Application();
app.bootstrap({
  module: 'CommonJS',
  target: 'ES5',
  readme: 'none',
  theme: path.join(__dirname, 'mytheme', 'custom-theme'),
  plugin: 'typedoc-plugin-markdown',
});

app.generateDocs(app.expandInputFiles(['./src']), 'docs');
```

See https://typedoc.org/guides/installation/#node-module

## Built-in themes

### `docusaurus` / `docusaurus2`

The --out path is assumed be a Docusaurus docs directory.

- Adds Front Matter to pages to support Docusaurus [Markdown Headers](https://docusaurus.io/docs/en/doc-markdown#markdown-headers).
- Appends releavant JSON to website/sidebars.json|sidebars.js, to support [sidebar navigation](https://docusaurus.io/docs/en/navigation).

#### Output

```
root-directory
├── docs
│   ├── myapi
│   |   ├── classes
│   │   ├── enums
│   │   ├── interfaces
│   │   ├── index.md
│   │
└── website
    ├── sidebars.json

```

#### Adding links in siteconfig

Manually add the index page to headerLinks in the [siteConfig.js](https://docusaurus.io/docs/en/site-config) to access the api from header.

```js
headerLinks: [
  { doc: "myapi/index", label: "My API" },
],
```

### `vuepress`

- Adds Front Matter to pages.
- The --out path is assumed be a Vuepress docs directory.
- Will create:

  - `.vuepress/api-sidebar.json` to be used with [sidebar](https://vuepress.vuejs.org/default-theme-config/#sidebar).
  - `.vuepress/api-sidebar-relative.json` to be used with [multiple sidebars](https://vuepress.vuejs.org/default-theme-config/#multiple-sidebars).
  - `.vuepress/config.json`

#### Examples

```js
const apiSideBar = require('./api-sidebar.json');

// Without groups
module.exports = {
  themeConfig: {
    sidebar: ['some-content', ...apiSideBar],
  },
};

// With groups
module.exports = {
  themeConfig: {
    sidebar: ['some-content', { title: 'API', children: apiSideBar }],
  },
};
```

```js
const apiSideBarRelative = require('./api-sidebar-relative.json');

// Multiple sidebars
module.exports = {
  themeConfig: {
    sidebar: {
      '/guide/': ['some-content'],
      '/api/': apiSideBarRelative,
      '/': ['other'],
    },
  },
};
```

### `bitbucket`

_Note: this theme applicable to Bitbucket Cloud. If using Bitbucket Server please use the `--namedAnchors` argument to fix anchor links._

- Parses internal anchor links to support Bitbucket's internal anchor linking.
