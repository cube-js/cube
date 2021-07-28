import { createGlobalStyle } from 'styled-components';

import { CSS_PROPERTIES } from '../variables';

const GlobalStyles = createGlobalStyle`
  body {
    ${Object.entries(CSS_PROPERTIES)
      .map(([key, value]) => {
        return `${key}: ${value};`;
      })
      .join('\n    ')}
  }

  body {
    margin: 0;
    padding: 0;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
    background: var(--layout-body-background);
    overflow-x: hidden;
  }

  .inline-code {
    margin: 0 1px;
    padding: 0.2em 0.4em;
    font-size: 0.9em;
    background: #f2f4f5;
    border: 1px solid #eee;
    border-radius: 3px;
  }
  
  .ant-dropdown-menu::-webkit-scrollbar-track {
    background: var(--light-color);
  }
  
  .ant-dropdown-menu::-webkit-scrollbar-thumb {
    border-radius: 4px;
    background: var(--dark-03-color);
  }
  
  .ant-dropdown-menu::-webkit-scrollbar {
    width: 8px;
    height: 8px;
  } 
  
  .schema-sidebar .ant-tabs-top-bar {
    padding: 0 16px;
  }
  
  .schema-sidebar .ant-menu {
    border: 0;
  }
  
  .ant-layout {
    height: 100%;
    background: var(--layout-body-background);
  }
  
  .ant-layout-header .ant-menu {
    height: 100%;
  }
  
  .ant-menu-horizontal .ant-menu-item {
    top: 0;
    height: 100%;
  }
  
  .ant-select-item.ant-select-item-option-selected:not(.ant-select-item-option-disabled) {
    color: white;
  }
  
  .ant-popover .ant-popover-inner-content {
    padding: 0;
  }
  
  
  code[class*="language-"],
  pre[class*="language-"] {
    color: var(--dark-01-color);
    background: none;
    /*text-shadow: 0 1px white;*/
    font-family: "Roboto Mono", Monaco, 'Andale Mono', 'Ubuntu Mono', monospace;
    text-align: left;
    font-weight: normal;
    font-size: 14px;
    line-height: 20px;
    white-space: pre;
    word-spacing: normal;
    word-break: normal;
    word-wrap: normal;
    border-radius: 4px;
    border: none;
  
    -moz-tab-size: 4;
    -o-tab-size: 4;
    tab-size: 4;
  
    -webkit-hyphens: none;
    -moz-hyphens: none;
    -ms-hyphens: none;
    hyphens: none;
  }

  code {
    font-family: source-code-pro, Menlo, Monaco, Consolas, "Courier New",
    monospace;
  }

  .schema-sidebar,
  .schema-sidebar .ant-tabs,
  .schema-sidebar .ant-tabs-content {
    height: 100%;
  }

  .schema-sidebar {
    background: #fff;
    border-right: 1px solid #eee;
    padding: 0;
  }

  .schema-sidebar .ant-tabs-tabpane {
    /* TODO: replace with flexbox */
    height: 85%;
    overflow: auto;
  }

  .ant-popover-disabled-compatible-wrapper {
    pointer-events: none;
  }
  
  pre[class*="language-"]::-moz-selection, pre[class*="language-"] ::-moz-selection,
  code[class*="language-"]::-moz-selection, code[class*="language-"] ::-moz-selection {
    text-shadow: none;
    /*background: #b3d4fc;*/
  }
  
  pre[class*="language-"]::selection, pre[class*="language-"] ::selection,
  code[class*="language-"]::selection, code[class*="language-"] ::selection {
    text-shadow: none;
    /*background: #b3d4fc;*/
  }
  
  @media print {
    code[class*="language-"],
    pre[class*="language-"] {
      text-shadow: none;
    }
  }
  
  /* Code blocks */
  pre[class*="language-"] {
    padding: 1em;
    overflow: auto;
  }
  
  :not(pre) > code[class*="language-"],
  pre[class*="language-"] {
    background: transparent;
  }
  
  /* Inline code */
  :not(pre) > code[class*="language-"] {
    padding: .1em;
    border-radius: .3em;
    white-space: normal;
  }
  
  .token.comment,
  .token.prolog,
  .token.doctype,
  .token.cdata {
    color: var(--dark-04-color);
  }
  
  .token.punctuation {
    color: var(--dark-04-color);
  }
  
  .namespace {
    opacity: .7;
  }
  
  .token.property,
  .token.tag,
  .token.boolean,
  .token.number,
  .token.constant,
  .token.symbol,
  .token.deleted {
    color: var(--pink-color);
  }
  
  .token.selector,
  .token.attr-name,
  .token.string,
  .token.char,
  .token.builtin,
  .token.inserted {
    color: var(--purple-color);
  }
  
  .token.operator,
  .token.entity,
  .token.url,
  .language-css .token.string,
  .style .token.string {
    color: var(--dark-01-color);
  }
  
  .token.atrule,
  .token.attr-value,
  .token.keyword {
    color: var(--dark-01-color);
  }
  
  .token.atrule,
  .token.keyword {
    font-weight: 500;
  }
  
  .token.function,
  .token.class-name {
    color: var(--pink-color);
  }
  
  .token.regex,
  .token.important,
  .token.variable {
    color: var(--pink-color);
  }
  
  .token.important,
  .token.bold {
    font-weight: bold;
  }
  
  .token.italic {
    font-style: italic;
  }
  
  .token.entity {
    cursor: help;
  }
  
  .missing-member-tooltip {
    max-width: none;
  }
  
  .ant-btn-link > span {
    text-decoration: underline;
  }
  
  a.ant-typography {
    text-decoration: underline;
  }
`;

export default GlobalStyles;
