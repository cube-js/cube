import { editor } from 'monaco-editor';

export const MONOSPACE_FONT_FAMILY =
  'Menlo, Monaco, Consolas, "Ubuntu Mono", "Liberation Mono", "DejaVu Sans Mono", "Courier New", monospace';

export const LANGUAGE_MAP = {
  js: 'javascript',
  yml: 'yaml',
  yaml: 'yaml',
  ts: 'typescript',
  css: 'css',
  json: 'json',
  html: 'html',
  dockerfile: 'dockerfile',
  sql: 'sql',
  rust: 'rust',
  scss: 'scss',
  less: 'less',
  xml: 'xml',
  md: 'markdown',
  lua: 'lua',
  sh: 'shell',
  java: 'java',
  toml: 'toml',
  py: 'python',
  jinja: 'jinja',
};

export type FileExtension = keyof typeof LANGUAGE_MAP;

const PURPLE = '#665DE8';
const PINK = '#D3005A';
const GREEN = '#30A666';
const VIOLET = '#993388';
const DARK = '#141446';
const GREY = '#828282';
const BROWN = '#986801';

editor.defineTheme('cube', {
  base: 'vs',
  inherit: true,
  colors: {},
  rules: [
    { token: 'keyword.sql', foreground: PURPLE },
    { token: 'number.sql', foreground: BROWN },
    { token: 'constant.other.table-name.sql', foreground: PURPLE },
    { token: 'string.sql', foreground: GREEN },
    { token: 'comment.js', foreground: GREY },
    { token: 'comment.yaml', foreground: GREY },
    { token: 'identifier', foreground: DARK },
    { token: 'operators', foreground: GREEN },
    { token: 'number', foreground: GREEN },
    { token: 'number.yaml', foreground: PINK },
    { token: 'entity', foreground: GREEN },
    { token: 'keyword', foreground: PURPLE },
    { token: 'keyword.json', foreground: PURPLE },
    { token: 'string', foreground: PINK },
    { token: 'string.key.json', foreground: PINK },
    { token: 'string.yaml', foreground: PURPLE },
    { token: 'string.value.json', foreground: PURPLE },
    { token: 'regexp', foreground: PINK },
    { token: 'annotation', foreground: PINK },
    { token: 'type', foreground: DARK },
    { token: 'constant', foreground: PURPLE },
    { token: 'variable', foreground: PURPLE },
    { token: 'operator.sql', foreground: PURPLE },
    { token: 'delimiter', foreground: VIOLET },
    { token: 'operators.yaml', foreground: VIOLET },
    { token: 'tag', foreground: VIOLET },
    { token: 'key', foreground: VIOLET },
  ],
});
