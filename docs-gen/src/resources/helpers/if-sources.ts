import MarkdownTheme from '../../theme';

export function ifSources(options) {
  return MarkdownTheme.handlebars.helpers.ifSources.call(this, options);
}
