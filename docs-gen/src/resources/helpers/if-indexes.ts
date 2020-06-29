import MarkdownTheme from '../../theme';

export function ifIndexes(options) {
  return MarkdownTheme.handlebars.helpers.ifIndexes.call(this, options);
}
