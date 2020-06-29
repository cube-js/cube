import MarkdownTheme from '../../theme';

export function ifNamedAnchors(options) {
  return MarkdownTheme.handlebars.helpers.ifNamedAnchors.call(this, options);
}
