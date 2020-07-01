import MarkdownTheme from '../../theme';

export function relativeURL(url: string) {
  return MarkdownTheme.handlebars.helpers.relativeURL(url);
}
