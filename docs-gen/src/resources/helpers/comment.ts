import MarkdownTheme from '../../theme';

export function comment(this: string) {
  return MarkdownTheme.handlebars.helpers.comment.call(this);
}
