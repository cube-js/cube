import { PageEvent } from 'typedoc/dist/lib/output/events';

import MarkdownTheme from '../../theme';

export function ifBreadcrumbs(this: PageEvent, options) {
  if (MarkdownTheme.isSingleFile) {
    return options.inverse(this);
  }
  return MarkdownTheme.handlebars.helpers.ifBreadcrumbs.call(this, options);
}
