import { Component, ContextAwareRendererComponent } from 'typedoc/dist/lib/output/components';

import MarkdownTheme from '../theme';

@Component({ name: 'options' })
export class OptionsComponent extends ContextAwareRendererComponent {
  initialize() {
    super.initialize();

    const namedAnchors = this.application.options.getValue('namedAnchors');
    const hideBreadcrumbs = this.application.options.getValue('hideBreadcrumbs');
    const hideIndexes = this.application.options.getValue('hideIndexes');
    const hideSourceFiles = this.application.options.getValue('hideSources');

    MarkdownTheme.handlebars.registerHelper('ifNamedAnchors', function(options) {
      return namedAnchors ? options.fn(this) : options.inverse(this);
    });

    MarkdownTheme.handlebars.registerHelper('ifBreadcrumbs', function(options) {
      return hideBreadcrumbs ? options.inverse(this) : options.fn(this);
    });

    MarkdownTheme.handlebars.registerHelper('ifIndexes', function(options) {
      return hideIndexes ? options.inverse(this) : options.fn(this);
    });

    MarkdownTheme.handlebars.registerHelper('ifSources', function(options) {
      return hideSourceFiles ? options.inverse(this) : options.fn(this);
    });
  }
}
