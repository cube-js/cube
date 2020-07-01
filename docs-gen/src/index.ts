import { Application } from 'typedoc/dist/lib/application';
import { ParameterType } from 'typedoc/dist/lib/utils/options/declaration';

import { MarkdownPlugin } from './plugin';
import CubejsGroupPlugin from './CubejsGroupPlugin';

export = (PluginHost: Application) => {
  const app = PluginHost.owner;
  if (app.converter.hasComponent('markdown')) {
    return;
  }
  
  app.options.addDeclaration({
    help: 'Markdown Plugin: Deprecated in favour of theme.',
    name: 'platform',
    type: ParameterType.String,
  });

  app.options.addDeclaration({
    help: 'Markdown Plugin: Deprecated.',
    name: 'hideProjectTitle',
    type: ParameterType.Boolean,
  });

  app.options.addDeclaration({
    help: 'Markdown Plugin: Do not print source file link rendering.',
    name: 'hideSources',
    type: ParameterType.Boolean,
  });

  app.options.addDeclaration({
    help: 'Markdown Plugin: Do not print breadcrumbs.',
    name: 'hideBreadcrumbs',
    type: ParameterType.Boolean,
  });

  app.options.addDeclaration({
    help: 'Markdown Plugin: Do not print indexes.',
    name: 'hideIndexes',
    type: ParameterType.Boolean,
  });

  app.options.addDeclaration({
    help:
      'Markdown Plugin: Use HTML named anchors as fragment identifiers for engines that do not automatically assign header ids.',
    name: 'namedAnchors',
    type: ParameterType.Boolean,
  });

  app.options.addDeclaration({
    help:
      'Markdown Plugin: Use long navigation title instead of default short one (applicable to navigation / front-matter only).',
    name: 'longTitle',
    type: ParameterType.Boolean,
  });

  app.options.addDeclaration({
    help: 'Skips updating of the sidebar.json file when used with docusaurus or docusaurus2 theme',
    name: 'skipSidebar',
    type: ParameterType.Boolean,
    
  });

  app.converter.addComponent('markdown', new MarkdownPlugin(app.converter));
  
  app.converter.removeComponent('group');
  app.converter.addComponent('cubejs-group', new CubejsGroupPlugin(app.converter))
};
