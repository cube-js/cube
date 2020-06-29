import * as fs from 'fs-extra';
import * as Handlebars from 'handlebars';
import * as path from 'path';
import { MarkedLinksPlugin, ProjectReflection, Reflection } from 'typedoc';
import { Component, ContextAwareRendererComponent } from 'typedoc/dist/lib/output/components';
import { PageEvent, RendererEvent } from 'typedoc/dist/lib/output/events';
import * as Util from 'util';

import MarkdownTheme from '../theme';

/**
 * This component is essentially a combination of TypeDoc's 'MarkedPlugin' and 'MarkedLinksPlugin'.
 * The options are unchanged , but strips out all of the html configs.
 */

@Component({ name: 'helpers' })
export class ContextAwareHelpersComponent extends ContextAwareRendererComponent {
  /**
   * The path referenced files are located in.
   */
  private includes?: string;

  /**
   * Path to the output media directory.
   */
  private mediaDirectory?: string;

  /**
   * The pattern used to find references in markdown.
   */
  private includePattern: RegExp = /\[\[include:([^\]]+?)\]\]/g;

  /**
   * The pattern used to find media links.
   */
  private mediaPattern: RegExp = /media:\/\/([^ "\)\]\}]+)/g;

  /**
   * Regular expression for detecting bracket links.
   */
  private brackets: RegExp = /\[\[([^\]]+)\]\]/g;

  /**
   * Regular expression for detecting inline tags like {@link ...}.
   */
  private inlineTag: RegExp = /(?:\[(.+?)\])?\{@(link|linkcode|linkplain)\s+((?:.|\n)+?)\}/gi;

  private listInvalidSymbolLinks: boolean;

  private warnings: string[] = [];

  initialize() {
    super.initialize();

    this.includes = this.application.options.getValue('includes');
    this.mediaDirectory = this.application.options.getValue('media');
    this.listInvalidSymbolLinks = this.application.options.getValue('listInvalidSymbolLinks');

    this.listenTo(
      this.owner,
      {
        [RendererEvent.END]: this.onEndRenderer,
      },
      undefined,
      100,
    );

    const component = this;

    MarkdownTheme.handlebars.registerHelper('comment', function(this: string) {
      return component.parseComments(this);
    });

    MarkdownTheme.handlebars.registerHelper('breadcrumbs', function(this: PageEvent) {
      return component.breadcrumb(this.model, this.project, []);
    });

    MarkdownTheme.handlebars.registerHelper('relativeURL', (url: string) => {
      return url ? this.getRelativeUrl(url) : url;
    });
  }

  public breadcrumb(model: Reflection, project: ProjectReflection, md: string[]) {
    const theme = this.application.renderer.theme as MarkdownTheme;
    if (model && model.parent) {
      this.breadcrumb(model.parent, project, md);
      if (model.url) {
        md.push(`[${model.name}](${this.getRelativeUrl(model.url)})`);
      } else {
        md.push(model.url);
      }
    } else {
      if (!!project.readme) {
        md.push(`[${project.name}](${this.getRelativeUrl(theme.indexName + theme.fileExt)})`);
      }
      md.push(`[${project.readme ? 'Globals' : project.name}](${this.getRelativeUrl(project.url)})`);
    }
    return md.join(' › ');
  }

  /**
   * Parse the given comemnts string and return the resulting html.
   *
   * @param text  The markdown string that should be parsed.
   * @param context  The current handlebars context.
   * @returns The resulting html string.
   */
  public parseComments(text: string) {
    const context = Object.assign(text, '');

    if (this.includes) {
      text = text.replace(this.includePattern, (match: string, includesPath: string) => {
        includesPath = path.join(this.includes!, includesPath.trim());
        if (fs.existsSync(includesPath) && fs.statSync(includesPath).isFile()) {
          const contents = fs.readFileSync(includesPath, 'utf-8');
          if (includesPath.substr(-4).toLocaleLowerCase() === '.hbs') {
            const template = Handlebars.compile(contents);
            return template(context);
          } else {
            return contents;
          }
        } else {
          return '';
        }
      });
    }

    if (this.mediaDirectory) {
      text = text.replace(this.mediaPattern, (match: string, mediaPath: string) => {
        if (fs.existsSync(path.join(this.mediaDirectory!, mediaPath))) {
          return this.getRelativeUrl('media') + '/' + mediaPath;
        } else {
          return match;
        }
      });
    }

    return this.replaceInlineTags(this.replaceBrackets(text));
  }

  /**
   * Find all references to symbols within the given text and transform them into a link.
   *
   * This function is aware of the current context and will try to find the symbol within the
   * current reflection. It will walk up the reflection chain till the symbol is found or the
   * root reflection is reached. As a last resort the function will search the entire project
   * for the given symbol.
   *
   * @param text  The text that should be parsed.
   * @returns The text with symbol references replaced by links.
   */
  private replaceBrackets(text: string): string {
    return text.replace(this.brackets, (match: string, content: string): string => {
      const split = MarkedLinksPlugin.splitLinkText(content);
      return this.buildLink(match, split.target, split.caption);
    });
  }

  /**
   * Find symbol {@link ...} strings in text and turn into html links
   *
   * @param text  The string in which to replace the inline tags.
   * @return      The updated string.
   */
  private replaceInlineTags(text: string): string {
    return text.replace(this.inlineTag, (match: string, leading: string, tagName: string, content: string): string => {
      const split = MarkedLinksPlugin.splitLinkText(content);
      const target = split.target;
      const caption = leading || split.caption;
      const monospace = tagName === 'linkcode';

      return this.buildLink(match, target, caption, monospace);
    });
  }

  /**
   * Format a link with the given text and target.
   *
   * @param original   The original link string, will be returned if the target cannot be resolved..
   * @param target     The link target.
   * @param caption    The caption of the link.
   * @param monospace  Whether to use monospace formatting or not.
   * @returns A html link tag.
   */
  private buildLink(original: string, target: string, caption: string, monospace?: boolean): string {
    if (!this.urlPrefix.test(target)) {
      let reflection: Reflection | undefined;
      if (this.reflection) {
        reflection = this.reflection.findReflectionByName(target);
      } else if (this.project) {
        reflection = this.project.findReflectionByName(target);
      }

      if (reflection && reflection.url) {
        if (this.urlPrefix.test(reflection.url)) {
          target = reflection.url;
        } else {
          target = this.getRelativeUrl(reflection.url);
        }
      } else {
        const fullName = (this.reflection || this.project)!.getFullName();
        this.warnings.push(`In ${fullName}: ${original}`);
        return original;
      }
    }

    if (monospace) {
      caption = '`' + caption + '`';
    }

    return Util.format('[%s](%s)', caption, target);
  }

  /**
   * Triggered when [[Renderer]] is finished
   */
  onEndRenderer(event: RendererEvent) {
    if (this.listInvalidSymbolLinks && this.warnings.length > 0) {
      this.application.logger.write('');
      this.application.logger.warn(
        'Found invalid symbol reference(s) in JSDocs, ' +
          'they will not render as links in the generated documentation.',
      );

      for (const warning of this.warnings) {
        this.application.logger.write('  ' + warning);
      }
    }
  }
}
