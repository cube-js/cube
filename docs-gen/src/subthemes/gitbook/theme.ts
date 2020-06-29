import * as fs from 'fs-extra';
import { RendererEvent } from 'typedoc/dist/lib/output/events';
import { Renderer } from 'typedoc/dist/lib/output/renderer';

import MarkdownTheme from '../../theme';

export default class GitbookTheme extends MarkdownTheme {
  constructor(renderer: Renderer, basePath: string) {
    super(renderer, basePath);
    this.listenTo(renderer, RendererEvent.END, this.writeSummary, 1024);
  }

  writeSummary(renderer: RendererEvent) {
    const outputDirectory = renderer.outputDirectory;
    const summaryMarkdown = this.getSummaryMarkdown(renderer);
    try {
      fs.writeFileSync(`${outputDirectory}/SUMMARY.md`, summaryMarkdown);
      this.application.logger.write(`[typedoc-plugin-markdown] SUMMARY.md written to ${outputDirectory}`);
    } catch (e) {
      this.application.logger.write(`[typedoc-plugin-markdown] failed to write SUMMARY at ${outputDirectory}`);
    }
  }

  getSummaryMarkdown(renderer: RendererEvent) {
    const md = [];
    md.push(`* [Globals](globals.md)`);
    this.getNavigation(renderer.project).children.forEach(rootNavigation => {
      if (rootNavigation.children) {
        md.push(`* [${rootNavigation.title}](${rootNavigation.url})`);
        rootNavigation.children.forEach(item => {
          md.push(`  * [${item.title}](${item.url})`);
        });
      }
    });
    return md.join('\n');
  }

  allowedDirectoryListings() {
    return ['README.md', 'globals.md', 'classes', 'enums', 'interfaces', 'modules', 'media', '.DS_Store', 'SUMMARY.md'];
  }
}
