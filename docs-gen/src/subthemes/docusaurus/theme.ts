import * as fs from 'fs-extra';
import * as path from 'path';
import { RendererEvent } from 'typedoc/dist/lib/output/events';
import { Renderer } from 'typedoc/dist/lib/output/renderer';

import { FrontMatterComponent } from '../../components/front-matter.component';
import MarkdownTheme from '../../theme';

export default class DocusaurusTheme extends MarkdownTheme {
  constructor(renderer: Renderer, basePath: string) {
    super(renderer, basePath);
    this.indexName = 'index';
    renderer.addComponent('frontmatter', new FrontMatterComponent(renderer));
    this.listenTo(renderer, RendererEvent.END, this.onRendererEnd, 1024);
  }

  onRendererEnd(renderer: RendererEvent) {
    if (!this.application.options.getValue('skipSidebar')) {
      const docusarusRoot = this.findDocusaurusRoot(renderer.outputDirectory);
      if (docusarusRoot === null) {
        this.application.logger.warn(
          `[typedoc-markdown-plugin] sidebars.json not written as could not locate docusaurus root directory. In order to to implemnent sidebars.json functionality, the output directory must be a child of a 'docs' directory.`,
        );
        return;
      }
      this.writeSideBar(renderer, docusarusRoot);
    }
  }

  writeSideBar(renderer: RendererEvent, docusarusRoot: string) {
    const childDirectory = renderer.outputDirectory.split(docusarusRoot + 'docs/')[1];
    const docsRoot = childDirectory ? childDirectory + '/' : '';
    const websitePath = docusarusRoot + 'website';
    const packageName = renderer.project.packageInfo.name;
    const navObject = this.getNavObject(renderer, docsRoot);
    const sidebarPath = websitePath + '/sidebars.json';
    let contents: any;
    if (!fs.existsSync(sidebarPath)) {
      contents = '{}';
      if (!fs.existsSync(websitePath)) {
        fs.mkdirSync(websitePath);
      }
    } else {
      contents = fs.readFileSync(sidebarPath);
    }
    const jsonContent = JSON.parse(contents.toString());
    const update = {
      ...jsonContent,
      [packageName]: navObject,
    };
    try {
      fs.writeFileSync(sidebarPath, JSON.stringify(update, null, 2));
      this.application.logger.write(`[typedoc-plugin-markdown] sidebars.json updated at ${sidebarPath}`);
    } catch (e) {
      this.application.logger.write(`[typedoc-plugin-markdown] failed to update sidebars.json at ${sidebarPath}`);
    }
  }

  getNavObject(renderer: RendererEvent, docsRoot: string) {
    const projectUrls = [docsRoot + this.indexName.replace('.md', '')];
    if (renderer.project.url === 'globals.md') {
      projectUrls.push(docsRoot + 'globals');
    }
    const navObject = {
      ['Introduction']: projectUrls,
    };

    this.getNavigation(renderer.project).children.forEach(rootNavigation => {
      navObject[rootNavigation.title] = rootNavigation.children.map(item => {
        return docsRoot + item.url.replace('.md', '');
      });
    });

    return navObject;
  }

  findDocusaurusRoot(outputDirectory: string) {
    const docsName = 'docs';
    function splitPath(dir: string) {
      const parts = dir.split(/(\/|\\)/);
      if (!parts.length) {
        return parts;
      }
      return !parts[0].length ? parts.slice(1) : parts;
    }
    function testDir(parts) {
      if (parts.length === 0) {
        return null;
      }
      const p = parts.join('');
      const itdoes = fs.existsSync(path.join(p, docsName));
      return itdoes ? p : testDir(parts.slice(0, -1));
    }
    return testDir(splitPath(outputDirectory));
  }
}
