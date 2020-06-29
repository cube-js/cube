import * as path from 'path';
import { NavigationItem } from 'typedoc';
import { Component, ContextAwareRendererComponent } from 'typedoc/dist/lib/output/components';
import { PageEvent } from 'typedoc/dist/lib/output/events';

@Component({ name: 'frontmatter' })
export class FrontMatterComponent extends ContextAwareRendererComponent {
  initialize() {
    super.initialize();

    this.listenTo(this.application.renderer, {
      [PageEvent.END]: this.onPageEnd,
    });
  }

  onPageEnd(page: PageEvent) {
    page.contents = page.contents.replace(/^/, this.getYamlString(page) + '\n\n').replace(/[\r\n]{3,}/g, '\n\n');
  }

  getYamlString(page: PageEvent) {
    const yaml = `---
id: "${this.escapeYAMLString(this.getId(page))}"
title: "${this.escapeYAMLString(this.getTitle(page))}"
sidebar_label: "${this.escapeYAMLString(this.getLabel(page))}"
---`;
    return yaml;
  }

  getId(page: PageEvent) {
    return this.stripExt(page.url);
  }

  getTitle(page: PageEvent) {
    if (page.url === page.project.url) {
      return this.getProjectName(page);
    }
    return this.getTitleFromNavigation(page, page.url) || this.getProjectName(page);
  }

  getLabel(page: PageEvent) {
    if (this.stripExt(page.url) === 'globals') {
      return 'Globals';
    }
    const title = this.getTitleFromNavigation(page, page.url);
    return title ? title : !!page.project.readme ? 'README' : 'Globals';
  }

  // prettier-ignore
  escapeYAMLString(str: string) {
    return str.replace(/([^\\])'/g, '$1\\\'');
  }

  getProjectName(page: PageEvent) {
    return (page.project.packageInfo && page.project.packageInfo.label) || page.project.name;
  }

  getTitleFromNavigation(page: PageEvent, url: string) {
    const item = this.findNavigationItem(page.navigation.children, url, null);
    return item ? item.title : null;
  }

  findNavigationItem(navigation: NavigationItem[], url, item: NavigationItem) {
    navigation.forEach(navigationChild => {
      if (navigationChild.url === url) {
        item = navigationChild;
        return;
      }
      if (navigationChild.children) {
        item = this.findNavigationItem(navigationChild.children, url, item);
      }
    });
    return item;
  }

  stripExt(url: string) {
    return path.basename(url, path.extname(url));
  }
}
