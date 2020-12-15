export enum MobileModes {
  CONTENT = 'content',
  MENU = 'menu',
  SEARCH = 'search',
}

export enum Scopes {
  DEFAULT = 'default',
  CUBEJS = 'cubejs',
}

export type Page = {
  scope: Scopes;
  category: string;
  mobileMode?: MobileModes;
  noscrollmenu?: boolean;
};

export type Section = {
  id: string;
  type: string;
  className?: string;
  nodes: any[];
  title: string;
};

export type SectionWithoutNodes = Omit<Section, 'className' | 'nodes'>;

export type SetScrollSectionsAndGithubUrlFunction = (
  sections: SectionWithoutNodes[],
  githubUrl: string
) => void;

export type Frontmatter = {
  permalink: string;
  title: string;
  menuTitle?: null;
  scope?: null;
  category: string;
  menuOrder?: number;
  subCategory?: string;
  frameworkOfChoice?: string;
};

export type MarkdownNode = {
  html: string;
  fileAbsolutePath: string;
  frontmatter: Frontmatter;
};

export type Category = {
  [categoryName: string]: MarkdownNode[];
};

export type ParsedNodeResults = {
  [key: string]: Category;
};
