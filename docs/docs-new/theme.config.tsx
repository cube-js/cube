import type { DocsThemeConfig } from "nextra-theme-docs";
import { components } from "@/components/mdx";
import { CubeLogo } from "@/components/common/CubeLogo";
import { Footer } from "@/components/common/Footer";
import { SearchTrigger } from "@cube-dev/marketing-ui";
import { MainLayout } from '@/components/layouts/MainLayout';

const repo = "https://github.com/cube-js/cube";
const branch = "master";
const path = "/docs/docs-new/";

const config: DocsThemeConfig = {
  logo: CubeLogo,
  docsRepositoryBase: `${repo}/blob/${branch}${path}`,
  project: {
    link: repo,
  },
  useNextSeoProps() {
    return {
      titleTemplate: "%s | Cube Docs",
    };
  },
  primaryHue: {
    light: 251,
    dark: 342,
  },
  components,
  main: MainLayout,
  sidebar: {
    defaultMenuCollapseLevel: 1,
    //
    // @TODO This is disabled for now because there's no way to modify the title
    // for the breadcrumb, which results in breadcrumbs like "Foo > `@cubejs-client/core`",
    // when instead we want the backticks to be processed as Markdown and transformed into
    // a code block.
    // titleComponent: ({ title }) => {
    //   const normalizedTitle = title.startsWith('`') && title.endsWith('`')
    //     ? title.replace(/`/g, '')
    //     : title;
    //   return (<>{normalizedTitle}</>);
    // },
  },
  search: {
    component: <SearchTrigger>Search</SearchTrigger>,
  },
  gitTimestamp: () => null,
  footer: {
    component: <Footer />,
  },
  darkMode: false,
  nextThemes: {
    defaultTheme: "light",
    forcedTheme: "light",
  },
};

export default config;
