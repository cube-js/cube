import type { DocsThemeConfig } from "nextra-theme-docs";
import { components } from "@/components/mdx";
// import { CubeLogo } from "@/components/common/CubeLogo";
import { Footer } from "@/components/common/Footer";
import { SearchIcon, SearchTrigger } from '@cube-dev/marketing-ui';
import { MainLayout } from '@/components/layouts/MainLayout';
import { LogoWithVersion } from '@/components/common/LogoWithVersion/LogoWithVersion';

const repo = "https://github.com/cube-js/cube";
const branch = "master";
const path = "/docs/docs-new/";

const config: DocsThemeConfig = {
  logo: LogoWithVersion,
  logoLink: undefined,
  docsRepositoryBase: `${repo}/blob/${branch}${path}`,
  project: {
    link: repo,
  },
  head: (
    <>
      <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    </>
  ),
  useNextSeoProps: () => {
    return {
      description: "Documentation for Cube, the Semantic Layer for building data apps",
      titleTemplate: "%s | Cube Docs",
      openGraph: {
        description: "Documentation for Cube, the Semantic Layer for building data apps",
      },
      twitter: {
        handle: 'the_cube_dev',
        site: 'the_cube_dev',
      },
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
    component: <SearchTrigger><SearchIcon /></SearchTrigger>,
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
