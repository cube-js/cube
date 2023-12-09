import type { DocsThemeConfig } from "nextra-theme-docs";
import { components } from "@/components/mdx";
// import { CubeLogo } from "@/components/common/CubeLogo";
import { Footer } from "@/components/common/Footer";
import { SearchIcon, SearchTrigger } from '@cube-dev/marketing-ui';
import { MainLayout } from '@/components/layouts/MainLayout';
import { LogoWithVersion } from '@/components/common/LogoWithVersion/LogoWithVersion';

const repo = "https://github.com/cube-js/cube";
const branch = "master";
const path = "/docs/";

const config: DocsThemeConfig = {
  logo: LogoWithVersion,
  logoLink: undefined,
  docsRepositoryBase: `${repo}/blob/${branch}${path}`,
  project: {
    link: repo,
  },
  chat: {
    link: 'https://slack.cube.dev',
    icon: <svg fill="#000000" width="24" height="24" viewBox="0 0 512 512" xmlns="http://www.w3.org/2000/svg"><title>ionicons-v5_logos</title><path d="M126.12,315.1A47.06,47.06,0,1,1,79.06,268h47.06Z"/><path d="M149.84,315.1a47.06,47.06,0,0,1,94.12,0V432.94a47.06,47.06,0,1,1-94.12,0Z"/><path d="M196.9,126.12A47.06,47.06,0,1,1,244,79.06v47.06Z"/><path d="M196.9,149.84a47.06,47.06,0,0,1,0,94.12H79.06a47.06,47.06,0,0,1,0-94.12Z"/><path d="M385.88,196.9A47.06,47.06,0,1,1,432.94,244H385.88Z"/><path d="M362.16,196.9a47.06,47.06,0,0,1-94.12,0V79.06a47.06,47.06,0,1,1,94.12,0Z"/><path d="M315.1,385.88A47.06,47.06,0,1,1,268,432.94V385.88Z"/><path d="M315.1,362.16a47.06,47.06,0,0,1,0-94.12H432.94a47.06,47.06,0,1,1,0,94.12Z"/></svg>
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
