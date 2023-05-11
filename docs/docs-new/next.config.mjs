import nextra from "nextra";
import remarkMath from "remark-math";
import remarkHtmlKatex from "remark-html-katex";
import linkEnvironmentVariables from "./plugins/link-environment-variables.mjs";

/**
 * @type {import('next').NextConfig}
 */
const config = {
  basePath: process.env.BASE_PATH || "",
  async redirects() {
    return [
      // {
      //   source: "/",
      //   destination: "/getting-started",
      //   permanent: true,
      // },
    ];
  },
};

const withNextra = nextra({
  theme: "nextra-theme-docs",
  themeConfig: "./theme.config.tsx",
  defaultShowCopyCode: true,
  mdxOptions: {
    remarkPlugins: [remarkMath, remarkHtmlKatex, linkEnvironmentVariables],
  },
});

export default withNextra(config);
