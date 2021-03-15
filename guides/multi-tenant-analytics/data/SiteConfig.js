const config = {
  siteTitle: "Multi-Tenant Analytics with Auth0 and Cube.js", // Site title.
  siteTitleShort: "Multi-Tenant Analytics with Auth0 and Cube.js", // Short site title for homescreen (PWA). Preferably should be under 12 characters to prevent truncation.
  siteTitleAlt: "Multi-Tenant Analytics with Auth0 and Cube.js", // Alternative site title for SEO.
  siteLogo: "/logos/icon.png", // Logo used for SEO and manifest.
  previewImage: "/logos/preview.png",
  siteUrl: "https://multi-tenant-analytics.cube.dev", // Domain of your website without pathPrefix.
  siteDescription: "We'll learn how to secure web applications with industry-standard and proven authentication mechanisms such as JSON Web Tokens, JSON Web Keys, OAuth 2.0 protocol. We'll start with an openly accessible, insecure analytical app and walk through a series of steps to turn it into a secure, multi-tenant app with role-based access control and an external authentication provider.", // Website description used for RSS feeds/meta description tag.
  googleAnalyticsID: "UA-70480064-3", // GA tracking ID.
  themeColor: "#c62828", // Used for setting manifest and progress theme colors.
  backgroundColor: "#e0e0e0", // Used for setting manifest background color.
  pathPrefix: "",
  githubUrl: "https://github.com/cube-js/cube.js/tree/master/guides/multi-tenant-analytics"
};

// Validate

// Make sure pathPrefix is empty if not needed
//if (config.pathPrefix === "/") {
//  config.pathPrefix = "";
//} else {
//  // Make sure pathPrefix only contains the first forward slash
//  config.pathPrefix = `/${config.pathPrefix.replace(/^\/|\/$/g, "")}`;
//}

// Make sure siteUrl doesn't have an ending forward slash
if (config.siteUrl.substr(-1) === "/")
  config.siteUrl = config.siteUrl.slice(0, -1);

// Make sure siteRss has a starting forward slash
if (config.siteRss && config.siteRss[0] !== "/")
  config.siteRss = `/${config.siteRss}`;

module.exports = config;
