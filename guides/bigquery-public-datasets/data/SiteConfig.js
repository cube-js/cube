const config = {
  siteTitle: "BigQuery Public Datasets for COVID-19 Impact Research", // Site title.
  siteTitleShort: "BigQuery Public Datasets for COVID-19 Impact Research", // Short site title for homescreen (PWA). Preferably should be under 12 characters to prevent truncation.
  siteTitleAlt: "BigQuery Public Datasets for COVID-19 Impact Research", // Alternative site title for SEO.
  siteLogo: "/logos/icon.png", // Logo used for SEO and manifest.
  previewImage: "/logos/preview.png",
  siteUrl: "https://bigquery-public-datasets.cube.dev", // Domain of your website without pathPrefix.
  siteDescription: "We'll explore how to build an analytical application on top of Google BigQuery, a serverless data warehouse, and use a few public datasets to visualize the impact of the COVID-19 pandemic on people's lives.", // Website description used for RSS feeds/meta description tag.
  googleAnalyticsID: "UA-70480064-3", // GA tracking ID.
  themeColor: "#c62828", // Used for setting manifest and progress theme colors.
  backgroundColor: "#e0e0e0", // Used for setting manifest background color.
  pathPrefix: "",
  githubUrl: "https://github.com/cube-js/cube.js/tree/master/guides/bigquery-public-datasets"
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
