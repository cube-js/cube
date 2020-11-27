/* eslint "no-console": "off" */

const path = require("path");
const gatsbyNode = require("guides-base/gatsby-node");
const siteConfig = require("./data/SiteConfig.js");

exports.onCreateNode = gatsbyNode.onCreateNode();
exports.createPages = gatsbyNode.createPages(siteConfig);
