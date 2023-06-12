/** @type {import('next-sitemap').IConfig} */
module.exports = {
  exclude: ['*/_meta'],
  siteUrl: process.env.SITE_URL || 'https://cube.dev/docs-next',
  generateRobotsTxt: true, // (optional)
  // ...other options
}
