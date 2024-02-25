/** @type {import('next-sitemap').IConfig} */
module.exports = {
  exclude: ['*/_meta', '/docs'],
  siteUrl: process.env.SITE_URL || 'https://cube.dev/docs',
  generateRobotsTxt: true, // (optional),
  robotsTxtOptions: {
    policies: [{ userAgent: '*', disallow: '/' }],
  },
  // ...other options
}
