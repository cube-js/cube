import nextra from 'nextra'
import path from 'path'
import redirects from './redirects.json' with { type: 'json' }

const withNextra = nextra({
  contentDirBasePath: '/',
  search: false,
  mdxOptions: {
    rehypePrettyCodeOptions: {
      theme: {
        light: 'one-light',
        dark: 'one-dark-pro'
      }
    }
  }
})

export default withNextra({
  basePath: process.env.BASE_PATH || '',
  async redirects() {
    return [
      {
        source: '/',
        destination: '/product/introduction',
        permanent: false
      },
      ...redirects
    ]
  },
  outputFileTracingRoot: import.meta.dirname,
  turbopack: {
    root: import.meta.dirname,
    resolveAlias: {
      'next-mdx-import-source-file': './mdx-components.jsx'
    }
  },
  webpack: (config) => {
    config.resolve.alias['next-mdx-import-source-file'] = path.resolve(import.meta.dirname, 'mdx-components.jsx')
    return config
  }
})
