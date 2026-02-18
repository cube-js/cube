import nextra from 'nextra'
import path from 'path'

const withNextra = nextra({
  contentDirBasePath: '/'
})

export default withNextra({
  async redirects() {
    return [
      {
        source: '/',
        destination: '/product/introduction',
        permanent: false
      }
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
