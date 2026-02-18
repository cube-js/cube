import { Footer, Layout, Navbar } from 'nextra-theme-docs'
import { Head } from 'nextra/components'
import { getPageMap } from 'nextra/page-map'
import 'nextra-theme-docs/style.css'

export const metadata = {
  title: 'Cube Documentation',
  description: 'Cube documentation built with Nextra'
}

const navbar = (
  <Navbar
    logo={<b>Cube</b>}
  />
)

const footer = <Footer>MIT {new Date().getFullYear()} Cube Dev, Inc.</Footer>

export default async function RootLayout({ children }) {
  return (
    <html
      lang="en"
      dir="ltr"
      suppressHydrationWarning
    >
      <Head>
        {/* Additional head tags can be added here */}
      </Head>
      <body>
        <Layout
          navbar={navbar}
          pageMap={await getPageMap()}
          docsRepositoryBase="https://github.com/cube-js/cube/tree/master/docs_v4"
          footer={footer}
        >
          {children}
        </Layout>
      </body>
    </html>
  )
}
