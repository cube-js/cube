const React = require(`react`)
const fs = require(`fs`)
const { join } = require(`path`)
const { renderToString, renderToStaticMarkup } = require(`react-dom/server`)
const { ServerLocation, Router, isRedirect } = require(`@reach/router`)
const {
  get,
  merge,
  isObject,
  flatten,
  uniqBy,
  flattenDeep,
  replace,
  concat,
  memoize,
} = require(`lodash`)

const { RouteAnnouncerProps } = require(`./route-announcer-props`)
const apiRunner = require(`./api-runner-ssr`)
const syncRequires = require(`$virtual/sync-requires`)
const { version: gatsbyVersion } = require(`gatsby/package.json`)
const { grabMatchParams } = require(`./find-path`)

const stats = JSON.parse(
  fs.readFileSync(`${process.cwd()}/public/webpack.stats.json`, `utf-8`)
)

const chunkMapping = JSON.parse(
  fs.readFileSync(`${process.cwd()}/public/chunk-map.json`, `utf-8`)
)

// const testRequireError = require("./test-require-error")
// For some extremely mysterious reason, webpack adds the above module *after*
// this module so that when this code runs, testRequireError is undefined.
// So in the meantime, we'll just inline it.
const testRequireError = (moduleName, err) => {
  const regex = new RegExp(`Error: Cannot find module\\s.${moduleName}`)
  const firstLine = err.toString().split(`\n`)[0]
  return regex.test(firstLine)
}

let Html
try {
  Html = require(`../src/html`)
} catch (err) {
  if (testRequireError(`../src/html`, err)) {
    Html = require(`./default-html`)
  } else {
    throw err
  }
}

Html = Html && Html.__esModule ? Html.default : Html

const getPageDataPath = path => {
  const fixedPagePath = path === `/` ? `index` : path
  return join(`page-data`, fixedPagePath, `page-data.json`)
}

const getPageDataUrl = pagePath => {
  const pageDataPath = getPageDataPath(pagePath)
  return `${__PATH_PREFIX__}/${pageDataPath}`
}

const getStaticQueryUrl = hash =>
  `${__PATH_PREFIX__}/page-data/sq/d/${hash}.json`

const getPageData = pagePath => {
  const pageDataPath = getPageDataPath(pagePath)
  const absolutePageDataPath = join(process.cwd(), `public`, pageDataPath)
  const pageDataRaw = fs.readFileSync(absolutePageDataPath)

  try {
    return JSON.parse(pageDataRaw.toString())
  } catch (err) {
    return null
  }
}

const appDataPath = join(`page-data`, `app-data.json`)

const getAppDataUrl = memoize(() => {
  let appData

  try {
    const absoluteAppDataPath = join(process.cwd(), `public`, appDataPath)
    const appDataRaw = fs.readFileSync(absoluteAppDataPath)
    appData = JSON.parse(appDataRaw.toString())

    if (!appData) {
      return null
    }
  } catch (err) {
    return null
  }

  return `${__PATH_PREFIX__}/${appDataPath}`
})

const loadPageDataSync = pagePath => {
  const pageDataPath = getPageDataPath(pagePath)
  const pageDataFile = join(process.cwd(), `public`, pageDataPath)
  try {
    const pageDataJson = fs.readFileSync(pageDataFile)
    return JSON.parse(pageDataJson)
  } catch (error) {
    // not an error if file is not found. There's just no page data
    return null
  }
}

const createElement = React.createElement

export const sanitizeComponents = components => {
  const componentsArray = ensureArray(components)
  return componentsArray.map(component => {
    // Ensure manifest is always loaded from content server
    // And not asset server when an assetPrefix is used
    if (__ASSET_PREFIX__ && component.props.rel === `manifest`) {
      return React.cloneElement(component, {
        href: replace(component.props.href, __ASSET_PREFIX__, ``),
      })
    }
    return component
  })
}

const ensureArray = components => {
  if (Array.isArray(components)) {
    // remove falsy items and flatten
    return flattenDeep(
      components.filter(val => (Array.isArray(val) ? val.length > 0 : val))
    )
  } else {
    // we also accept single components, so we need to handle this case as well
    return components ? [components] : []
  }
}

export default (pagePath, callback) => {
  let bodyHtml = ``
  let headComponents = [
    <meta
      name="generator"
      content={`Gatsby ${gatsbyVersion}`}
      key={`generator-${gatsbyVersion}`}
    />,
  ]
  let htmlAttributes = {}
  let bodyAttributes = {}
  let preBodyComponents = []
  let postBodyComponents = []
  let bodyProps = {}

  const replaceBodyHTMLString = body => {
    bodyHtml = body
  }

  const setHeadComponents = components => {
    headComponents = headComponents.concat(sanitizeComponents(components))
  }

  const setHtmlAttributes = attributes => {
    htmlAttributes = merge(htmlAttributes, attributes)
  }

  const setBodyAttributes = attributes => {
    bodyAttributes = merge(bodyAttributes, attributes)
  }

  const setPreBodyComponents = components => {
    preBodyComponents = preBodyComponents.concat(sanitizeComponents(components))
  }

  const setPostBodyComponents = components => {
    postBodyComponents = postBodyComponents.concat(
      sanitizeComponents(components)
    )
  }

  const setBodyProps = props => {
    bodyProps = merge({}, bodyProps, props)
  }

  const getHeadComponents = () => headComponents

  const replaceHeadComponents = components => {
    headComponents = sanitizeComponents(components)
  }

  const getPreBodyComponents = () => preBodyComponents

  const replacePreBodyComponents = components => {
    preBodyComponents = sanitizeComponents(components)
  }

  const getPostBodyComponents = () => postBodyComponents

  const replacePostBodyComponents = components => {
    postBodyComponents = sanitizeComponents(components)
  }

  const pageData = getPageData(pagePath)
  const pageDataUrl = getPageDataUrl(pagePath)

  const appDataUrl = getAppDataUrl()

  const { componentChunkName, staticQueryHashes = [] } = pageData

  const staticQueryUrls = staticQueryHashes.map(getStaticQueryUrl)

  class RouteHandler extends React.Component {
    render() {
      const props = {
        ...this.props,
        ...pageData.result,
        params: {
          ...grabMatchParams(this.props.location.pathname),
          ...(pageData.result?.pageContext?.__params || {}),
        },
        // pathContext was deprecated in v2. Renamed to pageContext
        pathContext: pageData.result ? pageData.result.pageContext : undefined,
      }

      const pageElement = createElement(
        syncRequires.components[componentChunkName],
        props
      )

      const wrappedPage = apiRunner(
        `wrapPageElement`,
        { element: pageElement, props },
        pageElement,
        ({ result }) => {
          return { element: result, props }
        }
      ).pop()

      return wrappedPage
    }
  }

  const routerElement = (
    <ServerLocation url={`${__BASE_PATH__}${pagePath}`}>
      <Router id="gatsby-focus-wrapper" baseuri={__BASE_PATH__}>
        <RouteHandler path="/*" />
      </Router>
      <div {...RouteAnnouncerProps} />
    </ServerLocation>
  )

  const bodyComponent = apiRunner(
    `wrapRootElement`,
    { element: routerElement, pathname: pagePath },
    routerElement,
    ({ result }) => {
      return { element: result, pathname: pagePath }
    }
  ).pop()

  // Let the site or plugin render the page component.
  apiRunner(`replaceRenderer`, {
    bodyComponent,
    replaceBodyHTMLString,
    setHeadComponents,
    setHtmlAttributes,
    setBodyAttributes,
    setPreBodyComponents,
    setPostBodyComponents,
    setBodyProps,
    pathname: pagePath,
    pathPrefix: __PATH_PREFIX__,
  })

  // If no one stepped up, we'll handle it.
  if (!bodyHtml) {
    try {
      bodyHtml = renderToString(bodyComponent)
    } catch (e) {
      // ignore @reach/router redirect errors
      if (!isRedirect(e)) throw e
    }
  }

  // Create paths to scripts
  let scriptsAndStyles = flatten(
    [`app`, componentChunkName].map(s => {
      const fetchKey = `assetsByChunkName[${s}]`

      let chunks = get(stats, fetchKey)
      const namedChunkGroups = get(stats, `namedChunkGroups`)

      if (!chunks) {
        return null
      }

      chunks = chunks.map(chunk => {
        if (chunk === `/`) {
          return null
        }
        return { rel: `preload`, name: chunk }
      })

      namedChunkGroups[s].assets.forEach(asset =>
        chunks.push({ rel: `preload`, name: asset })
      )

      const childAssets = namedChunkGroups[s].childAssets
      for (const rel in childAssets) {
        chunks = concat(
          chunks,
          childAssets[rel].map(chunk => {
            return { rel, name: chunk }
          })
        )
      }

      return chunks
    })
  )
    .filter(s => isObject(s))
    .sort((s1, s2) => (s1.rel == `preload` ? -1 : 1)) // given priority to preload

  scriptsAndStyles = uniqBy(scriptsAndStyles, item => item.name)

  const scripts = scriptsAndStyles.filter(
    script => script.name && script.name.endsWith(`.js`)
  )
  const styles = scriptsAndStyles.filter(
    style => style.name && style.name.endsWith(`.css`)
  )

  apiRunner(`onRenderBody`, {
    setHeadComponents,
    setHtmlAttributes,
    setBodyAttributes,
    setPreBodyComponents,
    setPostBodyComponents,
    setBodyProps,
    pathname: pagePath,
    loadPageDataSync,
    bodyHtml,
    scripts,
    styles,
    pathPrefix: __PATH_PREFIX__,
  })

  scripts
    .slice(0)
    .reverse()
    .forEach(script => {
      // Add preload/prefetch <link>s for scripts.
      headComponents.push(
        <link
          as="script"
          rel={script.rel}
          key={script.name}
          href={`${__PATH_PREFIX__}/${script.name}`}
        />
      )
    })

  if (pageData) {
    headComponents.push(
      <link
        as="fetch"
        rel="preload"
        key={pageDataUrl}
        href={pageDataUrl}
        crossOrigin="anonymous"
      />
    )
  }
  staticQueryUrls.forEach(staticQueryUrl =>
    headComponents.push(
      <link
        as="fetch"
        rel="preload"
        key={staticQueryUrl}
        href={staticQueryUrl}
        crossOrigin="anonymous"
      />
    )
  )

  if (appDataUrl) {
    headComponents.push(
      <link
        as="fetch"
        rel="preload"
        key={appDataUrl}
        href={appDataUrl}
        crossOrigin="anonymous"
      />
    )
  }

  styles
    .slice(0)
    .reverse()
    .forEach(style => {
      // Add <link>s for styles that should be prefetched
      // otherwise, inline as a <style> tag

      if (style.rel === `prefetch`) {
        headComponents.push(
          <link
            as="style"
            rel={style.rel}
            key={style.name}
            href={`${__PATH_PREFIX__}/${style.name}`}
          />
        )
      } else {
        headComponents.unshift(
          <style
            data-href={`${__PATH_PREFIX__}/${style.name}`}
            dangerouslySetInnerHTML={{
              __html: fs.readFileSync(
                join(process.cwd(), `public`, style.name),
                `utf-8`
              ),
            }}
          />
        )
      }
    })

  // Add page metadata for the current page
  const windowPageData = `/*<![CDATA[*/window.pagePath="${pagePath}";/*]]>*/`

  postBodyComponents.push(
    <script
      key={`script-loader`}
      id={`gatsby-script-loader`}
      dangerouslySetInnerHTML={{
        __html: windowPageData,
      }}
    />
  )

  // Add chunk mapping metadata
  const scriptChunkMapping = `/*<![CDATA[*/window.___chunkMapping=${JSON.stringify(
    chunkMapping
  )};/*]]>*/`

  postBodyComponents.push(
    <script
      key={`chunk-mapping`}
      id={`gatsby-chunk-mapping`}
      dangerouslySetInnerHTML={{
        __html: scriptChunkMapping,
      }}
    />
  )

  let bodyScripts = []
  if (chunkMapping[`polyfill`]) {
    chunkMapping[`polyfill`].forEach(script => {
      const scriptPath = `${__PATH_PREFIX__}${script}`
      bodyScripts.push(
        <script key={scriptPath} src={scriptPath} noModule={true} />
      )
    })
  }

  // Filter out prefetched bundles as adding them as a script tag
  // would force high priority fetching.
  bodyScripts = bodyScripts.concat(
    scripts
      .filter(s => s.rel !== `prefetch`)
      .map(s => {
        const scriptPath = `${__PATH_PREFIX__}/${JSON.stringify(s.name).slice(
          1,
          -1
        )}`
        return <script key={scriptPath} src={scriptPath} async />
      })
  )

  postBodyComponents.push(...bodyScripts)

  apiRunner(`onPreRenderHTML`, {
    getHeadComponents,
    replaceHeadComponents,
    getPreBodyComponents,
    replacePreBodyComponents,
    getPostBodyComponents,
    replacePostBodyComponents,
    pathname: pagePath,
    pathPrefix: __PATH_PREFIX__,
  })

  const html = `<!DOCTYPE html>${renderToStaticMarkup(
    <Html
      {...bodyProps}
      headComponents={headComponents}
      htmlAttributes={htmlAttributes}
      bodyAttributes={bodyAttributes}
      preBodyComponents={preBodyComponents}
      postBodyComponents={postBodyComponents}
      body={bodyHtml}
      path={pagePath}
    />
  )}`

  callback(null, html)
}
