"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _apiRunnerBrowser = require("./api-runner-browser");

var _react = _interopRequireDefault(require("react"));

var _reactDom = _interopRequireDefault(require("react-dom"));

var _router = require("@reach/router");

var _gatsbyReactRouterScroll = require("gatsby-react-router-scroll");

var _domready = _interopRequireDefault(require("@mikaelkristiansson/domready"));

var _gatsby = require("gatsby");

var _navigation = require("./navigation");

var _emitter = _interopRequireDefault(require("./emitter"));

var _pageRenderer = _interopRequireDefault(require("./page-renderer"));

var _asyncRequires = _interopRequireDefault(require("$virtual/async-requires"));

var _loader = require("./loader");

var _ensureResources = _interopRequireDefault(require("./ensure-resources"));

var _stripPrefix = _interopRequireDefault(require("./strip-prefix"));

var _matchPaths = _interopRequireDefault(require("$virtual/match-paths.json"));

// Generated during bootstrap
const loader = new _loader.ProdLoader(_asyncRequires.default, _matchPaths.default);
(0, _loader.setLoader)(loader);
loader.setApiRunner(_apiRunnerBrowser.apiRunner);
window.asyncRequires = _asyncRequires.default;
window.___emitter = _emitter.default;
window.___loader = _loader.publicLoader;
(0, _navigation.init)();
(0, _apiRunnerBrowser.apiRunnerAsync)(`onClientEntry`).then(() => {
  // Let plugins register a service worker. The plugin just needs
  // to return true.
  if ((0, _apiRunnerBrowser.apiRunner)(`registerServiceWorker`).length > 0) {
    require(`./register-service-worker`);
  } // In gatsby v2 if Router is used in page using matchPaths
  // paths need to contain full path.
  // For example:
  //   - page have `/app/*` matchPath
  //   - inside template user needs to use `/app/xyz` as path
  // Resetting `basepath`/`baseuri` keeps current behaviour
  // to not introduce breaking change.
  // Remove this in v3


  const RouteHandler = props => /*#__PURE__*/_react.default.createElement(_router.BaseContext.Provider, {
    value: {
      baseuri: `/`,
      basepath: `/`
    }
  }, /*#__PURE__*/_react.default.createElement(_pageRenderer.default, props));

  const DataContext = /*#__PURE__*/_react.default.createContext({});

  class GatsbyRoot extends _react.default.Component {
    render() {
      const {
        children
      } = this.props;
      return /*#__PURE__*/_react.default.createElement(_router.Location, null, ({
        location
      }) => /*#__PURE__*/_react.default.createElement(_ensureResources.default, {
        location: location
      }, ({
        pageResources,
        location
      }) => {
        const staticQueryResults = (0, _loader.getStaticQueryResults)();
        return /*#__PURE__*/_react.default.createElement(_gatsby.StaticQueryContext.Provider, {
          value: staticQueryResults
        }, /*#__PURE__*/_react.default.createElement(DataContext.Provider, {
          value: {
            pageResources,
            location
          }
        }, children));
      }));
    }

  }

  class LocationHandler extends _react.default.Component {
    render() {
      return /*#__PURE__*/_react.default.createElement(DataContext.Consumer, null, ({
        pageResources,
        location
      }) => /*#__PURE__*/_react.default.createElement(_navigation.RouteUpdates, {
        location: location
      }, /*#__PURE__*/_react.default.createElement(_gatsbyReactRouterScroll.ScrollContext, {
        location: location,
        shouldUpdateScroll: _navigation.shouldUpdateScroll
      }, /*#__PURE__*/_react.default.createElement(_router.Router, {
        basepath: __BASE_PATH__,
        location: location,
        id: "gatsby-focus-wrapper"
      }, /*#__PURE__*/_react.default.createElement(RouteHandler, (0, _extends2.default)({
        path: pageResources.page.path === `/404.html` ? (0, _stripPrefix.default)(location.pathname, __BASE_PATH__) : encodeURI(pageResources.page.matchPath || pageResources.page.path)
      }, this.props, {
        location: location,
        pageResources: pageResources
      }, pageResources.json))))));
    }

  }

  const {
    pagePath,
    location: browserLoc
  } = window; // Explicitly call navigate if the canonical path (window.pagePath)
  // is different to the browser path (window.location.pathname). But
  // only if NONE of the following conditions hold:
  //
  // - The url matches a client side route (page.matchPath)
  // - it's a 404 page
  // - it's the offline plugin shell (/offline-plugin-app-shell-fallback/)

  if (pagePath && __BASE_PATH__ + pagePath !== browserLoc.pathname && !(loader.findMatchPath((0, _stripPrefix.default)(browserLoc.pathname, __BASE_PATH__)) || pagePath === `/404.html` || pagePath.match(/^\/404\/?$/) || pagePath.match(/^\/offline-plugin-app-shell-fallback\/?$/))) {
    (0, _router.navigate)(__BASE_PATH__ + pagePath + browserLoc.search + browserLoc.hash, {
      replace: true
    });
  }

  _loader.publicLoader.loadPage(browserLoc.pathname).then(page => {
    if (!page || page.status === _loader.PageResourceStatus.Error) {
      throw new Error(`page resources for ${browserLoc.pathname} not found. Not rendering React`);
    }

    window.___webpackCompilationHash = page.page.webpackCompilationHash;
    const SiteRoot = (0, _apiRunnerBrowser.apiRunner)(`wrapRootElement`, {
      element: /*#__PURE__*/_react.default.createElement(LocationHandler, null)
    }, /*#__PURE__*/_react.default.createElement(LocationHandler, null), ({
      result
    }) => {
      return {
        element: result
      };
    }).pop();

    const App = () => /*#__PURE__*/_react.default.createElement(GatsbyRoot, null, SiteRoot);

    const renderer = (0, _apiRunnerBrowser.apiRunner)(`replaceHydrateFunction`, undefined, _reactDom.default.hydrate)[0];
    (0, _domready.default)(() => {
      renderer( /*#__PURE__*/_react.default.createElement(App, null), typeof window !== `undefined` ? document.getElementById(`___gatsby`) : void 0, () => {
        (0, _apiRunnerBrowser.apiRunner)(`onInitialClientRender`);
      });
    });
  });
});