"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard");

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

exports.__esModule = true;
exports.graphql = graphql;
exports.unstable_collectionGraphql = unstable_collectionGraphql;
exports.prefetchPathname = exports.useStaticQuery = exports.StaticQuery = exports.StaticQueryContext = void 0;

var _react = _interopRequireDefault(require("react"));

var _propTypes = _interopRequireDefault(require("prop-types"));

var _gatsbyLink = _interopRequireWildcard(require("gatsby-link"));

exports.Link = _gatsbyLink.default;
exports.withPrefix = _gatsbyLink.withPrefix;
exports.withAssetPrefix = _gatsbyLink.withAssetPrefix;
exports.navigate = _gatsbyLink.navigate;
exports.push = _gatsbyLink.push;
exports.replace = _gatsbyLink.replace;
exports.navigateTo = _gatsbyLink.navigateTo;
exports.parsePath = _gatsbyLink.parsePath;

var _gatsbyReactRouterScroll = require("gatsby-react-router-scroll");

exports.useScrollRestoration = _gatsbyReactRouterScroll.useScrollRestoration;

var _publicPageRenderer = _interopRequireDefault(require("./public-page-renderer"));

exports.PageRenderer = _publicPageRenderer.default;

var _loader = _interopRequireDefault(require("./loader"));

const prefetchPathname = _loader.default.enqueue;
exports.prefetchPathname = prefetchPathname;

const StaticQueryContext = /*#__PURE__*/_react.default.createContext({});

exports.StaticQueryContext = StaticQueryContext;

function StaticQueryDataRenderer({
  staticQueryData,
  data,
  query,
  render
}) {
  const finalData = data ? data.data : staticQueryData[query] && staticQueryData[query].data;
  return /*#__PURE__*/_react.default.createElement(_react.default.Fragment, null, finalData && render(finalData), !finalData && /*#__PURE__*/_react.default.createElement("div", null, "Loading (StaticQuery)"));
}

const StaticQuery = props => {
  const {
    data,
    query,
    render,
    children
  } = props;
  return /*#__PURE__*/_react.default.createElement(StaticQueryContext.Consumer, null, staticQueryData => /*#__PURE__*/_react.default.createElement(StaticQueryDataRenderer, {
    data: data,
    query: query,
    render: render || children,
    staticQueryData: staticQueryData
  }));
};

exports.StaticQuery = StaticQuery;

const useStaticQuery = query => {
  var _context$query;

  if (typeof _react.default.useContext !== `function` && process.env.NODE_ENV === `development`) {
    throw new Error(`You're likely using a version of React that doesn't support Hooks\n` + `Please update React and ReactDOM to 16.8.0 or later to use the useStaticQuery hook.`);
  }

  const context = _react.default.useContext(StaticQueryContext); // query is a stringified number like `3303882` when wrapped with graphql, If a user forgets
  // to wrap the query in a grqphql, then casting it to a Number results in `NaN` allowing us to
  // catch the misuse of the API and give proper direction


  if (isNaN(Number(query))) {
    throw new Error(`useStaticQuery was called with a string but expects to be called using \`graphql\`. Try this:

import { useStaticQuery, graphql } from 'gatsby';

useStaticQuery(graphql\`${query}\`);
`);
  }

  if (context === null || context === void 0 ? void 0 : (_context$query = context[query]) === null || _context$query === void 0 ? void 0 : _context$query.data) {
    return context[query].data;
  } else {
    throw new Error(`The result of this StaticQuery could not be fetched.\n\n` + `This is likely a bug in Gatsby and if refreshing the page does not fix it, ` + `please open an issue in https://github.com/gatsbyjs/gatsby/issues`);
  }
};

exports.useStaticQuery = useStaticQuery;
StaticQuery.propTypes = {
  data: _propTypes.default.object,
  query: _propTypes.default.string.isRequired,
  render: _propTypes.default.func,
  children: _propTypes.default.func
};

function graphql() {
  throw new Error(`It appears like Gatsby is misconfigured. Gatsby related \`graphql\` calls ` + `are supposed to only be evaluated at compile time, and then compiled away. ` + `Unfortunately, something went wrong and the query was left in the compiled code.\n\n` + `Unless your site has a complex or custom babel/Gatsby configuration this is likely a bug in Gatsby.`);
}

function unstable_collectionGraphql() {
  // TODO: Strip this out of the component and throw error if it gets called
  return null;
}