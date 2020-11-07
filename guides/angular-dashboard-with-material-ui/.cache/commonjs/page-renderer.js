"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard");

exports.__esModule = true;
exports.default = void 0;

var _react = _interopRequireWildcard(require("react"));

var _propTypes = _interopRequireDefault(require("prop-types"));

var _loader = require("./loader");

var _apiRunnerBrowser = require("./api-runner-browser");

var _findPath = require("./find-path");

// Renders page
class PageRenderer extends _react.default.Component {
  render() {
    const props = { ...this.props,
      params: { ...(0, _findPath.grabMatchParams)(this.props.location.pathname),
        ...this.props.pageResources.json.pageContext.__params
      },
      pathContext: this.props.pageContext
    };
    const [replacementElement] = (0, _apiRunnerBrowser.apiRunner)(`replaceComponentRenderer`, {
      props: this.props,
      loader: _loader.publicLoader
    });
    const pageElement = replacementElement || /*#__PURE__*/(0, _react.createElement)(this.props.pageResources.component, { ...props,
      key: this.props.path || this.props.pageResources.page.path
    });
    const wrappedPage = (0, _apiRunnerBrowser.apiRunner)(`wrapPageElement`, {
      element: pageElement,
      props
    }, pageElement, ({
      result
    }) => {
      return {
        element: result,
        props
      };
    }).pop();
    return wrappedPage;
  }

}

PageRenderer.propTypes = {
  location: _propTypes.default.object.isRequired,
  pageResources: _propTypes.default.object.isRequired,
  data: _propTypes.default.object,
  pageContext: _propTypes.default.object.isRequired
};
var _default = PageRenderer;
exports.default = _default;