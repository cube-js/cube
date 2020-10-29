"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard");

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

exports.__esModule = true;
exports.default = void 0;

var _react = _interopRequireDefault(require("react"));

var _loader = _interopRequireWildcard(require("./loader"));

var _shallowCompare = _interopRequireDefault(require("shallow-compare"));

class EnsureResources extends _react.default.Component {
  constructor(props) {
    super();
    const {
      location,
      pageResources
    } = props;
    this.state = {
      location: { ...location
      },
      pageResources: pageResources || _loader.default.loadPageSync(location.pathname)
    };
  }

  static getDerivedStateFromProps({
    location
  }, prevState) {
    if (prevState.location.href !== location.href) {
      const pageResources = _loader.default.loadPageSync(location.pathname);

      return {
        pageResources,
        location: { ...location
        }
      };
    }

    return {
      location: { ...location
      }
    };
  }

  loadResources(rawPath) {
    _loader.default.loadPage(rawPath).then(pageResources => {
      if (pageResources && pageResources.status !== _loader.PageResourceStatus.Error) {
        this.setState({
          location: { ...window.location
          },
          pageResources
        });
      } else {
        window.history.replaceState({}, ``, location.href);
        window.location = rawPath;
      }
    });
  }

  shouldComponentUpdate(nextProps, nextState) {
    // Always return false if we're missing resources.
    if (!nextState.pageResources) {
      this.loadResources(nextProps.location.pathname);
      return false;
    } // Check if the component or json have changed.


    if (this.state.pageResources !== nextState.pageResources) {
      return true;
    }

    if (this.state.pageResources.component !== nextState.pageResources.component) {
      return true;
    }

    if (this.state.pageResources.json !== nextState.pageResources.json) {
      return true;
    } // Check if location has changed on a page using internal routing
    // via matchPath configuration.


    if (this.state.location.key !== nextState.location.key && nextState.pageResources.page && (nextState.pageResources.page.matchPath || nextState.pageResources.page.path)) {
      return true;
    }

    return (0, _shallowCompare.default)(this, nextProps, nextState);
  }

  render() {
    if (process.env.NODE_ENV !== `production` && !this.state.pageResources) {
      throw new Error(`EnsureResources was not able to find resources for path: "${this.props.location.pathname}"
This typically means that an issue occurred building components for that path.
Run \`gatsby clean\` to remove any cached elements.`);
    }

    return this.props.children(this.state);
  }

}

var _default = EnsureResources;
exports.default = _default;