'use strict';

Object.defineProperty(exports, '__esModule', { value: true });

function _interopDefault (ex) { return (ex && (typeof ex === 'object') && 'default' in ex) ? ex['default'] : ex; }

require('core-js/modules/es6.array.map');
var _slicedToArray = _interopDefault(require('@babel/runtime/helpers/slicedToArray'));
require('core-js/modules/es6.promise');
require('core-js/modules/web.dom.iterable');
require('core-js/modules/es6.array.iterator');
require('core-js/modules/es6.string.iterator');
var _classCallCheck = _interopDefault(require('@babel/runtime/helpers/classCallCheck'));
var _createClass = _interopDefault(require('@babel/runtime/helpers/createClass'));
var _possibleConstructorReturn = _interopDefault(require('@babel/runtime/helpers/possibleConstructorReturn'));
var _getPrototypeOf = _interopDefault(require('@babel/runtime/helpers/getPrototypeOf'));
var _inherits = _interopDefault(require('@babel/runtime/helpers/inherits'));
var React = _interopDefault(require('react'));
var PropTypes = require('prop-types');
var ramda = require('ramda');
var _extends = _interopDefault(require('@babel/runtime/helpers/extends'));
var _objectSpread = _interopDefault(require('@babel/runtime/helpers/objectSpread'));
var _objectWithoutProperties = _interopDefault(require('@babel/runtime/helpers/objectWithoutProperties'));

var QueryRenderer =
/*#__PURE__*/
function (_React$Component) {
  _inherits(QueryRenderer, _React$Component);

  function QueryRenderer(props) {
    var _this;

    _classCallCheck(this, QueryRenderer);

    _this = _possibleConstructorReturn(this, _getPrototypeOf(QueryRenderer).call(this, props));
    _this.state = {};
    return _this;
  }

  _createClass(QueryRenderer, [{
    key: "componentDidMount",
    value: function componentDidMount() {
      if (this.props.query) {
        this.load(this.props.query);
      }

      if (this.props.queries) {
        this.loadQueries(this.props.queries);
      }
    }
  }, {
    key: "componentDidUpdate",
    value: function componentDidUpdate(prevProps) {
      var query = this.props.query;

      if (!ramda.equals(prevProps.query, query)) {
        this.load(query);
      }

      var queries = this.props.queries;

      if (!ramda.equals(prevProps.queries, queries)) {
        this.loadQueries(queries);
      }
    }
  }, {
    key: "load",
    value: function load(query) {
      var _this2 = this;

      this.setState({
        isLoading: true,
        resultSet: null,
        error: null,
        sqlQuery: null
      });

      if (this.props.loadSql === 'only') {
        this.props.cubejsApi.sql(query).then(function (sqlQuery) {
          return _this2.setState({
            sqlQuery: sqlQuery,
            error: null,
            isLoading: false
          });
        }).catch(function (error) {
          return _this2.setState({
            resultSet: null,
            error: error,
            isLoading: false
          });
        });
      } else if (this.props.loadSql) {
        Promise.all([this.props.cubejsApi.sql(query), this.props.cubejsApi.load(query)]).then(function (_ref) {
          var _ref2 = _slicedToArray(_ref, 2),
              sqlQuery = _ref2[0],
              resultSet = _ref2[1];

          return _this2.setState({
            sqlQuery: sqlQuery,
            resultSet: resultSet,
            error: null,
            isLoading: false
          });
        }).catch(function (error) {
          return _this2.setState({
            resultSet: null,
            error: error,
            isLoading: false
          });
        });
      } else {
        this.props.cubejsApi.load(query).then(function (resultSet) {
          return _this2.setState({
            resultSet: resultSet,
            error: null,
            isLoading: false
          });
        }).catch(function (error) {
          return _this2.setState({
            resultSet: null,
            error: error,
            isLoading: false
          });
        });
      }
    }
  }, {
    key: "loadQueries",
    value: function loadQueries(queries) {
      var _this3 = this;

      this.setState({
        isLoading: true,
        resultSet: null,
        error: null
      });
      var resultPromises = Promise.all(ramda.toPairs(queries).map(function (_ref3) {
        var _ref4 = _slicedToArray(_ref3, 2),
            name = _ref4[0],
            query = _ref4[1];

        return _this3.props.cubejsApi.load(query).then(function (r) {
          return [name, r];
        });
      }));
      resultPromises.then(function (resultSet) {
        return _this3.setState({
          resultSet: ramda.fromPairs(resultSet),
          error: null,
          isLoading: false
        });
      }).catch(function (error) {
        return _this3.setState({
          resultSet: null,
          error: error,
          isLoading: false
        });
      });
    }
  }, {
    key: "render",
    value: function render() {
      var loadState = {
        error: this.state.error,
        resultSet: this.props.queries ? this.state.resultSet || {} : this.state.resultSet,
        loadingState: {
          isLoading: this.state.isLoading
        },
        sqlQuery: this.state.sqlQuery
      };

      if (this.props.render) {
        return this.props.render(loadState);
      }

      return null;
    }
  }]);

  return QueryRenderer;
}(React.Component);
QueryRenderer.propTypes = {
  render: PropTypes.func.required,
  afterRender: PropTypes.func,
  cubejsApi: PropTypes.object.required,
  query: PropTypes.object,
  queries: PropTypes.object,
  loadSql: PropTypes.any
};

var QueryRendererWithTotals = (function (_ref) {
  var query = _ref.query,
      restProps = _objectWithoutProperties(_ref, ["query"]);

  return React.createElement(QueryRenderer, _extends({
    queries: {
      totals: _objectSpread({}, query, {
        dimensions: [],
        timeDimensions: query.timeDimensions ? query.timeDimensions.map(function (td) {
          return _objectSpread({}, td, {
            granularity: null
          });
        }) : undefined
      }),
      main: query
    }
  }, restProps));
});

exports.QueryRenderer = QueryRenderer;
exports.QueryRendererWithTotals = QueryRendererWithTotals;
