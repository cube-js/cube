'use strict';

Object.defineProperty(exports, '__esModule', { value: true });

function _interopDefault (ex) { return (ex && (typeof ex === 'object') && 'default' in ex) ? ex['default'] : ex; }

require('core-js/modules/es6.array.map');
var _slicedToArray = _interopDefault(require('@babel/runtime/helpers/slicedToArray'));
require('core-js/modules/es6.promise');
require('core-js/modules/es6.string.iterator');
require('core-js/modules/web.dom.iterable');
require('core-js/modules/es6.array.iterator');
require('core-js/modules/es6.object.keys');
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
require('core-js/modules/es6.array.filter');
var _defineProperty = _interopDefault(require('@babel/runtime/helpers/defineProperty'));
require('core-js/modules/es6.function.name');
var _regeneratorRuntime = _interopDefault(require('@babel/runtime/regenerator'));
require('regenerator-runtime/runtime');
var _asyncToGenerator = _interopDefault(require('@babel/runtime/helpers/asyncToGenerator'));

var QueryRenderer =
/*#__PURE__*/
function (_React$Component) {
  _inherits(QueryRenderer, _React$Component);

  function QueryRenderer(props) {
    var _this;

    _classCallCheck(this, QueryRenderer);

    _this = _possibleConstructorReturn(this, _getPrototypeOf(QueryRenderer).call(this, props));
    _this.state = {};
    _this.mutexObj = {};
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

      if (query && Object.keys(query).length) {
        if (this.props.loadSql === 'only') {
          this.props.cubejsApi.sql(query, {
            mutexObj: this.mutexObj,
            mutexKey: 'sql'
          }).then(function (sqlQuery) {
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
          Promise.all([this.props.cubejsApi.sql(query, {
            mutexObj: this.mutexObj,
            mutexKey: 'sql'
          }), this.props.cubejsApi.load(query, {
            mutexObj: this.mutexObj,
            mutexKey: 'query'
          })]).then(function (_ref) {
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
          this.props.cubejsApi.load(query, {
            mutexObj: this.mutexObj,
            mutexKey: 'query'
          }).then(function (resultSet) {
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

        return _this3.props.cubejsApi.load(query, {
          mutexObj: _this3.mutexObj,
          mutexKey: name
        }).then(function (r) {
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
  render: PropTypes.func,
  afterRender: PropTypes.func,
  cubejsApi: PropTypes.object.isRequired,
  query: PropTypes.object,
  queries: PropTypes.object,
  loadSql: PropTypes.any
};
QueryRenderer.defaultProps = {
  query: {}
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

var QueryBuilder =
/*#__PURE__*/
function (_React$Component) {
  _inherits(QueryBuilder, _React$Component);

  function QueryBuilder(props) {
    var _this;

    _classCallCheck(this, QueryBuilder);

    _this = _possibleConstructorReturn(this, _getPrototypeOf(QueryBuilder).call(this, props));
    _this.state = {
      query: props.query,
      chartType: 'line'
    };
    return _this;
  }

  _createClass(QueryBuilder, [{
    key: "componentDidMount",
    value: function () {
      var _componentDidMount = _asyncToGenerator(
      /*#__PURE__*/
      _regeneratorRuntime.mark(function _callee() {
        var meta;
        return _regeneratorRuntime.wrap(function _callee$(_context) {
          while (1) {
            switch (_context.prev = _context.next) {
              case 0:
                _context.next = 2;
                return this.props.cubejsApi.meta();

              case 2:
                meta = _context.sent;
                this.setState({
                  meta: meta
                });

              case 4:
              case "end":
                return _context.stop();
            }
          }
        }, _callee, this);
      }));

      function componentDidMount() {
        return _componentDidMount.apply(this, arguments);
      }

      return componentDidMount;
    }()
  }, {
    key: "isQueryPresent",
    value: function isQueryPresent() {
      var query = this.state.query;
      return query.measures && query.measures.length || query.dimensions && query.dimensions.length || query.timeDimensions && query.timeDimensions.length;
    }
  }, {
    key: "prepareRenderProps",
    value: function prepareRenderProps(queryRendererProps) {
      var _this2 = this;

      var getName = function getName(member) {
        return member.name;
      };

      var toTimeDimension = function toTimeDimension(member) {
        return {
          dimension: member.dimension.name,
          granularity: member.granularity,
          dateRange: member.dateRange
        };
      };

      var toFilter = function toFilter(member) {
        return {
          dimension: member.dimension.name,
          operator: member.operator,
          values: member.values
        };
      };

      var updateMethods = function updateMethods(memberType) {
        var toQuery = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : getName;
        return {
          add: function add(member) {
            return _this2.setState({
              query: _objectSpread({}, _this2.state.query, _defineProperty({}, memberType, (_this2.state.query[memberType] || []).concat(toQuery(member))))
            });
          },
          remove: function remove(member) {
            var members = (_this2.state.query[memberType] || []).concat([]);
            members.splice(member.index, 1);
            return _this2.setState({
              query: _objectSpread({}, _this2.state.query, _defineProperty({}, memberType, members))
            });
          },
          update: function update(member, updateWith) {
            var members = (_this2.state.query[memberType] || []).concat([]);
            members.splice(member.index, 1, toQuery(updateWith));
            return _this2.setState({
              query: _objectSpread({}, _this2.state.query, _defineProperty({}, memberType, members))
            });
          }
        };
      };

      var granularities = [{
        name: 'hour',
        title: 'Hour'
      }, {
        name: 'day',
        title: 'Day'
      }, {
        name: 'week',
        title: 'Week'
      }, {
        name: 'month',
        title: 'Month'
      }, {
        name: 'year',
        title: 'Year'
      }];
      return _objectSpread({
        meta: this.state.meta,
        query: this.state.query,
        validatedQuery: this.validatedQuery(),
        isQueryPresent: this.isQueryPresent(),
        chartType: this.state.chartType,
        measures: (this.state.meta && this.state.query.measures || []).map(function (m, i) {
          return _objectSpread({
            index: i
          }, _this2.state.meta.resolveMember(m, 'measures'));
        }),
        dimensions: (this.state.meta && this.state.query.dimensions || []).map(function (m, i) {
          return _objectSpread({
            index: i
          }, _this2.state.meta.resolveMember(m, 'dimensions'));
        }),
        segments: (this.state.meta && this.state.query.segments || []).map(function (m, i) {
          return _objectSpread({
            index: i
          }, _this2.state.meta.resolveMember(m, 'segments'));
        }),
        timeDimensions: (this.state.meta && this.state.query.timeDimensions || []).map(function (m, i) {
          return _objectSpread({}, m, {
            dimension: _objectSpread({}, _this2.state.meta.resolveMember(m.dimension, 'dimensions'), {
              granularities: granularities
            }),
            index: i
          });
        }),
        filters: (this.state.meta && this.state.query.filters || []).map(function (m, i) {
          return _objectSpread({}, m, {
            dimension: _this2.state.meta.resolveMember(m.dimension, ['dimensions', 'measures']),
            operators: _this2.state.meta.filterOperatorsForMember(m.dimension, ['dimensions', 'measures']),
            index: i
          });
        }),
        availableMeasures: this.state.meta && this.state.meta.membersForQuery(this.state.query, 'measures') || [],
        availableDimensions: this.state.meta && this.state.meta.membersForQuery(this.state.query, 'dimensions') || [],
        availableTimeDimensions: (this.state.meta && this.state.meta.membersForQuery(this.state.query, 'dimensions') || []).filter(function (m) {
          return m.type === 'time';
        }),
        availableSegments: this.state.meta && this.state.meta.membersForQuery(this.state.query, 'segments') || [],
        updateMeasures: updateMethods('measures'),
        updateDimensions: updateMethods('dimensions'),
        updateSegments: updateMethods('segments'),
        updateTimeDimensions: updateMethods('timeDimensions', toTimeDimension),
        updateFilters: updateMethods('filters', toFilter),
        updateChartType: function updateChartType(chartType) {
          return _this2.setState({
            chartType: chartType
          });
        }
      }, queryRendererProps);
    }
  }, {
    key: "validatedQuery",
    value: function validatedQuery() {
      var query = this.state.query;
      return _objectSpread({}, query, {
        filters: (query.filters || []).filter(function (f) {
          return f.operator;
        })
      });
    }
  }, {
    key: "render",
    value: function render() {
      var _this3 = this;

      var _this$props = this.props,
          cubejsApi = _this$props.cubejsApi,
          _render = _this$props.render;
      return React.createElement(QueryRenderer, {
        query: this.validatedQuery(),
        cubejsApi: cubejsApi,
        render: function render(queryRendererProps) {
          if (_render) {
            return _render(_this3.prepareRenderProps(queryRendererProps));
          }

          return null;
        }
      });
    }
  }]);

  return QueryBuilder;
}(React.Component);

exports.QueryRenderer = QueryRenderer;
exports.QueryRendererWithTotals = QueryRendererWithTotals;
exports.QueryBuilder = QueryBuilder;
