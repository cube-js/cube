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
      var _this$props = this.props,
          query = _this$props.query,
          queries = _this$props.queries;

      if (query) {
        this.load(query);
      }

      if (queries) {
        this.loadQueries(queries);
      }
    }
  }, {
    key: "componentDidUpdate",
    value: function componentDidUpdate(prevProps) {
      var _this$props2 = this.props,
          query = _this$props2.query,
          queries = _this$props2.queries;

      if (!ramda.equals(prevProps.query, query)) {
        this.load(query);
      }

      if (!ramda.equals(prevProps.queries, queries)) {
        this.loadQueries(queries);
      }
    }
  }, {
    key: "isQueryPresent",
    value: function isQueryPresent(query) {
      return query.measures && query.measures.length || query.dimensions && query.dimensions.length || query.timeDimensions && query.timeDimensions.length;
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
      var _this$props3 = this.props,
          loadSql = _this$props3.loadSql,
          cubejsApi = _this$props3.cubejsApi;

      if (query && this.isQueryPresent(query)) {
        if (loadSql === 'only') {
          cubejsApi.sql(query, {
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
        } else if (loadSql) {
          Promise.all([cubejsApi.sql(query, {
            mutexObj: this.mutexObj,
            mutexKey: 'sql'
          }), cubejsApi.load(query, {
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
          cubejsApi.load(query, {
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
        var cubejsApi, meta;
        return _regeneratorRuntime.wrap(function _callee$(_context) {
          while (1) {
            switch (_context.prev = _context.next) {
              case 0:
                cubejsApi = this.props.cubejsApi;
                _context.next = 3;
                return cubejsApi.meta();

              case 3:
                meta = _context.sent;
                this.setState({
                  meta: meta
                });

              case 5:
              case "end":
                return _context.stop();
            }
          }
        }, _callee, this);
      }));

      return function componentDidMount() {
        return _componentDidMount.apply(this, arguments);
      };
    }()
  }, {
    key: "componentDidUpdate",
    value: function componentDidUpdate(prevProps) {
      var query = this.props.query;

      if (!ramda.equals(prevProps.query, query)) {
        // eslint-disable-next-line react/no-did-update-set-state
        this.setState({
          query: query
        });
      }
    }
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
            var query = _this2.state.query;

            _this2.updateQuery(_defineProperty({}, memberType, (query[memberType] || []).concat(toQuery(member))));
          },
          remove: function remove(member) {
            var query = _this2.state.query;
            var members = (query[memberType] || []).concat([]);
            members.splice(member.index, 1);
            return _this2.updateQuery(_defineProperty({}, memberType, members));
          },
          update: function update(member, updateWith) {
            var query = _this2.state.query;
            var members = (query[memberType] || []).concat([]);
            members.splice(member.index, 1, toQuery(updateWith));
            return _this2.updateQuery(_defineProperty({}, memberType, members));
          }
        };
      };

      var granularities = [{
        name: undefined,
        title: 'w/o grouping'
      }, {
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
      var _this$state = this.state,
          meta = _this$state.meta,
          query = _this$state.query,
          chartType = _this$state.chartType;
      return _objectSpread({
        meta: meta,
        query: query,
        validatedQuery: this.validatedQuery(),
        isQueryPresent: this.isQueryPresent(),
        chartType: chartType,
        measures: (meta && query.measures || []).map(function (m, i) {
          return _objectSpread({
            index: i
          }, meta.resolveMember(m, 'measures'));
        }),
        dimensions: (meta && query.dimensions || []).map(function (m, i) {
          return _objectSpread({
            index: i
          }, meta.resolveMember(m, 'dimensions'));
        }),
        segments: (meta && query.segments || []).map(function (m, i) {
          return _objectSpread({
            index: i
          }, meta.resolveMember(m, 'segments'));
        }),
        timeDimensions: (meta && query.timeDimensions || []).map(function (m, i) {
          return _objectSpread({}, m, {
            dimension: _objectSpread({}, meta.resolveMember(m.dimension, 'dimensions'), {
              granularities: granularities
            }),
            index: i
          });
        }),
        filters: (meta && query.filters || []).map(function (m, i) {
          return _objectSpread({}, m, {
            dimension: meta.resolveMember(m.dimension, ['dimensions', 'measures']),
            operators: meta.filterOperatorsForMember(m.dimension, ['dimensions', 'measures']),
            index: i
          });
        }),
        availableMeasures: meta && meta.membersForQuery(query, 'measures') || [],
        availableDimensions: meta && meta.membersForQuery(query, 'dimensions') || [],
        availableTimeDimensions: (meta && meta.membersForQuery(query, 'dimensions') || []).filter(function (m) {
          return m.type === 'time';
        }),
        availableSegments: meta && meta.membersForQuery(query, 'segments') || [],
        updateMeasures: updateMethods('measures'),
        updateDimensions: updateMethods('dimensions'),
        updateSegments: updateMethods('segments'),
        updateTimeDimensions: updateMethods('timeDimensions', toTimeDimension),
        updateFilters: updateMethods('filters', toFilter),
        updateChartType: function updateChartType(newChartType) {
          return _this2.updateVizState({
            chartType: newChartType
          });
        }
      }, queryRendererProps);
    }
  }, {
    key: "updateQuery",
    value: function updateQuery(queryUpdate) {
      var query = this.state.query;
      this.updateVizState({
        query: _objectSpread({}, query, queryUpdate)
      });
    }
  }, {
    key: "updateVizState",
    value: function updateVizState(state) {
      var setQuery = this.props.setQuery;
      var finalState = this.applyStateChangeHeuristics(state);
      this.setState(finalState);
      finalState = _objectSpread({}, this.state, finalState);

      if (setQuery) {
        setQuery(finalState.query);
      }
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
    key: "defaultHeuristics",
    value: function defaultHeuristics(newState) {
      var _this$state2 = this.state,
          query = _this$state2.query,
          sessionGranularity = _this$state2.sessionGranularity;
      var defaultGranularity = sessionGranularity || 'day';

      if (newState.query) {
        var oldQuery = query;
        var newQuery = newState.query;
        var meta = this.state.meta;

        if ((oldQuery.timeDimensions || []).length === 1 && (newQuery.timeDimensions || []).length === 1 && newQuery.timeDimensions[0].granularity && oldQuery.timeDimensions[0].granularity !== newQuery.timeDimensions[0].granularity) {
          newState = _objectSpread({}, newState, {
            sessionGranularity: newQuery.timeDimensions[0].granularity
          });
        }

        if ((oldQuery.measures || []).length === 0 && (newQuery.measures || []).length > 0 || (oldQuery.measures || []).length === 1 && (newQuery.measures || []).length === 1 && oldQuery.measures[0] !== newQuery.measures[0]) {
          var defaultTimeDimension = meta.defaultTimeDimensionNameFor(newQuery.measures[0]);
          newQuery = _objectSpread({}, newQuery, {
            timeDimensions: defaultTimeDimension ? [{
              dimension: defaultTimeDimension,
              granularity: defaultGranularity
            }] : []
          });
          return _objectSpread({}, newState, {
            query: newQuery,
            chartType: defaultTimeDimension ? 'line' : 'number'
          });
        }

        if ((oldQuery.dimensions || []).length === 0 && (newQuery.dimensions || []).length > 0) {
          newQuery = _objectSpread({}, newQuery, {
            timeDimensions: (newQuery.timeDimensions || []).map(function (td) {
              return _objectSpread({}, td, {
                granularity: undefined
              });
            })
          });
          return _objectSpread({}, newState, {
            query: newQuery,
            chartType: 'table'
          });
        }

        if ((oldQuery.dimensions || []).length > 0 && (newQuery.dimensions || []).length === 0) {
          newQuery = _objectSpread({}, newQuery, {
            timeDimensions: (newQuery.timeDimensions || []).map(function (td) {
              return _objectSpread({}, td, {
                granularity: td.granularity || defaultGranularity
              });
            })
          });
          return _objectSpread({}, newState, {
            query: newQuery,
            chartType: (newQuery.timeDimensions || []).length ? 'line' : 'number'
          });
        }

        if (((oldQuery.dimensions || []).length > 0 || (oldQuery.measures || []).length > 0) && (newQuery.dimensions || []).length === 0 && (newQuery.measures || []).length === 0) {
          newQuery = _objectSpread({}, newQuery, {
            timeDimensions: [],
            filters: []
          });
          return _objectSpread({}, newState, {
            query: newQuery,
            sessionGranularity: null
          });
        }

        return newState;
      }

      if (newState.chartType) {
        var newChartType = newState.chartType;

        if ((newChartType === 'line' || newChartType === 'area') && (query.timeDimensions || []).length === 1 && !query.timeDimensions[0].granularity) {
          var _query$timeDimensions = _slicedToArray(query.timeDimensions, 1),
              td = _query$timeDimensions[0];

          return _objectSpread({}, newState, {
            query: _objectSpread({}, query, {
              timeDimensions: [_objectSpread({}, td, {
                granularity: defaultGranularity
              })]
            })
          });
        }

        if ((newChartType === 'pie' || newChartType === 'table' || newChartType === 'number') && (query.timeDimensions || []).length === 1 && query.timeDimensions[0].granularity) {
          var _query$timeDimensions2 = _slicedToArray(query.timeDimensions, 1),
              _td = _query$timeDimensions2[0];

          return _objectSpread({}, newState, {
            query: _objectSpread({}, query, {
              timeDimensions: [_objectSpread({}, _td, {
                granularity: undefined
              })]
            })
          });
        }
      }

      return newState;
    }
  }, {
    key: "applyStateChangeHeuristics",
    value: function applyStateChangeHeuristics(newState) {
      var _this$props = this.props,
          stateChangeHeuristics = _this$props.stateChangeHeuristics,
          disableHeuristics = _this$props.disableHeuristics;

      if (disableHeuristics) {
        return newState;
      }

      return stateChangeHeuristics && stateChangeHeuristics(this.state, newState) || this.defaultHeuristics(newState);
    }
  }, {
    key: "render",
    value: function render() {
      var _this3 = this;

      var _this$props2 = this.props,
          cubejsApi = _this$props2.cubejsApi,
          _render = _this$props2.render;
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
QueryBuilder.propTypes = {
  render: PropTypes.func,
  stateChangeHeuristics: PropTypes.func,
  setQuery: PropTypes.func,
  cubejsApi: PropTypes.object.isRequired,
  disableHeuristics: PropTypes.bool,
  query: PropTypes.object
};
QueryBuilder.defaultProps = {
  query: {},
  setQuery: null,
  stateChangeHeuristics: null,
  disableHeuristics: false,
  render: null
};

exports.QueryRenderer = QueryRenderer;
exports.QueryRendererWithTotals = QueryRendererWithTotals;
exports.QueryBuilder = QueryBuilder;
