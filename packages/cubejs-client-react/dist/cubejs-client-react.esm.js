import 'core-js/modules/es.array.iterator';
import 'core-js/modules/es.array.map';
import 'core-js/modules/es.object.to-string';
import 'core-js/modules/es.promise';
import 'core-js/modules/es.string.iterator';
import 'core-js/modules/web.dom-collections.iterator';
import _slicedToArray from '@babel/runtime/helpers/slicedToArray';
import _objectSpread2 from '@babel/runtime/helpers/objectSpread';
import _classCallCheck from '@babel/runtime/helpers/classCallCheck';
import _possibleConstructorReturn from '@babel/runtime/helpers/possibleConstructorReturn';
import _getPrototypeOf from '@babel/runtime/helpers/getPrototypeOf';
import _createClass from '@babel/runtime/helpers/createClass';
import _inherits from '@babel/runtime/helpers/inherits';
import React, { createContext, useRef, useState, useContext, useEffect } from 'react';
import { func, object, any, bool } from 'prop-types';
import { equals, toPairs, fromPairs, uniqBy, prop, indexBy } from 'ramda';
import _extends from '@babel/runtime/helpers/extends';
import _objectWithoutProperties from '@babel/runtime/helpers/objectWithoutProperties';
import 'core-js/modules/es.array.concat';
import 'core-js/modules/es.array.filter';
import 'core-js/modules/es.array.for-each';
import 'core-js/modules/es.array.includes';
import 'core-js/modules/es.array.splice';
import 'core-js/modules/es.function.name';
import 'core-js/modules/es.object.entries';
import 'core-js/modules/es.string.includes';
import 'core-js/modules/web.dom-collections.for-each';
import _defineProperty from '@babel/runtime/helpers/defineProperty';
import _regeneratorRuntime from '@babel/runtime/regenerator';
import 'regenerator-runtime/runtime';
import _asyncToGenerator from '@babel/runtime/helpers/asyncToGenerator';
import _toConsumableArray from '@babel/runtime/helpers/toConsumableArray';
import { ResultSet } from '@cubejs-client/core';
import 'core-js/modules/es.object.keys';

var isQueryPresent = (function (query) {
  return query.measures && query.measures.length || query.dimensions && query.dimensions.length || query.timeDimensions && query.timeDimensions.length;
});

var CubeContext = createContext(null);

var QueryRenderer =
/*#__PURE__*/
function (_React$Component) {
  _inherits(QueryRenderer, _React$Component);

  _createClass(QueryRenderer, null, [{
    key: "isQueryPresent",
    value: function isQueryPresent$$1(query) {
      return isQueryPresent(query);
    }
  }]);

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
    key: "shouldComponentUpdate",
    value: function shouldComponentUpdate(nextProps, nextState) {
      var _this$props2 = this.props,
          query = _this$props2.query,
          queries = _this$props2.queries,
          render = _this$props2.render,
          cubejsApi = _this$props2.cubejsApi,
          loadSql = _this$props2.loadSql,
          updateOnlyOnStateChange = _this$props2.updateOnlyOnStateChange;

      if (!updateOnlyOnStateChange) {
        return true;
      }

      return !equals(nextProps.query, query) || !equals(nextProps.queries, queries) || (nextProps.render == null || render == null) && nextProps.render !== render || nextProps.cubejsApi !== cubejsApi || nextProps.loadSql !== loadSql || !equals(nextState, this.state) || nextProps.updateOnlyOnStateChange !== updateOnlyOnStateChange;
    }
  }, {
    key: "componentDidUpdate",
    value: function componentDidUpdate(prevProps) {
      var _this$props3 = this.props,
          query = _this$props3.query,
          queries = _this$props3.queries;

      if (!equals(prevProps.query, query)) {
        this.load(query);
      }

      if (!equals(prevProps.queries, queries)) {
        this.loadQueries(queries);
      }
    }
  }, {
    key: "cubejsApi",
    value: function cubejsApi() {
      // eslint-disable-next-line react/destructuring-assignment
      return this.props.cubejsApi || this.context && this.context.cubejsApi;
    }
  }, {
    key: "load",
    value: function load(query) {
      var _this2 = this;

      var resetResultSetOnChange = this.props.resetResultSetOnChange;
      this.setState(_objectSpread2({
        isLoading: true,
        error: null,
        sqlQuery: null
      }, resetResultSetOnChange ? {
        resultSet: null
      } : {}));
      var loadSql = this.props.loadSql;
      var cubejsApi = this.cubejsApi();

      if (query && QueryRenderer.isQueryPresent(query)) {
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
          })["catch"](function (error) {
            return _this2.setState(_objectSpread2({}, resetResultSetOnChange ? {
              resultSet: null
            } : {}, {
              error: error,
              isLoading: false
            }));
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
          })["catch"](function (error) {
            return _this2.setState(_objectSpread2({}, resetResultSetOnChange ? {
              resultSet: null
            } : {}, {
              error: error,
              isLoading: false
            }));
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
          })["catch"](function (error) {
            return _this2.setState(_objectSpread2({}, resetResultSetOnChange ? {
              resultSet: null
            } : {}, {
              error: error,
              isLoading: false
            }));
          });
        }
      }
    }
  }, {
    key: "loadQueries",
    value: function loadQueries(queries) {
      var _this3 = this;

      var cubejsApi = this.cubejsApi();
      var resetResultSetOnChange = this.props.resetResultSetOnChange;
      this.setState(_objectSpread2({
        isLoading: true
      }, resetResultSetOnChange ? {
        resultSet: null
      } : {}, {
        error: null
      }));
      var resultPromises = Promise.all(toPairs(queries).map(function (_ref3) {
        var _ref4 = _slicedToArray(_ref3, 2),
            name = _ref4[0],
            query = _ref4[1];

        return cubejsApi.load(query, {
          mutexObj: _this3.mutexObj,
          mutexKey: name
        }).then(function (r) {
          return [name, r];
        });
      }));
      resultPromises.then(function (resultSet) {
        return _this3.setState({
          resultSet: fromPairs(resultSet),
          error: null,
          isLoading: false
        });
      })["catch"](function (error) {
        return _this3.setState(_objectSpread2({}, resetResultSetOnChange ? {
          resultSet: null
        } : {}, {
          error: error,
          isLoading: false
        }));
      });
    }
  }, {
    key: "render",
    value: function render() {
      var _this$state = this.state,
          error = _this$state.error,
          queries = _this$state.queries,
          resultSet = _this$state.resultSet,
          isLoading = _this$state.isLoading,
          sqlQuery = _this$state.sqlQuery;
      var render = this.props.render;
      var loadState = {
        error: error,
        resultSet: queries ? resultSet || {} : resultSet,
        loadingState: {
          isLoading: isLoading
        },
        sqlQuery: sqlQuery
      };

      if (render) {
        return render(loadState);
      }

      return null;
    }
  }]);

  return QueryRenderer;
}(React.Component);
QueryRenderer.contextType = CubeContext;
QueryRenderer.propTypes = {
  render: func,
  cubejsApi: object,
  query: object,
  queries: object,
  loadSql: any,
  resetResultSetOnChange: bool,
  updateOnlyOnStateChange: bool
};
QueryRenderer.defaultProps = {
  cubejsApi: null,
  query: null,
  render: null,
  queries: null,
  loadSql: null,
  updateOnlyOnStateChange: false,
  resetResultSetOnChange: true
};

var QueryRendererWithTotals = function QueryRendererWithTotals(_ref) {
  var query = _ref.query,
      restProps = _objectWithoutProperties(_ref, ["query"]);

  return React.createElement(QueryRenderer, _extends({
    queries: {
      totals: _objectSpread2({}, query, {
        dimensions: [],
        timeDimensions: query.timeDimensions ? query.timeDimensions.map(function (td) {
          return _objectSpread2({}, td, {
            granularity: null
          });
        }) : undefined
      }),
      main: query
    }
  }, restProps));
};

QueryRendererWithTotals.propTypes = {
  render: func,
  cubejsApi: object.isRequired,
  query: object,
  queries: object,
  loadSql: any
};
QueryRendererWithTotals.defaultProps = {
  query: null,
  render: null,
  queries: null,
  loadSql: null
};

function reorder(list, sourceIndex, destinationIndex) {
  var result = _toConsumableArray(list);

  var _result$splice = result.splice(sourceIndex, 1),
      _result$splice2 = _slicedToArray(_result$splice, 1),
      removed = _result$splice2[0];

  result.splice(destinationIndex, 0, removed);
  return result;
}

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

var QueryBuilder =
/*#__PURE__*/
function (_React$Component) {
  _inherits(QueryBuilder, _React$Component);

  _createClass(QueryBuilder, null, [{
    key: "getDerivedStateFromProps",
    value: function getDerivedStateFromProps(props, state) {
      var nextState = _objectSpread2({}, state, {}, props.vizState || {});

      return _objectSpread2({}, nextState, {
        query: _objectSpread2({}, nextState.query, {}, props.query || {})
      });
    }
  }, {
    key: "resolveMember",
    value: function resolveMember(type, _ref) {
      var meta = _ref.meta,
          query = _ref.query;

      if (!meta) {
        return [];
      }

      if (type === 'timeDimensions') {
        return (query.timeDimensions || []).map(function (m, index) {
          return _objectSpread2({}, m, {
            dimension: _objectSpread2({}, meta.resolveMember(m.dimension, 'dimensions'), {
              granularities: granularities
            }),
            index: index
          });
        });
      }

      return (query[type] || []).map(function (m, index) {
        return _objectSpread2({
          index: index
        }, meta.resolveMember(m, type));
      });
    }
  }, {
    key: "getOrderMembers",
    value: function getOrderMembers(state) {
      var query = state.query,
          meta = state.meta;

      if (!meta) {
        return [];
      }

      var toOrderMember = function toOrderMember(member) {
        return {
          id: member.name,
          title: member.title
        };
      };

      return uniqBy(prop('id'), [].concat(_toConsumableArray(QueryBuilder.resolveMember('measures', state).map(toOrderMember)), _toConsumableArray(QueryBuilder.resolveMember('dimensions', state).map(toOrderMember)), _toConsumableArray(QueryBuilder.resolveMember('timeDimensions', state).map(function (td) {
        return toOrderMember(td.dimension);
      }))).map(function (member) {
        return _objectSpread2({}, member, {
          order: query.order && query.order[member.id] || 'none'
        });
      }));
    }
  }]);

  function QueryBuilder(props) {
    var _this;

    _classCallCheck(this, QueryBuilder);

    _this = _possibleConstructorReturn(this, _getPrototypeOf(QueryBuilder).call(this, props));
    _this.state = _objectSpread2({
      query: props.query,
      chartType: 'line',
      orderMembers: [],
      pivotConfig: null
    }, props.vizState);
    _this.mutexObj = {};
    return _this;
  }

  _createClass(QueryBuilder, [{
    key: "componentDidMount",
    value: function () {
      var _componentDidMount = _asyncToGenerator(
      /*#__PURE__*/
      _regeneratorRuntime.mark(function _callee() {
        var _this$state, query, pivotConfig, meta;

        return _regeneratorRuntime.wrap(function _callee$(_context) {
          while (1) {
            switch (_context.prev = _context.next) {
              case 0:
                _this$state = this.state, query = _this$state.query, pivotConfig = _this$state.pivotConfig;
                _context.next = 3;
                return this.cubejsApi().meta();

              case 3:
                meta = _context.sent;
                this.setState({
                  meta: meta,
                  orderMembers: QueryBuilder.getOrderMembers({
                    meta: meta,
                    query: query
                  }),
                  pivotConfig: ResultSet.getNormalizedPivotConfig(query || {}, pivotConfig)
                });

              case 5:
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
    key: "cubejsApi",
    value: function cubejsApi() {
      var cubejsApi = this.props.cubejsApi; // eslint-disable-next-line react/destructuring-assignment

      return cubejsApi || this.context && this.context.cubejsApi;
    }
  }, {
    key: "isQueryPresent",
    value: function isQueryPresent() {
      var query = this.state.query;
      return QueryRenderer.isQueryPresent(query);
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

      var _this$state2 = this.state,
          meta = _this$state2.meta,
          query = _this$state2.query,
          _this$state2$orderMem = _this$state2.orderMembers,
          orderMembers = _this$state2$orderMem === void 0 ? [] : _this$state2$orderMem,
          chartType = _this$state2.chartType,
          pivotConfig = _this$state2.pivotConfig;
      return _objectSpread2({
        meta: meta,
        query: query,
        validatedQuery: this.validatedQuery(),
        isQueryPresent: this.isQueryPresent(),
        chartType: chartType,
        measures: QueryBuilder.resolveMember('measures', this.state),
        dimensions: QueryBuilder.resolveMember('dimensions', this.state),
        timeDimensions: QueryBuilder.resolveMember('timeDimensions', this.state),
        segments: (meta && query.segments || []).map(function (m, i) {
          return _objectSpread2({
            index: i
          }, meta.resolveMember(m, 'segments'));
        }),
        filters: (meta && query.filters || []).map(function (m, i) {
          return _objectSpread2({}, m, {
            dimension: meta.resolveMember(m.dimension, ['dimensions', 'measures']),
            operators: meta.filterOperatorsForMember(m.dimension, ['dimensions', 'measures']),
            index: i
          });
        }),
        orderMembers: orderMembers,
        availableMeasures: meta && meta.membersForQuery(query, 'measures') || [],
        availableDimensions: meta && meta.membersForQuery(query, 'dimensions') || [],
        availableTimeDimensions: (meta && meta.membersForQuery(query, 'dimensions') || []).filter(function (m) {
          return m.type === 'time';
        }),
        availableSegments: meta && meta.membersForQuery(query, 'segments') || [],
        updateQuery: function updateQuery(queryUpdate) {
          return _this2.updateQuery(queryUpdate);
        },
        updateMeasures: updateMethods('measures'),
        updateDimensions: updateMethods('dimensions'),
        updateSegments: updateMethods('segments'),
        updateTimeDimensions: updateMethods('timeDimensions', toTimeDimension),
        updateFilters: updateMethods('filters', toFilter),
        updateChartType: function updateChartType(newChartType) {
          return _this2.updateVizState({
            chartType: newChartType
          });
        },
        updateOrder: {
          set: function set(memberId) {
            var order = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : 'asc';

            _this2.updateVizState({
              orderMembers: orderMembers.map(function (orderMember) {
                return _objectSpread2({}, orderMember, {
                  order: orderMember.id === memberId ? order : orderMember.order
                });
              })
            });
          },
          update: function update(order) {
            _this2.updateQuery({
              order: order
            });
          },
          reorder: function reorder$$1(sourceIndex, destinationIndex) {
            if (sourceIndex == null || destinationIndex == null) {
              return;
            }

            _this2.updateVizState({
              orderMembers: reorder(orderMembers, sourceIndex, destinationIndex)
            });
          }
        },
        pivotConfig: pivotConfig,
        updatePivotConfig: {
          moveItem: function moveItem(_ref2) {
            var sourceIndex = _ref2.sourceIndex,
                destinationIndex = _ref2.destinationIndex,
                sourceAxis = _ref2.sourceAxis,
                destinationAxis = _ref2.destinationAxis;

            var nextPivotConfig = _objectSpread2({}, pivotConfig, {
              x: _toConsumableArray(pivotConfig.x),
              y: _toConsumableArray(pivotConfig.y)
            });

            var id = pivotConfig[sourceAxis][sourceIndex];
            var lastIndex = nextPivotConfig[destinationAxis].length - 1;

            if (id === 'measures') {
              destinationIndex = lastIndex + 1;
            } else if (destinationIndex >= lastIndex && nextPivotConfig[destinationAxis][lastIndex] === 'measures') {
              destinationIndex = lastIndex - 1;
            }

            nextPivotConfig[sourceAxis].splice(sourceIndex, 1);
            nextPivotConfig[destinationAxis].splice(destinationIndex, 0, id);

            _this2.updateVizState({
              pivotConfig: nextPivotConfig
            });
          },
          update: function update(config) {
            _this2.updateVizState({
              pivotConfig: _objectSpread2({}, pivotConfig, {}, config)
            });
          }
        }
      }, queryRendererProps);
    }
  }, {
    key: "updateQuery",
    value: function updateQuery(queryUpdate) {
      var query = this.state.query;
      this.updateVizState({
        query: _objectSpread2({}, query, {}, queryUpdate)
      });
    }
  }, {
    key: "updateVizState",
    value: function () {
      var _updateVizState = _asyncToGenerator(
      /*#__PURE__*/
      _regeneratorRuntime.mark(function _callee2(state) {
        var _this$props, setQuery, setVizState, _this$state3, stateQuery, statePivotConfig, finalState, _ref3, _, query, _ref4, sqlQuery, activePivotConfig, updatedOrderMembers, currentOrderMemberIds, currentOrderMembers, nextOrder, nextQuery, _finalState, _meta, toSet;

        return _regeneratorRuntime.wrap(function _callee2$(_context2) {
          while (1) {
            switch (_context2.prev = _context2.next) {
              case 0:
                _this$props = this.props, setQuery = _this$props.setQuery, setVizState = _this$props.setVizState;
                _this$state3 = this.state, stateQuery = _this$state3.query, statePivotConfig = _this$state3.pivotConfig;
                finalState = this.applyStateChangeHeuristics(state);
                _ref3 = finalState.query || {}, _ = _ref3.order, query = _objectWithoutProperties(_ref3, ["order"]);

                if (!(finalState.shouldApplyHeuristicOrder && QueryRenderer.isQueryPresent(query))) {
                  _context2.next = 17;
                  break;
                }

                _context2.prev = 5;
                _context2.next = 8;
                return this.cubejsApi().sql(query, {
                  mutexObj: this.mutexObj
                });

              case 8:
                _ref4 = _context2.sent;
                sqlQuery = _ref4.sqlQuery;
                finalState.query.order = sqlQuery.sql.order;
                _context2.next = 17;
                break;

              case 13:
                _context2.prev = 13;
                _context2.t0 = _context2["catch"](5);

                if (!(_context2.t0.response.code !== 'MISSING_DATE_RANGE')) {
                  _context2.next = 17;
                  break;
                }

                throw _context2.t0;

              case 17:
                activePivotConfig = finalState.pivotConfig !== undefined ? finalState.pivotConfig : statePivotConfig;
                updatedOrderMembers = indexBy(prop('id'), QueryBuilder.getOrderMembers(_objectSpread2({}, this.state, {}, finalState)));
                currentOrderMemberIds = (finalState.orderMembers || []).map(function (_ref5) {
                  var id = _ref5.id;
                  return id;
                });
                currentOrderMembers = (finalState.orderMembers || []).filter(function (_ref6) {
                  var id = _ref6.id;
                  return Boolean(updatedOrderMembers[id]);
                });
                Object.entries(updatedOrderMembers).forEach(function (_ref7) {
                  var _ref8 = _slicedToArray(_ref7, 2),
                      id = _ref8[0],
                      orderMember = _ref8[1];

                  if (!currentOrderMemberIds.includes(id)) {
                    currentOrderMembers.push(orderMember);
                  }
                });
                nextOrder = fromPairs(currentOrderMembers.map(function (_ref9) {
                  var id = _ref9.id,
                      order = _ref9.order;
                  return order !== 'none' ? [id, order] : false;
                }).filter(Boolean));
                nextQuery = _objectSpread2({}, stateQuery, {}, query, {
                  order: nextOrder
                });
                finalState = _objectSpread2({}, finalState, {
                  query: nextQuery,
                  orderMembers: currentOrderMembers,
                  pivotConfig: ResultSet.getNormalizedPivotConfig(nextQuery, activePivotConfig)
                });
                this.setState(finalState);
                finalState = _objectSpread2({}, this.state, {}, finalState);

                if (setQuery) {
                  setQuery(finalState.query);
                }

                if (setVizState) {
                  _finalState = finalState, _meta = _finalState.meta, toSet = _objectWithoutProperties(_finalState, ["meta"]);
                  setVizState(toSet);
                }

              case 29:
              case "end":
                return _context2.stop();
            }
          }
        }, _callee2, this, [[5, 13]]);
      }));

      function updateVizState(_x) {
        return _updateVizState.apply(this, arguments);
      }

      return updateVizState;
    }()
  }, {
    key: "validatedQuery",
    value: function validatedQuery() {
      var query = this.state.query;
      return _objectSpread2({}, query, {
        filters: (query.filters || []).filter(function (f) {
          return f.operator;
        })
      });
    }
  }, {
    key: "defaultHeuristics",
    value: function defaultHeuristics(newState) {
      var _this$state4 = this.state,
          query = _this$state4.query,
          sessionGranularity = _this$state4.sessionGranularity;
      var defaultGranularity = sessionGranularity || 'day';

      if (newState.query) {
        var oldQuery = query;
        var newQuery = newState.query;
        var _meta2 = this.state.meta;

        if ((oldQuery.timeDimensions || []).length === 1 && (newQuery.timeDimensions || []).length === 1 && newQuery.timeDimensions[0].granularity && oldQuery.timeDimensions[0].granularity !== newQuery.timeDimensions[0].granularity) {
          newState = _objectSpread2({}, newState, {
            sessionGranularity: newQuery.timeDimensions[0].granularity
          });
        }

        if ((oldQuery.measures || []).length === 0 && (newQuery.measures || []).length > 0 || (oldQuery.measures || []).length === 1 && (newQuery.measures || []).length === 1 && oldQuery.measures[0] !== newQuery.measures[0]) {
          var defaultTimeDimension = _meta2.defaultTimeDimensionNameFor(newQuery.measures[0]);

          newQuery = _objectSpread2({}, newQuery, {
            timeDimensions: defaultTimeDimension ? [{
              dimension: defaultTimeDimension,
              granularity: defaultGranularity
            }] : []
          });
          return _objectSpread2({}, newState, {
            pivotConfig: null,
            shouldApplyHeuristicOrder: true,
            query: newQuery,
            chartType: defaultTimeDimension ? 'line' : 'number'
          });
        }

        if ((oldQuery.dimensions || []).length === 0 && (newQuery.dimensions || []).length > 0) {
          newQuery = _objectSpread2({}, newQuery, {
            timeDimensions: (newQuery.timeDimensions || []).map(function (td) {
              return _objectSpread2({}, td, {
                granularity: undefined
              });
            })
          });
          return _objectSpread2({}, newState, {
            pivotConfig: null,
            shouldApplyHeuristicOrder: true,
            query: newQuery,
            chartType: 'table'
          });
        }

        if ((oldQuery.dimensions || []).length > 0 && (newQuery.dimensions || []).length === 0) {
          newQuery = _objectSpread2({}, newQuery, {
            timeDimensions: (newQuery.timeDimensions || []).map(function (td) {
              return _objectSpread2({}, td, {
                granularity: td.granularity || defaultGranularity
              });
            })
          });
          return _objectSpread2({}, newState, {
            pivotConfig: null,
            shouldApplyHeuristicOrder: true,
            query: newQuery,
            chartType: (newQuery.timeDimensions || []).length ? 'line' : 'number'
          });
        }

        if (((oldQuery.dimensions || []).length > 0 || (oldQuery.measures || []).length > 0) && (newQuery.dimensions || []).length === 0 && (newQuery.measures || []).length === 0) {
          newQuery = _objectSpread2({}, newQuery, {
            timeDimensions: [],
            filters: []
          });
          return _objectSpread2({}, newState, {
            pivotConfig: null,
            shouldApplyHeuristicOrder: true,
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

          return _objectSpread2({}, newState, {
            pivotConfig: null,
            query: _objectSpread2({}, query, {
              timeDimensions: [_objectSpread2({}, td, {
                granularity: defaultGranularity
              })]
            })
          });
        }

        if ((newChartType === 'pie' || newChartType === 'table' || newChartType === 'number') && (query.timeDimensions || []).length === 1 && query.timeDimensions[0].granularity) {
          var _query$timeDimensions2 = _slicedToArray(query.timeDimensions, 1),
              _td = _query$timeDimensions2[0];

          return _objectSpread2({}, newState, {
            pivotConfig: null,
            shouldApplyHeuristicOrder: true,
            query: _objectSpread2({}, query, {
              timeDimensions: [_objectSpread2({}, _td, {
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
      var _this$props2 = this.props,
          stateChangeHeuristics = _this$props2.stateChangeHeuristics,
          disableHeuristics = _this$props2.disableHeuristics;

      if (disableHeuristics) {
        return newState;
      }

      return stateChangeHeuristics && stateChangeHeuristics(this.state, newState) || this.defaultHeuristics(newState);
    }
  }, {
    key: "render",
    value: function render() {
      var _this3 = this;

      var _this$props3 = this.props,
          cubejsApi = _this$props3.cubejsApi,
          _render = _this$props3.render,
          wrapWithQueryRenderer = _this$props3.wrapWithQueryRenderer;

      if (wrapWithQueryRenderer) {
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
      } else {
        if (_render) {
          return _render(this.prepareRenderProps());
        }

        return null;
      }
    }
  }]);

  return QueryBuilder;
}(React.Component);
QueryBuilder.contextType = CubeContext;
QueryBuilder.propTypes = {
  render: func,
  stateChangeHeuristics: func,
  setQuery: func,
  setVizState: func,
  cubejsApi: object,
  disableHeuristics: bool,
  wrapWithQueryRenderer: bool,
  query: object,
  vizState: object
};
QueryBuilder.defaultProps = {
  cubejsApi: null,
  query: {},
  setQuery: null,
  setVizState: null,
  stateChangeHeuristics: null,
  disableHeuristics: false,
  render: null,
  wrapWithQueryRenderer: true,
  vizState: {}
};

var CubeProvider = function CubeProvider(_ref) {
  var cubejsApi = _ref.cubejsApi,
      children = _ref.children;
  return React.createElement(CubeContext.Provider, {
    value: {
      cubejsApi: cubejsApi
    }
  }, children);
};

CubeProvider.propTypes = {
  cubejsApi: object.isRequired,
  children: any.isRequired
};

function useDeepCompareMemoize(value) {
  var ref = useRef([]);

  if (!equals(value, ref.current)) {
    ref.current = value;
  }

  return ref.current;
}

var useCubeQuery = (function (query) {
  var options = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};
  var mutexRef = useRef({});

  var _useState = useState(null),
      _useState2 = _slicedToArray(_useState, 2),
      currentQuery = _useState2[0],
      setCurrentQuery = _useState2[1];

  var _useState3 = useState(false),
      _useState4 = _slicedToArray(_useState3, 2),
      isLoading = _useState4[0],
      setLoading = _useState4[1];

  var _useState5 = useState(null),
      _useState6 = _slicedToArray(_useState5, 2),
      resultSet = _useState6[0],
      setResultSet = _useState6[1];

  var _useState7 = useState(null),
      _useState8 = _slicedToArray(_useState7, 2),
      error = _useState8[0],
      setError = _useState8[1];

  var context = useContext(CubeContext);
  var subscribeRequest = null;
  useEffect(function () {
    var _options$skip = options.skip,
        skip = _options$skip === void 0 ? false : _options$skip,
        resetResultSetOnChange = options.resetResultSetOnChange;

    function loadQuery() {
      return _loadQuery.apply(this, arguments);
    }

    function _loadQuery() {
      _loadQuery = _asyncToGenerator(
      /*#__PURE__*/
      _regeneratorRuntime.mark(function _callee() {
        var hasOrderChanged, cubejsApi;
        return _regeneratorRuntime.wrap(function _callee$(_context) {
          while (1) {
            switch (_context.prev = _context.next) {
              case 0:
                if (!(!skip && query && isQueryPresent(query))) {
                  _context.next = 26;
                  break;
                }

                hasOrderChanged = !equals(Object.keys(currentQuery && currentQuery.order || {}), Object.keys(query.order || {}));

                if (hasOrderChanged || !equals(currentQuery, query)) {
                  if (resetResultSetOnChange == null || resetResultSetOnChange) {
                    setResultSet(null);
                  }

                  setError(null);
                  setCurrentQuery(query);
                }

                setLoading(true);
                _context.prev = 4;

                if (!subscribeRequest) {
                  _context.next = 9;
                  break;
                }

                _context.next = 8;
                return subscribeRequest.unsubscribe();

              case 8:
                subscribeRequest = null;

              case 9:
                cubejsApi = options.cubejsApi || context && context.cubejsApi;

                if (!options.subscribe) {
                  _context.next = 14;
                  break;
                }

                subscribeRequest = cubejsApi.subscribe(query, {
                  mutexObj: mutexRef.current,
                  mutexKey: 'query'
                }, function (e, result) {
                  if (e) {
                    setError(e);
                  } else {
                    setResultSet(result);
                  }

                  setLoading(false);
                });
                _context.next = 20;
                break;

              case 14:
                _context.t0 = setResultSet;
                _context.next = 17;
                return cubejsApi.load(query, {
                  mutexObj: mutexRef.current,
                  mutexKey: 'query'
                });

              case 17:
                _context.t1 = _context.sent;
                (0, _context.t0)(_context.t1);
                setLoading(false);

              case 20:
                _context.next = 26;
                break;

              case 22:
                _context.prev = 22;
                _context.t2 = _context["catch"](4);
                setError(_context.t2);
                setLoading(false);

              case 26:
              case "end":
                return _context.stop();
            }
          }
        }, _callee, null, [[4, 22]]);
      }));
      return _loadQuery.apply(this, arguments);
    }

    loadQuery();
    return function () {
      if (subscribeRequest) {
        subscribeRequest.unsubscribe();
        subscribeRequest = null;
      }
    };
  }, useDeepCompareMemoize([query, Object.keys(query && query.order || {}), options, context]));
  return {
    isLoading: isLoading,
    resultSet: resultSet,
    error: error
  };
});

export { QueryRenderer, QueryRendererWithTotals, QueryBuilder, isQueryPresent, CubeContext, CubeProvider, useCubeQuery };
