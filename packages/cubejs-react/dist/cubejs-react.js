import 'core-js/modules/es6.array.map';
import _slicedToArray from '@babel/runtime/helpers/slicedToArray';
import 'core-js/modules/es6.promise';
import 'core-js/modules/es6.string.iterator';
import 'core-js/modules/web.dom.iterable';
import 'core-js/modules/es6.array.iterator';
import 'core-js/modules/es6.object.keys';
import _classCallCheck from '@babel/runtime/helpers/classCallCheck';
import _createClass from '@babel/runtime/helpers/createClass';
import _possibleConstructorReturn from '@babel/runtime/helpers/possibleConstructorReturn';
import _getPrototypeOf from '@babel/runtime/helpers/getPrototypeOf';
import _inherits from '@babel/runtime/helpers/inherits';
import React from 'react';
import { func, object, any } from 'prop-types';
import { equals, toPairs, fromPairs } from 'ramda';
import _extends from '@babel/runtime/helpers/extends';
import _objectSpread from '@babel/runtime/helpers/objectSpread';
import _objectWithoutProperties from '@babel/runtime/helpers/objectWithoutProperties';
import 'core-js/modules/es6.array.filter';
import _defineProperty from '@babel/runtime/helpers/defineProperty';
import 'core-js/modules/es6.function.name';
import _regeneratorRuntime from '@babel/runtime/regenerator';
import 'regenerator-runtime/runtime';
import _asyncToGenerator from '@babel/runtime/helpers/asyncToGenerator';

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

      if (!equals(prevProps.query, query)) {
        this.load(query);
      }

      var queries = this.props.queries;

      if (!equals(prevProps.queries, queries)) {
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
      var resultPromises = Promise.all(toPairs(queries).map(function (_ref3) {
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
          resultSet: fromPairs(resultSet),
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
  render: func,
  afterRender: func,
  cubejsApi: object,
  query: object,
  queries: object,
  loadSql: any
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

      return function componentDidMount() {
        return _componentDidMount.apply(this, arguments);
      };
    }()
  }, {
    key: "render",
    value: function render() {
      var _this2 = this;

      return React.createElement(QueryRenderer, {
        query: this.state.query,
        cubejsApi: this.props.cubejsApi,
        render: function render(queryRendererProps) {
          if (_this2.props.render) {
            return _this2.props.render(_this2.prepareRenderProps(queryRendererProps));
          }
        }
      });
    }
  }, {
    key: "prepareRenderProps",
    value: function prepareRenderProps(queryRendererProps) {
      var _this3 = this;

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

      var updateMethods = function updateMethods(memberType) {
        var toQuery = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : getName;
        return {
          add: function add(member) {
            return _this3.setState({
              query: _objectSpread({}, _this3.state.query, _defineProperty({}, memberType, (_this3.state.query[memberType] || []).concat(toQuery(member))))
            });
          },
          remove: function remove(member) {
            var members = (_this3.state.query[memberType] || []).concat([]);
            members.splice(member.index, 1);
            return _this3.setState({
              query: _objectSpread({}, _this3.state.query, _defineProperty({}, memberType, members))
            });
          },
          update: function update(member, updateWith) {
            var members = (_this3.state.query[memberType] || []).concat([]);
            members.splice(member.index, 1, toQuery(updateWith));
            return _this3.setState({
              query: _objectSpread({}, _this3.state.query, _defineProperty({}, memberType, members))
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
        chartType: this.state.chartType,
        measures: (this.state.meta && this.state.query.measures || []).map(function (m, i) {
          return _objectSpread({
            index: i
          }, _this3.state.meta.resolveMember(m, 'measures'));
        }),
        dimensions: (this.state.meta && this.state.query.dimensions || []).map(function (m, i) {
          return _objectSpread({
            index: i
          }, _this3.state.meta.resolveMember(m, 'dimensions'));
        }),
        segments: (this.state.meta && this.state.query.segments || []).map(function (m, i) {
          return _objectSpread({
            index: i
          }, _this3.state.meta.resolveMember(m, 'segments'));
        }),
        timeDimensions: (this.state.meta && this.state.query.timeDimensions || []).map(function (m, i) {
          return _objectSpread({}, m, {
            dimension: _objectSpread({}, _this3.state.meta.resolveMember(m.dimension, 'dimensions'), {
              granularities: granularities
            }),
            index: i
          });
        }),
        filters: (this.state.meta && this.state.query.filters || []).map(function (m, i) {
          return _objectSpread({}, m, {
            dimension: _this3.state.meta.resolveMember(m.dimension, 'dimensions'),
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
        updateChartType: function updateChartType(chartType) {
          return _this3.setState({
            chartType: chartType
          });
        }
      }, queryRendererProps);
    }
  }]);

  return QueryBuilder;
}(React.Component);

export { QueryRenderer, QueryRendererWithTotals, QueryBuilder };
