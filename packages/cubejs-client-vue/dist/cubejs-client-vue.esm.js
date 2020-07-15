import 'core-js/modules/es.array.iterator';
import 'core-js/modules/es.array.map';
import 'core-js/modules/es.object.assign';
import 'core-js/modules/es.object.keys';
import 'core-js/modules/es.object.to-string';
import 'core-js/modules/es.promise';
import 'core-js/modules/es.string.iterator';
import 'core-js/modules/web.dom-collections.iterator';
import _slicedToArray from '@babel/runtime/helpers/slicedToArray';
import _objectSpread2 from '@babel/runtime/helpers/objectSpread';
import _regeneratorRuntime from '@babel/runtime/regenerator';
import 'regenerator-runtime/runtime';
import _asyncToGenerator from '@babel/runtime/helpers/asyncToGenerator';
import { toPairs, fromPairs } from 'ramda';
import 'core-js/modules/es.array.filter';
import 'core-js/modules/es.array.find';
import 'core-js/modules/es.array.find-index';
import 'core-js/modules/es.array.for-each';
import 'core-js/modules/es.array.slice';
import 'core-js/modules/es.array.splice';
import 'core-js/modules/es.function.name';
import 'core-js/modules/web.dom-collections.for-each';

var QueryRenderer = {
  props: {
    query: {
      type: Object,
      "default": function _default() {
        return {};
      }
    },
    // TODO: validate with current react implementation
    queries: {
      type: Object
    },
    loadSql: {
      required: false
    },
    cubejsApi: {
      type: Object,
      required: true
    },
    builderProps: {
      type: Object,
      required: false,
      "default": function _default() {
        return {};
      }
    }
  },
  data: function data() {
    return {
      mutexObj: {},
      error: undefined,
      resultSet: undefined,
      loading: true,
      sqlQuery: undefined
    };
  },
  mounted: function mounted() {
    var _this = this;

    return _asyncToGenerator(
    /*#__PURE__*/
    _regeneratorRuntime.mark(function _callee() {
      var query, queries;
      return _regeneratorRuntime.wrap(function _callee$(_context) {
        while (1) {
          switch (_context.prev = _context.next) {
            case 0:
              query = _this.query, queries = _this.queries;

              if (!query) {
                _context.next = 4;
                break;
              }

              _context.next = 4;
              return _this.load();

            case 4:
              if (!queries) {
                _context.next = 7;
                break;
              }

              _context.next = 7;
              return _this.loadQueries(queries);

            case 7:
            case "end":
              return _context.stop();
          }
        }
      }, _callee);
    }))();
  },
  render: function render(createElement) {
    var $scopedSlots = this.$scopedSlots,
        resultSet = this.resultSet,
        error = this.error,
        loading = this.loading,
        sqlQuery = this.sqlQuery;
    var empty = createElement('div', {});
    var slot = this.$slots.empty ? this.$slots.empty : empty;
    var controls = createElement('div', {});
    var onlyDefault = !('empty' in this.$slots) && !('error' in this.$scopedSlots);

    if ($scopedSlots.builder && this.builderProps.measures) {
      controls = $scopedSlots.builder(_objectSpread2({}, this.builderProps));
    }

    if (!loading && resultSet && !error || onlyDefault) {
      var slotProps = {
        resultSet: resultSet,
        sqlQuery: sqlQuery,
        query: this.builderProps.query || this.query
      };

      if (onlyDefault) {
        Object.assign(slotProps, _objectSpread2({
          loading: loading,
          error: error
        }, this.builderProps));
      }

      slot = $scopedSlots["default"] ? $scopedSlots["default"](slotProps) : slot;
    } else if (error) {
      slot = $scopedSlots.error ? $scopedSlots.error({
        error: error,
        sqlQuery: sqlQuery
      }) : slot;
    }

    return createElement('div', {}, [controls, slot]);
  },
  methods: {
    load: function load() {
      var _this2 = this;

      return _asyncToGenerator(
      /*#__PURE__*/
      _regeneratorRuntime.mark(function _callee2() {
        var query;
        return _regeneratorRuntime.wrap(function _callee2$(_context2) {
          while (1) {
            switch (_context2.prev = _context2.next) {
              case 0:
                query = _this2.query;
                _context2.prev = 1;
                _this2.loading = true;
                _this2.error = undefined;

                if (!(query && Object.keys(query).length > 0)) {
                  _context2.next = 23;
                  break;
                }

                if (!(_this2.loadSql === 'only')) {
                  _context2.next = 11;
                  break;
                }

                _context2.next = 8;
                return _this2.cubejsApi.sql(query, {
                  mutexObj: _this2.mutexObj,
                  mutexKey: 'sql'
                });

              case 8:
                _this2.sqlQuery = _context2.sent;
                _context2.next = 23;
                break;

              case 11:
                if (!_this2.loadSql) {
                  _context2.next = 20;
                  break;
                }

                _context2.next = 14;
                return _this2.cubejsApi.sql(query, {
                  mutexObj: _this2.mutexObj,
                  mutexKey: 'sql'
                });

              case 14:
                _this2.sqlQuery = _context2.sent;
                _context2.next = 17;
                return _this2.cubejsApi.load(query, {
                  mutexObj: _this2.mutexObj,
                  mutexKey: 'query'
                });

              case 17:
                _this2.resultSet = _context2.sent;
                _context2.next = 23;
                break;

              case 20:
                _context2.next = 22;
                return _this2.cubejsApi.load(query, {
                  mutexObj: _this2.mutexObj,
                  mutexKey: 'query'
                });

              case 22:
                _this2.resultSet = _context2.sent;

              case 23:
                _this2.loading = false;
                _context2.next = 31;
                break;

              case 26:
                _context2.prev = 26;
                _context2.t0 = _context2["catch"](1);
                _this2.error = _context2.t0;
                _this2.resultSet = undefined;
                _this2.loading = false;

              case 31:
              case "end":
                return _context2.stop();
            }
          }
        }, _callee2, null, [[1, 26]]);
      }))();
    },
    loadQueries: function loadQueries() {
      var _this3 = this;

      return _asyncToGenerator(
      /*#__PURE__*/
      _regeneratorRuntime.mark(function _callee3() {
        var queries, resultPromises;
        return _regeneratorRuntime.wrap(function _callee3$(_context3) {
          while (1) {
            switch (_context3.prev = _context3.next) {
              case 0:
                queries = _this3.queries;
                _context3.prev = 1;
                _this3.error = undefined;
                _this3.loading = true;
                resultPromises = Promise.all(toPairs(queries).map(function (_ref) {
                  var _ref2 = _slicedToArray(_ref, 2),
                      name = _ref2[0],
                      query = _ref2[1];

                  return _this3.cubejsApi.load(query, {
                    mutexObj: _this3.mutexObj,
                    mutexKey: name
                  }).then(function (r) {
                    return [name, r];
                  });
                }));
                _context3.t0 = fromPairs;
                _context3.next = 8;
                return resultPromises;

              case 8:
                _context3.t1 = _context3.sent;
                _this3.resultSet = (0, _context3.t0)(_context3.t1);
                _this3.loading = false;
                _context3.next = 17;
                break;

              case 13:
                _context3.prev = 13;
                _context3.t2 = _context3["catch"](1);
                _this3.error = _context3.t2;
                _this3.loading = false;

              case 17:
              case "end":
                return _context3.stop();
            }
          }
        }, _callee3, null, [[1, 13]]);
      }))();
    }
  },
  watch: {
    query: {
      handler: function handler(val) {
        if (val) {
          this.load();
        }
      },
      deep: true
    },
    queries: {
      handler: function handler(val) {
        if (val) {
          this.loadQueries();
        }
      },
      deep: true
    }
  }
};

var QUERY_ELEMENTS = ['measures', 'dimensions', 'segments', 'timeDimensions', 'filters'];
var QueryBuilder = {
  components: {
    QueryRenderer: QueryRenderer
  },
  props: {
    query: {
      type: Object
    },
    cubejsApi: {
      type: Object,
      required: true
    }
  },
  data: function data() {
    var data = {
      meta: undefined,
      chartType: undefined,
      measures: [],
      dimensions: [],
      segments: [],
      timeDimensions: [],
      filters: [],
      availableMeasures: [],
      availableDimensions: [],
      availableTimeDimensions: [],
      availableSegments: [],
      limit: null,
      offset: null,
      renewQuery: false,
      order: null
    };
    data.granularities = [{
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
    return data;
  },
  render: function render(createElement) {
    var _this = this;

    var chartType = this.chartType,
        cubejsApi = this.cubejsApi,
        dimensions = this.dimensions,
        filters = this.filters,
        measures = this.measures,
        meta = this.meta,
        query = this.query,
        segments = this.segments,
        timeDimensions = this.timeDimensions,
        validatedQuery = this.validatedQuery,
        isQueryPresent = this.isQueryPresent,
        availableSegments = this.availableSegments,
        availableTimeDimensions = this.availableTimeDimensions,
        availableDimensions = this.availableDimensions,
        availableMeasures = this.availableMeasures,
        limit = this.limit,
        offset = this.offset,
        setLimit = this.setLimit,
        removeLimit = this.removeLimit,
        setOffset = this.setOffset,
        removeOffset = this.removeOffset,
        renewQuery = this.renewQuery,
        order = this.order;
    var builderProps = {};

    if (meta) {
      builderProps = {
        query: query,
        validatedQuery: validatedQuery,
        isQueryPresent: isQueryPresent,
        chartType: chartType,
        measures: measures,
        dimensions: dimensions,
        segments: segments,
        timeDimensions: timeDimensions,
        filters: filters,
        availableSegments: availableSegments,
        availableTimeDimensions: availableTimeDimensions,
        availableDimensions: availableDimensions,
        availableMeasures: availableMeasures,
        updateChartType: this.updateChart,
        limit: limit,
        offset: offset,
        setLimit: setLimit,
        removeLimit: removeLimit,
        setOffset: setOffset,
        removeOffset: removeOffset,
        renewQuery: renewQuery,
        order: order,
        setOrder: this.setOrder
      };
      QUERY_ELEMENTS.forEach(function (e) {
        var name = e.charAt(0).toUpperCase() + e.slice(1);

        builderProps["add".concat(name)] = function (member) {
          _this.addMember(e, member);
        };

        builderProps["update".concat(name)] = function (member, updateWith) {
          _this.updateMember(e, member, updateWith);
        };

        builderProps["remove".concat(name)] = function (member) {
          _this.removeMember(e, member);
        };

        builderProps["set".concat(name)] = function (members) {
          _this.setMembers(e, members);
        };
      });
    } // Pass parent slots to child QueryRenderer component


    var children = Object.keys(this.$slots).map(function (slot) {
      return createElement('template', {
        slot: slot
      }, _this.$slots[slot]);
    });
    return createElement(QueryRenderer, {
      props: {
        query: this.validatedQuery,
        cubejsApi: cubejsApi,
        builderProps: builderProps
      },
      scopedSlots: this.$scopedSlots
    }, children);
  },
  computed: {
    isQueryPresent: function isQueryPresent() {
      var query = this.query;
      return Object.keys(query).length > 0;
    },
    validatedQuery: function validatedQuery() {
      var _this2 = this;

      var validatedQuery = {};

      var toQuery = function toQuery(member) {
        return member.name;
      }; // TODO: implement timezone


      var hasElements = false;
      QUERY_ELEMENTS.forEach(function (e) {
        if (!_this2[e]) {
          return;
        }

        if (e === 'timeDimensions') {
          toQuery = function toQuery(member) {
            return {
              dimension: member.dimension.name,
              granularity: member.granularity,
              dateRange: member.dateRange
            };
          };
        } else if (e === 'filters') {
          toQuery = function toQuery(member) {
            return {
              member: member.member.name,
              operator: member.operator,
              values: member.values
            };
          };
        }

        if (_this2[e].length > 0) {
          validatedQuery[e] = _this2[e].map(function (x) {
            return toQuery(x);
          });
          hasElements = true;
        }
      }); // TODO: implement default heuristics

      if (validatedQuery.filters) {
        validatedQuery.filters = validatedQuery.filters.filter(function (f) {
          return f.operator;
        });
      } // only set limit and offset if there are elements otherwise an invalid request with just limit/offset
      // gets sent when the component is first mounted, but before the actual query is constructed.


      if (hasElements) {
        if (this.limit) {
          validatedQuery.limit = this.limit;
        }

        if (this.offset) {
          validatedQuery.offset = this.offset;
        }

        if (this.order) {
          validatedQuery.order = this.order;
        }

        if (this.renewQuery) {
          validatedQuery.renewQuery = this.renewQuery;
        }
      }

      return validatedQuery;
    }
  },
  mounted: function mounted() {
    var _this3 = this;

    return _asyncToGenerator(
    /*#__PURE__*/
    _regeneratorRuntime.mark(function _callee() {
      return _regeneratorRuntime.wrap(function _callee$(_context) {
        while (1) {
          switch (_context.prev = _context.next) {
            case 0:
              _context.next = 2;
              return _this3.cubejsApi.meta();

            case 2:
              _this3.meta = _context.sent;

              _this3.copyQueryFromProps();

            case 4:
            case "end":
              return _context.stop();
          }
        }
      }, _callee);
    }))();
  },
  methods: {
    copyQueryFromProps: function copyQueryFromProps() {
      var _this4 = this;

      var _this$query = this.query,
          measures = _this$query.measures,
          dimensions = _this$query.dimensions,
          segments = _this$query.segments,
          timeDimensions = _this$query.timeDimensions,
          filters = _this$query.filters,
          limit = _this$query.limit,
          offset = _this$query.offset,
          renewQuery = _this$query.renewQuery,
          order = _this$query.order;
      this.measures = (measures || []).map(function (m, i) {
        return _objectSpread2({
          index: i
        }, _this4.meta.resolveMember(m, 'measures'));
      });
      this.dimensions = (dimensions || []).map(function (m, i) {
        return _objectSpread2({
          index: i
        }, _this4.meta.resolveMember(m, 'dimensions'));
      });
      this.segments = (segments || []).map(function (m, i) {
        return _objectSpread2({
          index: i
        }, _this4.meta.resolveMember(m, 'segments'));
      });
      this.timeDimensions = (timeDimensions || []).map(function (m, i) {
        return _objectSpread2({}, m, {
          dimension: _objectSpread2({}, _this4.meta.resolveMember(m.dimension, 'dimensions'), {
            granularities: _this4.granularities
          }),
          index: i
        });
      });
      this.filters = (filters || []).map(function (m, i) {
        return _objectSpread2({}, m, {
          member: _this4.meta.resolveMember(m.member || m.dimension, ['dimensions', 'measures']),
          operators: _this4.meta.filterOperatorsForMember(m.member || m.dimension, ['dimensions', 'measures']),
          index: i
        });
      });
      this.availableMeasures = this.meta.membersForQuery({}, 'measures') || [];
      this.availableDimensions = this.meta.membersForQuery({}, 'dimensions') || [];
      this.availableTimeDimensions = (this.meta.membersForQuery({}, 'dimensions') || []).filter(function (m) {
        return m.type === 'time';
      });
      this.availableSegments = this.meta.membersForQuery({}, 'segments') || [];
      this.limit = limit || null;
      this.offset = offset || null;
      this.renewQuery = renewQuery || false;
      this.order = order || null;
    },
    addMember: function addMember(element, member) {
      var name = element.charAt(0).toUpperCase() + element.slice(1);
      var mem;

      if (element === 'timeDimensions') {
        mem = this["available".concat(name)].find(function (m) {
          return m.name === member.dimension;
        });

        if (mem) {
          var dimension = _objectSpread2({}, this.meta.resolveMember(mem.name, 'dimensions'), {
            granularities: this.granularities
          });

          mem = _objectSpread2({}, mem, {
            granularity: member.granularity,
            dateRange: member.dateRange,
            dimension: dimension,
            index: this[element].length
          });
        }
      } else if (element === 'filters') {
        var filterMember = _objectSpread2({}, this.meta.resolveMember(member.member || member.dimension, ['dimensions', 'measures']));

        mem = _objectSpread2({}, member, {
          member: filterMember
        });
      } else {
        mem = this["available".concat(name)].find(function (m) {
          return m.name === member;
        });
      }

      if (mem) {
        this[element].push(mem);
      }
    },
    removeMember: function removeMember(element, member) {
      var name = element.charAt(0).toUpperCase() + element.slice(1);
      var mem;

      if (element === 'timeDimensions') {
        mem = this["available".concat(name)].find(function (x) {
          return x.name === member;
        });
      } else if (element === 'filters') {
        mem = member;
      } else {
        mem = this["available".concat(name)].find(function (m) {
          return m.name === member;
        });
      }

      if (mem) {
        var index = this[element].findIndex(function (x) {
          return x.name === mem;
        });
        this[element].splice(index, 1);
      }
    },
    updateMember: function updateMember(element, old, member) {
      var name = element.charAt(0).toUpperCase() + element.slice(1);
      var mem;
      var index;

      if (element === 'timeDimensions') {
        index = this[element].findIndex(function (x) {
          return x.dimension.name === old.dimension;
        });
        mem = this["available".concat(name)].find(function (m) {
          return m.name === member.dimension;
        });

        if (mem) {
          var dimension = _objectSpread2({}, this.meta.resolveMember(mem.name, 'dimensions'), {
            granularities: this.granularities
          });

          mem = _objectSpread2({}, mem, {
            dimension: dimension,
            granularity: member.granularity,
            dateRange: member.dateRange,
            index: index
          });
        }
      } else if (element === 'filters') {
        index = this[element].findIndex(function (x) {
          return x.dimension === old;
        });

        var filterMember = _objectSpread2({}, this.meta.resolveMember(member.member || member.dimension, ['dimensions', 'measures']));

        mem = _objectSpread2({}, member, {
          member: filterMember
        });
      } else {
        index = this[element].findIndex(function (x) {
          return x.name === old;
        });
        mem = this["available".concat(name)].find(function (m) {
          return m.name === member;
        });
      }

      if (mem) {
        this[element].splice(index, 1, mem);
      }
    },
    setMembers: function setMembers(element, members) {
      var _this5 = this;

      var name = element.charAt(0).toUpperCase() + element.slice(1);
      var mem;
      var elements = [];
      members.filter(Boolean).forEach(function (m) {
        if (element === 'timeDimensions') {
          mem = _this5["available".concat(name)].find(function (x) {
            return x.name === m.dimension;
          });

          if (mem) {
            var dimension = _objectSpread2({}, _this5.meta.resolveMember(mem.name, 'dimensions'), {
              granularities: _this5.granularities
            });

            mem = _objectSpread2({}, mem, {
              granularity: m.granularity,
              dateRange: m.dateRange,
              dimension: dimension,
              index: _this5[element].length
            });
          }
        } else if (element === 'filters') {
          var member = _objectSpread2({}, _this5.meta.resolveMember(m.member || m.dimension, ['dimensions', 'measures']));

          mem = _objectSpread2({}, m, {
            member: member
          });
        } else {
          mem = _this5["available".concat(name)].find(function (x) {
            return x.name === m;
          });
        }

        if (mem) {
          elements.push(mem);
        }
      });
      this[element] = elements;
    },
    setLimit: function setLimit(limit) {
      this.limit = limit;
    },
    removeLimit: function removeLimit() {
      this.limit = null;
    },
    setOffset: function setOffset(offset) {
      this.offset = offset;
    },
    removeOffset: function removeOffset() {
      this.offset = null;
    },
    updateChart: function updateChart(chartType) {
      this.chartType = chartType;
    },
    setOrder: function setOrder() {
      var order = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {};
      this.order = order;
    }
  },
  watch: {
    query: {
      deep: true,
      handler: function handler() {
        if (!this.meta) {
          // this is ok as if meta has not been loaded by the time query prop has changed,
          // then the promise for loading meta (found in mounted()) will call
          // copyQueryFromProps and will therefore update anyway.
          return;
        }

        this.copyQueryFromProps();
      }
    }
  }
};

var index = {};

export default index;
export { QueryRenderer, QueryBuilder };
