import _regeneratorRuntime from '@babel/runtime/regenerator';
import 'regenerator-runtime/runtime';
import _asyncToGenerator from '@babel/runtime/helpers/asyncToGenerator';
import 'core-js/modules/es6.object.assign';
import _classCallCheck from '@babel/runtime/helpers/classCallCheck';
import _createClass from '@babel/runtime/helpers/createClass';
import 'core-js/modules/es6.promise';
import fetch from 'cross-fetch';
import 'core-js/modules/es6.number.constructor';
import 'core-js/modules/es6.number.parse-float';
import _objectSpread from '@babel/runtime/helpers/objectSpread';
import 'core-js/modules/web.dom.iterable';
import 'core-js/modules/es6.array.iterator';
import 'core-js/modules/es6.object.keys';
import _slicedToArray from '@babel/runtime/helpers/slicedToArray';
import _defineProperty from '@babel/runtime/helpers/defineProperty';
import 'core-js/modules/es6.array.reduce';
import 'core-js/modules/es6.array.index-of';
import 'core-js/modules/es6.array.find';
import 'core-js/modules/es6.array.filter';
import _objectWithoutProperties from '@babel/runtime/helpers/objectWithoutProperties';
import 'core-js/modules/es6.string.iterator';
import 'core-js/modules/es6.array.from';
import 'core-js/modules/es6.array.map';
import { pipe, map, filter, reduce, minBy, maxBy, groupBy, equals, unnest, toPairs, uniq, dropLast, fromPairs } from 'ramda';
import Moment from 'moment';
import momentRange from 'moment-range';
import 'core-js/modules/es6.array.is-array';
import 'core-js/modules/es6.regexp.split';
import 'core-js/modules/es6.function.name';

var moment = momentRange.extendMoment(Moment);
var TIME_SERIES = {
  day: function day(range) {
    return Array.from(range.by('day')).map(function (d) {
      return d.format('YYYY-MM-DDT00:00:00.000');
    });
  },
  month: function month(range) {
    return Array.from(range.snapTo('month').by('month')).map(function (d) {
      return d.format('YYYY-MM-01T00:00:00.000');
    });
  },
  year: function year(range) {
    return Array.from(range.snapTo('year').by('year')).map(function (d) {
      return d.format('YYYY-01-01T00:00:00.000');
    });
  },
  hour: function hour(range) {
    return Array.from(range.by('hour')).map(function (d) {
      return d.format('YYYY-MM-DDTHH:00:00.000');
    });
  },
  week: function week(range) {
    return Array.from(range.snapTo('isoweek').by('week')).map(function (d) {
      return d.startOf('isoweek').format('YYYY-MM-DDT00:00:00.000');
    });
  }
};

var ResultSet =
/*#__PURE__*/
function () {
  function ResultSet(loadResponse) {
    _classCallCheck(this, ResultSet);

    this.loadResponse = loadResponse;
  }

  _createClass(ResultSet, [{
    key: "series",
    value: function series(pivotConfig) {
      var _this = this;

      return this.seriesNames(pivotConfig).map(function (_ref) {
        var title = _ref.title,
            key = _ref.key;
        return {
          title: title,
          series: _this.chartPivot(pivotConfig).map(function (_ref2) {
            var category = _ref2.category,
                x = _ref2.x,
                obj = _objectWithoutProperties(_ref2, ["category", "x"]);

            return {
              value: obj[key],
              category: category,
              x: x
            };
          })
        };
      });
    }
  }, {
    key: "axisValues",
    value: function axisValues(axis) {
      var query = this.loadResponse.query;
      return function (row) {
        var value = function value(measure) {
          return axis.filter(function (d) {
            return d !== 'measures';
          }).map(function (d) {
            return row[d] != null ? row[d] : null;
          }).concat(measure ? [measure] : []);
        };

        if (axis.find(function (d) {
          return d === 'measures';
        }) && (query.measures || []).length) {
          return query.measures.map(value);
        }

        return [value()];
      };
    }
  }, {
    key: "axisValuesString",
    value: function axisValuesString(axisValues, delimiter) {
      var formatValue = function formatValue(v) {
        if (v == null) {
          return '∅';
        } else if (v === '') {
          return '[Empty string]';
        } else {
          return v;
        }
      };

      return axisValues.map(formatValue).join(delimiter || ', ');
    }
  }, {
    key: "normalizePivotConfig",
    value: function normalizePivotConfig(pivotConfig) {
      var query = this.loadResponse.query;
      var timeDimensions = (query.timeDimensions || []).filter(function (td) {
        return !!td.granularity;
      });
      pivotConfig = pivotConfig || (timeDimensions.length ? {
        x: timeDimensions.map(function (td) {
          return td.dimension;
        }),
        y: query.dimensions || []
      } : {
        x: query.dimensions || [],
        y: []
      });
      pivotConfig.x = pivotConfig.x || [];
      pivotConfig.y = pivotConfig.y || [];
      var allIncludedDimensions = pivotConfig.x.concat(pivotConfig.y);
      var allDimensions = timeDimensions.map(function (td) {
        return td.dimension;
      }).concat(query.dimensions);
      pivotConfig.x = pivotConfig.x.concat(allDimensions.filter(function (d) {
        return allIncludedDimensions.indexOf(d) === -1;
      }));

      if (!pivotConfig.x.concat(pivotConfig.y).find(function (d) {
        return d === 'measures';
      })) {
        pivotConfig.y = pivotConfig.y.concat(['measures']);
      }

      if (pivotConfig.fillMissingDates == null) {
        pivotConfig.fillMissingDates = true;
      }

      return pivotConfig;
    }
  }, {
    key: "timeSeries",
    value: function timeSeries(timeDimension) {
      if (!timeDimension.granularity) {
        return null;
      }

      var dateRange = timeDimension.dateRange;

      if (!dateRange) {
        var dates = pipe(map(function (row) {
          return row[timeDimension.dimension] && moment(row[timeDimension.dimension]);
        }), filter(function (r) {
          return !!r;
        }))(this.loadResponse.data);
        dateRange = dates.length && [reduce(minBy(function (d) {
          return d.toDate();
        }), dates[0], dates), reduce(maxBy(function (d) {
          return d.toDate();
        }), dates[0], dates)] || null;
      }

      if (!dateRange) {
        return null;
      }

      var start = moment(dateRange[0]).format('YYYY-MM-DD 00:00:00');
      var end = moment(dateRange[1]).format('YYYY-MM-DD 23:59:59');
      var range = moment.range(start, end);

      if (!TIME_SERIES[timeDimension.granularity]) {
        throw new Error("Unsupported time granularity: ".concat(timeDimension.granularity));
      }

      return TIME_SERIES[timeDimension.granularity](range);
    }
  }, {
    key: "pivot",
    value: function pivot(pivotConfig) {
      var _this2 = this;

      pivotConfig = this.normalizePivotConfig(pivotConfig);
      var groupByXAxis = groupBy(function (_ref3) {
        var xValues = _ref3.xValues;
        return _this2.axisValuesString(xValues);
      }); // eslint-disable-next-line no-unused-vars

      var measureValue = function measureValue(row, measure, xValues) {
        return row[measure];
      };

      if (pivotConfig.fillMissingDates && pivotConfig.x.length === 1 && equals(pivotConfig.x, (this.loadResponse.query.timeDimensions || []).filter(function (td) {
        return !!td.granularity;
      }).map(function (td) {
        return td.dimension;
      }))) {
        var series = this.timeSeries(this.loadResponse.query.timeDimensions[0]);

        if (series) {
          groupByXAxis = function groupByXAxis(rows) {
            var byXValues = groupBy(function (_ref4) {
              var xValues = _ref4.xValues;
              return moment(xValues[0]).format(moment.HTML5_FMT.DATETIME_LOCAL_MS);
            }, rows);
            return series.map(function (d) {
              return _defineProperty({}, d, byXValues[d] || [{
                xValues: [d],
                row: {}
              }]);
            }).reduce(function (a, b) {
              return Object.assign(a, b);
            }, {});
          }; // eslint-disable-next-line no-unused-vars


          measureValue = function measureValue(row, measure, xValues) {
            return row[measure] || 0;
          };
        }
      }

      var xGrouped = pipe(map(function (row) {
        return _this2.axisValues(pivotConfig.x)(row).map(function (xValues) {
          return {
            xValues: xValues,
            row: row
          };
        });
      }), unnest, groupByXAxis, toPairs)(this.loadResponse.data);
      var allYValues = pipe(map( // eslint-disable-next-line no-unused-vars
      function (_ref6) {
        var _ref7 = _slicedToArray(_ref6, 2),
            xValuesString = _ref7[0],
            rows = _ref7[1];

        return unnest( // collect Y values only from filled rows
        rows.filter(function (_ref8) {
          var row = _ref8.row;
          return Object.keys(row).length > 0;
        }).map(function (_ref9) {
          var row = _ref9.row;
          return _this2.axisValues(pivotConfig.y)(row);
        }));
      }), unnest, uniq)(xGrouped); // eslint-disable-next-line no-unused-vars

      return xGrouped.map(function (_ref10) {
        var _ref11 = _slicedToArray(_ref10, 2),
            xValuesString = _ref11[0],
            rows = _ref11[1];

        var xValues = rows[0].xValues;
        var yGrouped = pipe(map(function (_ref12) {
          var row = _ref12.row;
          return _this2.axisValues(pivotConfig.y)(row).map(function (yValues) {
            return {
              yValues: yValues,
              row: row
            };
          });
        }), unnest, groupBy(function (_ref13) {
          var yValues = _ref13.yValues;
          return _this2.axisValuesString(yValues);
        }))(rows);
        return {
          xValues: xValues,
          yValuesArray: unnest(allYValues.map(function (yValues) {
            var measure = pivotConfig.x.find(function (d) {
              return d === 'measures';
            }) ? ResultSet.measureFromAxis(xValues) : ResultSet.measureFromAxis(yValues);
            return (yGrouped[_this2.axisValuesString(yValues)] || [{
              row: {}
            }]).map(function (_ref14) {
              var row = _ref14.row;
              return [yValues, measureValue(row, measure, xValues)];
            });
          }))
        };
      });
    }
  }, {
    key: "pivotedRows",
    value: function pivotedRows(pivotConfig) {
      // TODO
      return this.chartPivot(pivotConfig);
    }
  }, {
    key: "chartPivot",
    value: function chartPivot(pivotConfig) {
      var _this3 = this;

      return this.pivot(pivotConfig).map(function (_ref15) {
        var xValues = _ref15.xValues,
            yValuesArray = _ref15.yValuesArray;
        return _objectSpread({
          category: _this3.axisValuesString(xValues, ', '),
          // TODO deprecated
          x: _this3.axisValuesString(xValues, ', ')
        }, yValuesArray.map(function (_ref16) {
          var _ref17 = _slicedToArray(_ref16, 2),
              yValues = _ref17[0],
              m = _ref17[1];

          return _defineProperty({}, _this3.axisValuesString(yValues, ', '), m && Number.parseFloat(m));
        }).reduce(function (a, b) {
          return Object.assign(a, b);
        }, {}));
      });
    }
  }, {
    key: "tablePivot",
    value: function tablePivot(pivotConfig) {
      var normalizedPivotConfig = this.normalizePivotConfig(pivotConfig);

      var valueToObject = function valueToObject(valuesArray, measureValue) {
        return function (field, index) {
          return _defineProperty({}, field === 'measures' ? valuesArray[index] : field, field === 'measures' ? measureValue : valuesArray[index]);
        };
      };

      return this.pivot(normalizedPivotConfig).map(function (_ref20) {
        var xValues = _ref20.xValues,
            yValuesArray = _ref20.yValuesArray;
        return yValuesArray.map(function (_ref21) {
          var _ref22 = _slicedToArray(_ref21, 2),
              yValues = _ref22[0],
              m = _ref22[1];

          return normalizedPivotConfig.x.map(valueToObject(xValues, m)).concat(normalizedPivotConfig.y.map(valueToObject(yValues, m))).reduce(function (a, b) {
            return Object.assign(a, b);
          }, {});
        }).reduce(function (a, b) {
          return Object.assign(a, b);
        }, {});
      });
    }
  }, {
    key: "tableColumns",
    value: function tableColumns(pivotConfig) {
      var _this4 = this;

      var normalizedPivotConfig = this.normalizePivotConfig(pivotConfig);

      var column = function column(field) {
        return field === 'measures' ? (_this4.query().measures || []).map(function (m) {
          return {
            key: m,
            title: _this4.loadResponse.annotation.measures[m].title
          };
        }) : [{
          key: field,
          title: (_this4.loadResponse.annotation.dimensions[field] || _this4.loadResponse.annotation.timeDimensions[field]).title
        }];
      };

      return normalizedPivotConfig.x.map(column).concat(normalizedPivotConfig.y.map(column)).reduce(function (a, b) {
        return a.concat(b);
      });
    }
  }, {
    key: "totalRow",
    value: function totalRow() {
      return this.chartPivot()[0];
    }
  }, {
    key: "categories",
    value: function categories(pivotConfig) {
      // TODO
      return this.chartPivot(pivotConfig);
    }
  }, {
    key: "seriesNames",
    value: function seriesNames(pivotConfig) {
      var _this5 = this;

      pivotConfig = this.normalizePivotConfig(pivotConfig);
      return pipe(map(this.axisValues(pivotConfig.y)), unnest, uniq)(this.loadResponse.data).map(function (axisValues) {
        return {
          title: _this5.axisValuesString(pivotConfig.y.find(function (d) {
            return d === 'measures';
          }) ? dropLast(1, axisValues).concat(_this5.loadResponse.annotation.measures[ResultSet.measureFromAxis(axisValues)].title) : axisValues, ', '),
          key: _this5.axisValuesString(axisValues)
        };
      });
    }
  }, {
    key: "query",
    value: function query() {
      return this.loadResponse.query;
    }
  }, {
    key: "rawData",
    value: function rawData() {
      return this.loadResponse.data;
    }
  }], [{
    key: "measureFromAxis",
    value: function measureFromAxis(axisValues) {
      return axisValues[axisValues.length - 1];
    }
  }]);

  return ResultSet;
}();

var SqlQuery =
/*#__PURE__*/
function () {
  function SqlQuery(sqlQuery) {
    _classCallCheck(this, SqlQuery);

    this.sqlQuery = sqlQuery;
  }

  _createClass(SqlQuery, [{
    key: "rawQuery",
    value: function rawQuery() {
      return this.sqlQuery.sql;
    }
  }, {
    key: "sql",
    value: function sql() {
      return this.rawQuery().sql[0];
    }
  }]);

  return SqlQuery;
}();

var memberMap = function memberMap(memberArray) {
  return fromPairs(memberArray.map(function (m) {
    return [m.name, m];
  }));
};

var operators = {
  string: [{
    name: 'contains',
    title: 'contains'
  }, {
    name: 'notContains',
    title: 'does not contain'
  }, {
    name: 'equals',
    title: 'equals'
  }, {
    name: 'notEquals',
    title: 'does not equal'
  }, {
    name: 'set',
    title: 'is set'
  }, {
    name: 'notSet',
    title: 'is not set'
  }],
  number: [{
    name: 'equals',
    title: 'equals'
  }, {
    name: 'notEquals',
    title: 'does not equal'
  }, {
    name: 'set',
    title: 'is set'
  }, {
    name: 'notSet',
    title: 'is not set'
  }, {
    name: 'gt',
    title: '>'
  }, {
    name: 'gte',
    title: '>='
  }, {
    name: 'lt',
    title: '<'
  }, {
    name: 'lte',
    title: '<='
  }]
};

var Meta =
/*#__PURE__*/
function () {
  function Meta(metaResponse) {
    _classCallCheck(this, Meta);

    this.meta = metaResponse;
    var cubes = this.meta.cubes;
    this.cubes = cubes;
    this.cubesMap = fromPairs(cubes.map(function (c) {
      return [c.name, {
        measures: memberMap(c.measures),
        dimensions: memberMap(c.dimensions),
        segments: memberMap(c.segments)
      }];
    }));
  }

  _createClass(Meta, [{
    key: "membersForQuery",
    value: function membersForQuery(query, memberType) {
      return unnest(this.cubes.map(function (c) {
        return c[memberType];
      }));
    }
  }, {
    key: "resolveMember",
    value: function resolveMember(memberName, memberType) {
      var _this = this;

      var _memberName$split = memberName.split('.'),
          _memberName$split2 = _slicedToArray(_memberName$split, 1),
          cube = _memberName$split2[0];

      if (!this.cubesMap[cube]) {
        return {
          title: memberName,
          error: "Cube not found ".concat(cube, " for path '").concat(memberName, "'")
        };
      }

      var memberTypes = Array.isArray(memberType) ? memberType : [memberType];
      var member = memberTypes.map(function (type) {
        return _this.cubesMap[cube][type] && _this.cubesMap[cube][type][memberName];
      }).find(function (m) {
        return m;
      });

      if (!member) {
        return {
          title: memberName,
          error: "Path not found '".concat(memberName, "'")
        };
      }

      return member;
    }
  }, {
    key: "defaultTimeDimensionNameFor",
    value: function defaultTimeDimensionNameFor(memberName) {
      var _this2 = this;

      var _memberName$split3 = memberName.split('.'),
          _memberName$split4 = _slicedToArray(_memberName$split3, 1),
          cube = _memberName$split4[0];

      if (!this.cubesMap[cube]) {
        return null;
      }

      return Object.keys(this.cubesMap[cube].dimensions || {}).find(function (d) {
        return _this2.cubesMap[cube].dimensions[d].type === 'time';
      });
    }
  }, {
    key: "filterOperatorsForMember",
    value: function filterOperatorsForMember(memberName, memberType) {
      var member = this.resolveMember(memberName, memberType);
      return operators[member.type] || operators.string;
    }
  }]);

  return Meta;
}();

var ProgressResult =
/*#__PURE__*/
function () {
  function ProgressResult(progressResponse) {
    _classCallCheck(this, ProgressResult);

    this.progressResponse = progressResponse;
  }

  _createClass(ProgressResult, [{
    key: "stage",
    value: function stage() {
      return this.progressResponse.stage;
    }
  }, {
    key: "timeElapsed",
    value: function timeElapsed() {
      return this.progressResponse.timeElapsed;
    }
  }]);

  return ProgressResult;
}();

var API_URL = "https://statsbot.co/cubejs-api/v1";
var mutexCounter = 0;
var MUTEX_ERROR = 'Mutex has been changed';

var mutexPromise = function mutexPromise(promise) {
  return new Promise(function (resolve, reject) {
    promise.then(function (r) {
      return resolve(r);
    }, function (e) {
      return e !== MUTEX_ERROR && reject(e);
    });
  });
};

var CubejsApi =
/*#__PURE__*/
function () {
  function CubejsApi(apiToken, options) {
    _classCallCheck(this, CubejsApi);

    options = options || {};
    this.apiToken = apiToken;
    this.apiUrl = options.apiUrl || API_URL;
  }

  _createClass(CubejsApi, [{
    key: "request",
    value: function request(url, config) {
      return fetch("".concat(this.apiUrl).concat(url), Object.assign({
        headers: {
          Authorization: this.apiToken,
          'Content-Type': 'application/json'
        }
      }, config || {}));
    }
  }, {
    key: "loadMethod",
    value: function loadMethod(request, toResult, options, callback) {
      var mutexValue = ++mutexCounter;

      if (typeof options === 'function' && !callback) {
        callback = options;
        options = undefined;
      }

      options = options || {};
      var mutexKey = options.mutexKey || 'default';

      if (options.mutexObj) {
        options.mutexObj[mutexKey] = mutexValue;
      }

      var checkMutex = function checkMutex() {
        if (options.mutexObj && options.mutexObj[mutexKey] !== mutexValue) {
          throw MUTEX_ERROR;
        }
      };

      var loadImpl =
      /*#__PURE__*/
      function () {
        var _ref = _asyncToGenerator(
        /*#__PURE__*/
        _regeneratorRuntime.mark(function _callee() {
          var response, body;
          return _regeneratorRuntime.wrap(function _callee$(_context) {
            while (1) {
              switch (_context.prev = _context.next) {
                case 0:
                  _context.next = 2;
                  return request();

                case 2:
                  response = _context.sent;

                  if (!(response.status === 502)) {
                    _context.next = 6;
                    break;
                  }

                  checkMutex();
                  return _context.abrupt("return", loadImpl());

                case 6:
                  _context.next = 8;
                  return response.json();

                case 8:
                  body = _context.sent;

                  if (!(body.error === 'Continue wait')) {
                    _context.next = 13;
                    break;
                  }

                  checkMutex();

                  if (options.progressCallback) {
                    options.progressCallback(new ProgressResult(body));
                  }

                  return _context.abrupt("return", loadImpl());

                case 13:
                  if (!(response.status !== 200)) {
                    _context.next = 16;
                    break;
                  }

                  checkMutex();
                  throw new Error(body.error);

                case 16:
                  checkMutex();
                  return _context.abrupt("return", toResult(body));

                case 18:
                case "end":
                  return _context.stop();
              }
            }
          }, _callee, this);
        }));

        return function loadImpl() {
          return _ref.apply(this, arguments);
        };
      }();

      if (callback) {
        mutexPromise(loadImpl()).then(function (r) {
          return callback(null, r);
        }, function (e) {
          return callback(e);
        });
      } else {
        return mutexPromise(loadImpl());
      }
    }
  }, {
    key: "load",
    value: function load(query, options, callback) {
      var _this = this;

      return this.loadMethod(function () {
        return _this.request("/load?query=".concat(encodeURIComponent(JSON.stringify(query))));
      }, function (body) {
        return new ResultSet(body);
      }, options, callback);
    }
  }, {
    key: "sql",
    value: function sql(query, options, callback) {
      var _this2 = this;

      return this.loadMethod(function () {
        return _this2.request("/sql?query=".concat(JSON.stringify(query)));
      }, function (body) {
        return new SqlQuery(body);
      }, options, callback);
    }
  }, {
    key: "meta",
    value: function meta(options, callback) {
      var _this3 = this;

      return this.loadMethod(function () {
        return _this3.request("/meta");
      }, function (body) {
        return new Meta(body);
      }, options, callback);
    }
  }]);

  return CubejsApi;
}();

var index = (function (apiToken, options) {
  return new CubejsApi(apiToken, options);
});

export default index;
