'use strict';

function _interopDefault (ex) { return (ex && (typeof ex === 'object') && 'default' in ex) ? ex['default'] : ex; }

require('core-js/modules/es6.number.constructor');
require('core-js/modules/es6.number.parse-float');
var _objectSpread = _interopDefault(require('@babel/runtime/helpers/objectSpread'));
var _slicedToArray = _interopDefault(require('@babel/runtime/helpers/slicedToArray'));
require('core-js/modules/es6.object.assign');
var _defineProperty = _interopDefault(require('@babel/runtime/helpers/defineProperty'));
require('core-js/modules/es6.array.reduce');
require('core-js/modules/es6.array.find');
require('core-js/modules/es6.array.filter');
var _objectWithoutProperties = _interopDefault(require('@babel/runtime/helpers/objectWithoutProperties'));
var _classCallCheck = _interopDefault(require('@babel/runtime/helpers/classCallCheck'));
var _createClass = _interopDefault(require('@babel/runtime/helpers/createClass'));
require('core-js/modules/es6.string.iterator');
require('core-js/modules/es6.array.from');
require('core-js/modules/es6.array.map');
var ramda = require('ramda');
var Moment = require('moment');
var momentRange = require('moment-range');
var _regeneratorRuntime = _interopDefault(require('@babel/runtime/regenerator'));
require('regenerator-runtime/runtime');
var _asyncToGenerator = _interopDefault(require('@babel/runtime/helpers/asyncToGenerator'));
var whatwgFetch = require('whatwg-fetch');

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

      return axisValues.map(formatValue).join(delimiter || ':');
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
        var dates = ramda.pipe(ramda.map(function (row) {
          return row[timeDimension.dimension] && moment(row[timeDimension.dimension]);
        }), ramda.filter(function (r) {
          return !!r;
        }))(this.loadResponse.data);
        dateRange = dates.length && [ramda.reduce(ramda.minBy(function (d) {
          return d.toDate();
        }), dates[0], dates), ramda.reduce(ramda.maxBy(function (d) {
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
      var groupByXAxis = ramda.groupBy(function (_ref3) {
        var xValues = _ref3.xValues;
        return _this2.axisValuesString(xValues);
      });

      var measureValue = function measureValue(row, measure, xValues) {
        return row[measure];
      };

      if (pivotConfig.fillMissingDates && pivotConfig.x.length === 1 && ramda.equals(pivotConfig.x, (this.loadResponse.query.timeDimensions || []).filter(function (td) {
        return !!td.granularity;
      }).map(function (td) {
        return td.dimension;
      }))) {
        var series = this.timeSeries(this.loadResponse.query.timeDimensions[0]);

        if (series) {
          groupByXAxis = function groupByXAxis(rows) {
            var byXValues = ramda.groupBy(function (_ref4) {
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
          };

          measureValue = function measureValue(row, measure, xValues) {
            return row[measure] || 0;
          };
        }
      }

      var xGrouped = ramda.pipe(ramda.map(function (row) {
        return _this2.axisValues(pivotConfig.x)(row).map(function (xValues) {
          return {
            xValues: xValues,
            row: row
          };
        });
      }), ramda.unnest, groupByXAxis, ramda.toPairs)(this.loadResponse.data);
      var allYValues = ramda.pipe(ramda.map(function (_ref6) {
        var _ref7 = _slicedToArray(_ref6, 2),
            xValuesString = _ref7[0],
            rows = _ref7[1];

        return ramda.unnest(rows.map(function (_ref8) {
          var row = _ref8.row;
          return _this2.axisValues(pivotConfig.y)(row);
        }));
      }), ramda.unnest, ramda.uniq)(xGrouped);
      return xGrouped.map(function (_ref9) {
        var _ref10 = _slicedToArray(_ref9, 2),
            xValuesString = _ref10[0],
            rows = _ref10[1];

        var xValues = rows[0].xValues;
        var yGrouped = ramda.pipe(ramda.map(function (_ref11) {
          var row = _ref11.row;
          return _this2.axisValues(pivotConfig.y)(row).map(function (yValues) {
            return {
              yValues: yValues,
              row: row
            };
          });
        }), ramda.unnest, ramda.groupBy(function (_ref12) {
          var yValues = _ref12.yValues;
          return _this2.axisValuesString(yValues);
        }))(rows);
        return {
          xValues: xValues,
          yValuesArray: ramda.unnest(allYValues.map(function (yValues) {
            var measure = pivotConfig.x.find(function (d) {
              return d === 'measures';
            }) ? ResultSet.measureFromAxis(xValues) : ResultSet.measureFromAxis(yValues);
            return (yGrouped[_this2.axisValuesString(yValues)] || [{
              row: {}
            }]).map(function (_ref13) {
              var row = _ref13.row;
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

      return this.pivot(pivotConfig).map(function (_ref14) {
        var xValues = _ref14.xValues,
            yValuesArray = _ref14.yValuesArray;
        return _objectSpread({
          category: _this3.axisValuesString(xValues, ', '),
          //TODO deprecated
          x: _this3.axisValuesString(xValues, ', ')
        }, yValuesArray.map(function (_ref15) {
          var _ref16 = _slicedToArray(_ref15, 2),
              yValues = _ref16[0],
              m = _ref16[1];

          return _defineProperty({}, _this3.axisValuesString(yValues, ', '), m && Number.parseFloat(m));
        }).reduce(function (a, b) {
          return Object.assign(a, b);
        }, {}));
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
      //TODO
      return this.chartPivot(pivotConfig);
    }
  }, {
    key: "seriesNames",
    value: function seriesNames(pivotConfig) {
      var _this4 = this;

      pivotConfig = this.normalizePivotConfig(pivotConfig);
      return ramda.pipe(ramda.map(this.axisValues(pivotConfig.y)), ramda.unnest, ramda.uniq)(this.loadResponse.data).map(function (axisValues) {
        return {
          title: _this4.axisValuesString(pivotConfig.y.find(function (d) {
            return d === 'measures';
          }) ? ramda.dropLast(1, axisValues).concat(_this4.loadResponse.annotation.measures[ResultSet.measureFromAxis(axisValues)].title) : axisValues, ', '),
          key: _this4.axisValuesString(axisValues)
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
      return whatwgFetch.fetch("".concat(this.apiUrl).concat(url), Object.assign({
        headers: {
          Authorization: this.apiToken,
          'Content-Type': 'application/json'
        }
      }, config || {}));
    }
  }, {
    key: "loadMethod",
    value: function loadMethod(request, toResult, options, callback) {
      if (typeof options === 'function' && !callback) {
        callback = options;
        options = undefined;
      }

      options = options || {};

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
                    _context.next = 5;
                    break;
                  }

                  return _context.abrupt("return", loadImpl());

                case 5:
                  _context.next = 7;
                  return response.json();

                case 7:
                  body = _context.sent;

                  if (!(body.error === 'Continue wait')) {
                    _context.next = 11;
                    break;
                  }

                  if (options.progressCallback) {
                    options.progressCallback(new ProgressResult(body));
                  }

                  return _context.abrupt("return", loadImpl());

                case 11:
                  if (!(response.status !== 200)) {
                    _context.next = 13;
                    break;
                  }

                  throw new Error(body.error);

                case 13:
                  return _context.abrupt("return", toResult(body));

                case 14:
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
        loadImpl().then(function (r) {
          return callback(null, r);
        }, function (e) {
          return callback(e);
        });
      } else {
        return loadImpl();
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
  }]);

  return CubejsApi;
}();

var index = (function (apiToken, options) {
  return new CubejsApi(apiToken, options);
});

module.exports = index;
