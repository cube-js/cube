import 'core-js/modules/es6.number.constructor';
import 'core-js/modules/es6.number.parse-float';
import 'core-js/modules/es6.object.assign';
import _defineProperty from '@babel/runtime/helpers/defineProperty';
import 'core-js/modules/es6.array.reduce';
import _objectSpread from '@babel/runtime/helpers/objectSpread';
import _slicedToArray from '@babel/runtime/helpers/slicedToArray';
import 'core-js/modules/es6.array.find';
import 'core-js/modules/es6.array.filter';
import _objectWithoutProperties from '@babel/runtime/helpers/objectWithoutProperties';
import 'core-js/modules/es6.array.map';
import _classCallCheck from '@babel/runtime/helpers/classCallCheck';
import _createClass from '@babel/runtime/helpers/createClass';
import { groupBy, pipe, toPairs, uniq, map, unnest, dropLast } from 'ramda';
import _regeneratorRuntime from '@babel/runtime/regenerator';
import 'regenerator-runtime/runtime';
import _asyncToGenerator from '@babel/runtime/helpers/asyncToGenerator';
import { fetch } from 'whatwg-fetch';

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
          series: _this.pivotedRows(pivotConfig).map(function (_ref2) {
            var category = _ref2.category,
                obj = _objectWithoutProperties(_ref2, ["category"]);

            return {
              value: obj[key],
              category: category
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
            return row[d];
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
      return axisValues.map(function (v) {
        return v != null ? v : '∅';
      }).join(delimiter || ':');
    }
  }, {
    key: "normalizePivotConfig",
    value: function normalizePivotConfig(pivotConfig) {
      var query = this.loadResponse.query;
      var timeDimensions = (query.timeDimensions || []).filter(function (td) {
        return !!td.granularity;
      });
      pivotConfig = pivotConfig || timeDimensions.length ? {
        x: timeDimensions.map(function (td) {
          return td.dimension;
        }),
        y: query.dimensions || []
      } : {
        x: query.dimensions || [],
        y: []
      };

      if (!pivotConfig.x.concat(pivotConfig.y).find(function (d) {
        return d === 'measures';
      })) {
        pivotConfig.y = pivotConfig.y.concat(['measures']);
      }

      return pivotConfig;
    }
  }, {
    key: "pivot",
    value: function pivot(pivotConfig) {
      var _this2 = this;

      // TODO missing date filling
      pivotConfig = this.normalizePivotConfig(pivotConfig);
      return pipe(map(function (row) {
        return _this2.axisValues(pivotConfig.x)(row).map(function (xValues) {
          return {
            xValues: xValues,
            row: row
          };
        });
      }), unnest, groupBy(function (_ref3) {
        var xValues = _ref3.xValues;
        return _this2.axisValuesString(xValues);
      }), toPairs)(this.loadResponse.data).map(function (_ref4) {
        var _ref5 = _slicedToArray(_ref4, 2),
            xValuesString = _ref5[0],
            rows = _ref5[1];

        var xValues = rows[0].xValues;
        return _objectSpread({
          xValues: xValues
        }, rows.map(function (r) {
          return r.row;
        }).map(function (row) {
          return _this2.axisValues(pivotConfig.y)(row).map(function (yValues) {
            var measure = pivotConfig.x.find(function (d) {
              return d === 'measures';
            }) ? ResultSet.measureFromAxis(xValues) : ResultSet.measureFromAxis(yValues);
            return _defineProperty({}, _this2.axisValuesString(yValues), row[measure]);
          }).reduce(function (a, b) {
            return Object.assign(a, b);
          }, {});
        }).reduce(function (a, b) {
          return Object.assign(a, b);
        }, {}));
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

      return this.pivot(pivotConfig).map(function (_ref7) {
        var xValues = _ref7.xValues,
            measures = _objectWithoutProperties(_ref7, ["xValues"]);

        return _objectSpread({
          category: _this3.axisValuesString(xValues, ', ')
        }, map(function (m) {
          return m && Number.parseFloat(m);
        }, measures));
      });
    }
  }, {
    key: "totalRow",
    value: function totalRow() {
      return this.pivotedRows()[0];
    }
  }, {
    key: "categories",
    value: function categories(pivotConfig) {
      //TODO
      return this.pivotedRows(pivotConfig);
    }
  }, {
    key: "seriesNames",
    value: function seriesNames(pivotConfig) {
      var _this4 = this;

      pivotConfig = this.normalizePivotConfig(pivotConfig);
      return pipe(map(this.axisValues(pivotConfig.y)), unnest, uniq)(this.loadResponse.data).map(function (axisValues) {
        return {
          title: _this4.axisValuesString(pivotConfig.y.find(function (d) {
            return d === 'measures';
          }) ? dropLast(1, axisValues).concat(_this4.loadResponse.annotation.measures[ResultSet.measureFromAxis(axisValues)].title) : axisValues, ', '),
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
  function CubejsApi(apiToken) {
    _classCallCheck(this, CubejsApi);

    this.apiToken = apiToken;
  }

  _createClass(CubejsApi, [{
    key: "request",
    value: function request(url, config) {
      return fetch("".concat(API_URL).concat(url), Object.assign({
        headers: {
          Authorization: this.apiToken,
          'Content-Type': 'application/json'
        }
      }, config || {}));
    }
  }, {
    key: "load",
    value: function load(query, options, callback) {
      var _this = this;

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
                  return _this.request("/load?query=".concat(JSON.stringify(query)));

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
                  return _context.abrupt("return", new ResultSet(body));

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
  }]);

  return CubejsApi;
}();

var index = (function (apiToken) {
  return new CubejsApi(apiToken);
});

export default index;
