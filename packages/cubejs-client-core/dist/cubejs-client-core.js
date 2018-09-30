import 'core-js/modules/es6.regexp.split';
import 'core-js/modules/es6.regexp.replace';
import 'core-js/modules/es6.object.assign';
import 'core-js/modules/es6.number.constructor';
import 'core-js/modules/es6.number.parse-float';
import 'core-js/modules/es6.array.reduce';
import 'core-js/modules/es6.array.find';
import 'core-js/modules/es6.array.filter';
import 'core-js/modules/es6.array.map';
import { groupBy, pipe, toPairs, uniq, flatten, map } from 'ramda';
import 'regenerator-runtime/runtime';
import { fetch } from 'whatwg-fetch';

function asyncGeneratorStep(gen, resolve, reject, _next, _throw, key, arg) {
  try {
    var info = gen[key](arg);
    var value = info.value;
  } catch (error) {
    reject(error);
    return;
  }

  if (info.done) {
    resolve(value);
  } else {
    Promise.resolve(value).then(_next, _throw);
  }
}

function _asyncToGenerator(fn) {
  return function () {
    var self = this,
        args = arguments;
    return new Promise(function (resolve, reject) {
      var gen = fn.apply(self, args);

      function _next(value) {
        asyncGeneratorStep(gen, resolve, reject, _next, _throw, "next", value);
      }

      function _throw(err) {
        asyncGeneratorStep(gen, resolve, reject, _next, _throw, "throw", err);
      }

      _next(undefined);
    });
  };
}

function _classCallCheck(instance, Constructor) {
  if (!(instance instanceof Constructor)) {
    throw new TypeError("Cannot call a class as a function");
  }
}

function _defineProperties(target, props) {
  for (var i = 0; i < props.length; i++) {
    var descriptor = props[i];
    descriptor.enumerable = descriptor.enumerable || false;
    descriptor.configurable = true;
    if ("value" in descriptor) descriptor.writable = true;
    Object.defineProperty(target, descriptor.key, descriptor);
  }
}

function _createClass(Constructor, protoProps, staticProps) {
  if (protoProps) _defineProperties(Constructor.prototype, protoProps);
  if (staticProps) _defineProperties(Constructor, staticProps);
  return Constructor;
}

function _defineProperty(obj, key, value) {
  if (key in obj) {
    Object.defineProperty(obj, key, {
      value: value,
      enumerable: true,
      configurable: true,
      writable: true
    });
  } else {
    obj[key] = value;
  }

  return obj;
}

function _objectSpread(target) {
  for (var i = 1; i < arguments.length; i++) {
    var source = arguments[i] != null ? arguments[i] : {};
    var ownKeys = Object.keys(source);

    if (typeof Object.getOwnPropertySymbols === 'function') {
      ownKeys = ownKeys.concat(Object.getOwnPropertySymbols(source).filter(function (sym) {
        return Object.getOwnPropertyDescriptor(source, sym).enumerable;
      }));
    }

    ownKeys.forEach(function (key) {
      _defineProperty(target, key, source[key]);
    });
  }

  return target;
}

function _objectWithoutPropertiesLoose(source, excluded) {
  if (source == null) return {};
  var target = {};
  var sourceKeys = Object.keys(source);
  var key, i;

  for (i = 0; i < sourceKeys.length; i++) {
    key = sourceKeys[i];
    if (excluded.indexOf(key) >= 0) continue;
    target[key] = source[key];
  }

  return target;
}

function _objectWithoutProperties(source, excluded) {
  if (source == null) return {};

  var target = _objectWithoutPropertiesLoose(source, excluded);

  var key, i;

  if (Object.getOwnPropertySymbols) {
    var sourceSymbolKeys = Object.getOwnPropertySymbols(source);

    for (i = 0; i < sourceSymbolKeys.length; i++) {
      key = sourceSymbolKeys[i];
      if (excluded.indexOf(key) >= 0) continue;
      if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue;
      target[key] = source[key];
    }
  }

  return target;
}

function _slicedToArray(arr, i) {
  return _arrayWithHoles(arr) || _iterableToArrayLimit(arr, i) || _nonIterableRest();
}

function _arrayWithHoles(arr) {
  if (Array.isArray(arr)) return arr;
}

function _iterableToArrayLimit(arr, i) {
  var _arr = [];
  var _n = true;
  var _d = false;
  var _e = undefined;

  try {
    for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) {
      _arr.push(_s.value);

      if (i && _arr.length === i) break;
    }
  } catch (err) {
    _d = true;
    _e = err;
  } finally {
    try {
      if (!_n && _i["return"] != null) _i["return"]();
    } finally {
      if (_d) throw _e;
    }
  }

  return _arr;
}

function _nonIterableRest() {
  throw new TypeError("Invalid attempt to destructure non-iterable instance");
}

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
          }).concat(measure ? [measure] : []).map(function (v) {
            return v != null ? v : '∅';
          }).join(', ');
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
    key: "pivotedRows",
    value: function pivotedRows(pivotConfig) {
      var _this2 = this;

      // TODO missing date filling
      pivotConfig = this.normalizePivotConfig(pivotConfig);
      return pipe(groupBy(this.axisValues(pivotConfig.x)), toPairs)(this.loadResponse.data).map(function (_ref3) {
        var _ref4 = _slicedToArray(_ref3, 2),
            category = _ref4[0],
            rows = _ref4[1];

        return _objectSpread({
          category: category
        }, rows.map(function (row) {
          return _this2.axisValues(pivotConfig.y)(row).map(function (series) {
            var measure = pivotConfig.x.find(function (d) {
              return d === 'measures';
            }) ? ResultSet.measureFromAxis(category) : ResultSet.measureFromAxis(series);
            return _defineProperty({}, series, row[measure] && Number.parseFloat(row[measure]));
          }).reduce(function (a, b) {
            return Object.assign(a, b);
          }, {});
        }).reduce(function (a, b) {
          return Object.assign(a, b);
        }, {}));
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
      var _this3 = this;

      pivotConfig = this.normalizePivotConfig(pivotConfig);
      return pipe(map(this.axisValues(pivotConfig.y)), uniq, flatten)(this.loadResponse.data).map(function (axis) {
        return {
          title: pivotConfig.y.find(function (d) {
            return d === 'measures';
          }) ? axis.replace(ResultSet.measureFromAxis(axis), _this3.loadResponse.annotation.measures[ResultSet.measureFromAxis(axis)].title) : axis,
          key: axis
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
    value: function measureFromAxis(axis) {
      var axisValues = axis.split(', ');
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
        regeneratorRuntime.mark(function _callee() {
          var response, body;
          return regeneratorRuntime.wrap(function _callee$(_context) {
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
