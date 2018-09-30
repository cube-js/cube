import 'core-js/modules/es6.array.find';
import 'core-js/modules/es6.array.map';

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

var ChartjsResultSet =
/*#__PURE__*/
function () {
  function ChartjsResultSet(resultSet, userConfig) {
    _classCallCheck(this, ChartjsResultSet);

    this.resultSet = resultSet;
    this.userConfig = userConfig;
  }

  _createClass(ChartjsResultSet, [{
    key: "timeSeries",
    value: function timeSeries() {
      return _objectSpread({
        type: 'line',
        data: {
          datasets: this.resultSet.series().map(function (s) {
            return {
              label: s.title,
              data: s.series.map(function (r) {
                return {
                  t: r.category,
                  y: r.value
                };
              })
            };
          })
        },
        options: {
          scales: {
            xAxes: [{
              type: 'time',
              unit: this.resultSet.query().timeDimensions[0].granularity,
              distribution: 'series',
              bounds: 'data'
            }]
          }
        }
      }, this.userConfig);
    }
  }, {
    key: "categories",
    value: function categories() {
      return _objectSpread({
        type: 'bar',
        data: {
          labels: this.resultSet.categories().map(function (c) {
            return c.category;
          }),
          datasets: this.resultSet.series().map(function (s) {
            return {
              label: s.title,
              data: s.series.map(function (r) {
                return r.value;
              })
            };
          })
        }
      }, this.userConfig);
    }
  }, {
    key: "prepareConfig",
    value: function prepareConfig() {
      if ((this.resultSet.query().timeDimensions || []).find(function (td) {
        return !!td.granularity;
      })) {
        return this.timeSeries();
      } else {
        return this.categories();
      }
    }
  }]);

  return ChartjsResultSet;
}();

var index = (function (resultSet, userConfig) {
  return new ChartjsResultSet(resultSet, userConfig).prepareConfig();
});

export default index;
