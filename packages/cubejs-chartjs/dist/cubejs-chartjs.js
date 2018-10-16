'use strict';

function _interopDefault (ex) { return (ex && (typeof ex === 'object') && 'default' in ex) ? ex['default'] : ex; }

require('core-js/modules/es6.array.find');
require('core-js/modules/es6.array.map');
var _objectSpread = _interopDefault(require('@babel/runtime/helpers/objectSpread'));
var _classCallCheck = _interopDefault(require('@babel/runtime/helpers/classCallCheck'));
var _createClass = _interopDefault(require('@babel/runtime/helpers/createClass'));

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

module.exports = index;
