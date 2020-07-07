import 'core-js/modules/es.object.to-string';
import 'core-js/modules/es.promise';
import 'core-js/modules/web.timers';
import _regeneratorRuntime from '@babel/runtime/regenerator';
import 'regenerator-runtime/runtime';
import _asyncToGenerator from '@babel/runtime/helpers/asyncToGenerator';
import _objectSpread2 from '@babel/runtime/helpers/objectSpread';
import _typeof from '@babel/runtime/helpers/typeof';
import _classCallCheck from '@babel/runtime/helpers/classCallCheck';
import _createClass from '@babel/runtime/helpers/createClass';
import uuid from 'uuid/v4';
import 'core-js/modules/es.array.concat';
import 'core-js/modules/es.array.filter';
import 'core-js/modules/es.array.find';
import 'core-js/modules/es.array.for-each';
import 'core-js/modules/es.array.from';
import 'core-js/modules/es.array.includes';
import 'core-js/modules/es.array.join';
import 'core-js/modules/es.array.map';
import 'core-js/modules/es.array.reduce';
import 'core-js/modules/es.date.to-string';
import 'core-js/modules/es.number.constructor';
import 'core-js/modules/es.number.is-nan';
import 'core-js/modules/es.number.parse-float';
import 'core-js/modules/es.object.assign';
import 'core-js/modules/es.object.keys';
import 'core-js/modules/es.object.values';
import 'core-js/modules/es.regexp.exec';
import 'core-js/modules/es.regexp.to-string';
import 'core-js/modules/es.string.includes';
import 'core-js/modules/es.string.iterator';
import 'core-js/modules/es.string.match';
import 'core-js/modules/es.string.split';
import 'core-js/modules/es.string.trim';
import 'core-js/modules/web.dom-collections.for-each';
import _toConsumableArray from '@babel/runtime/helpers/toConsumableArray';
import _defineProperty from '@babel/runtime/helpers/defineProperty';
import _objectWithoutProperties from '@babel/runtime/helpers/objectWithoutProperties';
import _slicedToArray from '@babel/runtime/helpers/slicedToArray';
import { pipe, map, filter, reduce, minBy, maxBy, groupBy, equals, unnest, toPairs, uniq, fromPairs, dropLast } from 'ramda';
import Moment from 'moment';
import momentRange from 'moment-range';
import 'core-js/modules/es.array.is-array';
import 'core-js/modules/es.function.name';
import 'core-js/modules/es.array.iterator';
import 'core-js/modules/web.dom-collections.iterator';
import 'core-js/modules/web.url';
import fetch from 'cross-fetch';
import 'url-search-params-polyfill';
import _possibleConstructorReturn from '@babel/runtime/helpers/possibleConstructorReturn';
import _getPrototypeOf from '@babel/runtime/helpers/getPrototypeOf';
import _inherits from '@babel/runtime/helpers/inherits';
import _wrapNativeSuper from '@babel/runtime/helpers/wrapNativeSuper';

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
  minute: function minute(range) {
    return Array.from(range.by('minute')).map(function (d) {
      return d.format('YYYY-MM-DDTHH:mm:00.000');
    });
  },
  second: function second(range) {
    return Array.from(range.by('second')).map(function (d) {
      return d.format('YYYY-MM-DDTHH:mm:ss.000');
    });
  },
  week: function week(range) {
    return Array.from(range.snapTo('isoweek').by('week')).map(function (d) {
      return d.startOf('isoweek').format('YYYY-MM-DDT00:00:00.000');
    });
  }
};
var DateRegex = /^\d\d\d\d-\d\d-\d\d$/;
var LocalDateRegex = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z?$/;

var ResultSet =
/*#__PURE__*/
function () {
  function ResultSet(loadResponse, options) {
    _classCallCheck(this, ResultSet);

    options = options || {};
    this.loadResponse = loadResponse;
    this.parseDateMeasures = options.parseDateMeasures;
  }

  _createClass(ResultSet, [{
    key: "drillDown",
    value: function drillDown(drillDownLocator, pivotConfig) {
      var _drillDownLocator$xVa = drillDownLocator.xValues,
          xValues = _drillDownLocator$xVa === void 0 ? [] : _drillDownLocator$xVa,
          _drillDownLocator$yVa = drillDownLocator.yValues,
          yValues = _drillDownLocator$yVa === void 0 ? [] : _drillDownLocator$yVa;
      var normalizedPivotConfig = this.normalizePivotConfig(pivotConfig);
      var values = [];
      normalizedPivotConfig.x.forEach(function (member, currentIndex) {
        return values.push([member, xValues[currentIndex]]);
      });
      normalizedPivotConfig.y.forEach(function (member, currentIndex) {
        return values.push([member, yValues[currentIndex]]);
      });
      var measures = this.loadResponse.annotation.measures;

      var _ref = values.find(function (_ref3) {
        var _ref4 = _slicedToArray(_ref3, 1),
            member = _ref4[0];

        return member === 'measues';
      }) || [],
          _ref2 = _slicedToArray(_ref, 2),
          measureName = _ref2[1];

      if (measureName === undefined) {
        var _Object$keys = Object.keys(measures);

        var _Object$keys2 = _slicedToArray(_Object$keys, 1);

        measureName = _Object$keys2[0];
      }

      if (!(measures[measureName] && measures[measureName].drillMembers || []).length) {
        return null;
      }

      var filters = [{
        dimension: measureName,
        operator: 'measureFilter'
      }];
      var timeDimensions = [];
      values.filter(function (_ref5) {
        var _ref6 = _slicedToArray(_ref5, 1),
            member = _ref6[0];

        return member !== 'measures';
      }).forEach(function (_ref7) {
        var _ref8 = _slicedToArray(_ref7, 2),
            member = _ref8[0],
            value = _ref8[1];

        var _member$split = member.split('.'),
            _member$split2 = _slicedToArray(_member$split, 3),
            cubeName = _member$split2[0],
            dimension = _member$split2[1],
            granularity = _member$split2[2];

        if (granularity !== undefined) {
          var range = moment.range(value, value).snapTo(granularity);
          timeDimensions.push({
            dimension: [cubeName, dimension].join('.'),
            dateRange: [range.start, range.end].map(function (dt) {
              return dt.format(moment.HTML5_FMT.DATETIME_LOCAL_MS);
            })
          });
        } else {
          filters.push({
            member: member,
            operator: 'equals',
            values: [value.toString()]
          });
        }
      });
      return _objectSpread2({}, measures[measureName].drillMembersGrouped, {
        filters: filters,
        timeDimensions: timeDimensions
      });
    }
  }, {
    key: "series",
    value: function series(pivotConfig) {
      var _this = this;

      return this.seriesNames(pivotConfig).map(function (_ref9) {
        var title = _ref9.title,
            key = _ref9.key;
        return {
          title: title,
          key: key,
          series: _this.chartPivot(pivotConfig).map(function (_ref10) {
            var category = _ref10.category,
                x = _ref10.x,
                obj = _objectWithoutProperties(_ref10, ["category", "x"]);

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
      return ResultSet.getNormalizedPivotConfig(query, pivotConfig);
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
          return row[ResultSet.timeDimensionMember(timeDimension)] && moment(row[ResultSet.timeDimensionMember(timeDimension)]);
        }), filter(function (r) {
          return !!r;
        }))(this.timeDimensionBackwardCompatibleData());
        dateRange = dates.length && [reduce(minBy(function (d) {
          return d.toDate();
        }), dates[0], dates), reduce(maxBy(function (d) {
          return d.toDate();
        }), dates[0], dates)] || null;
      }

      if (!dateRange) {
        return null;
      }

      var padToDay = timeDimension.dateRange ? timeDimension.dateRange.find(function (d) {
        return d.match(DateRegex);
      }) : !['hour', 'minute', 'second'].includes(timeDimension.granularity);

      var _dateRange = dateRange,
          _dateRange2 = _slicedToArray(_dateRange, 2),
          start = _dateRange2[0],
          end = _dateRange2[1];

      var range = moment.range(start, end);

      if (!TIME_SERIES[timeDimension.granularity]) {
        throw new Error("Unsupported time granularity: ".concat(timeDimension.granularity));
      }

      return TIME_SERIES[timeDimension.granularity](padToDay ? range.snapTo('day') : range);
    }
  }, {
    key: "pivot",
    value: function pivot(pivotConfig) {
      var _this2 = this;

      pivotConfig = this.normalizePivotConfig(pivotConfig);
      var groupByXAxis = groupBy(function (_ref11) {
        var xValues = _ref11.xValues;
        return _this2.axisValuesString(xValues);
      }); // eslint-disable-next-line no-unused-vars

      var measureValue = function measureValue(row, measure, xValues) {
        return row[measure];
      };

      if (pivotConfig.fillMissingDates && pivotConfig.x.length === 1 && equals(pivotConfig.x, (this.loadResponse.query.timeDimensions || []).filter(function (td) {
        return !!td.granularity;
      }).map(function (td) {
        return ResultSet.timeDimensionMember(td);
      }))) {
        var series = this.timeSeries(this.loadResponse.query.timeDimensions[0]);

        if (series) {
          groupByXAxis = function groupByXAxis(rows) {
            var byXValues = groupBy(function (_ref12) {
              var xValues = _ref12.xValues;
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
      }), unnest, groupByXAxis, toPairs)(this.timeDimensionBackwardCompatibleData());
      var allYValues = pipe(map( // eslint-disable-next-line no-unused-vars
      function (_ref14) {
        var _ref15 = _slicedToArray(_ref14, 2),
            rows = _ref15[1];

        return unnest( // collect Y values only from filled rows
        rows.filter(function (_ref16) {
          var row = _ref16.row;
          return Object.keys(row).length > 0;
        }).map(function (_ref17) {
          var row = _ref17.row;
          return _this2.axisValues(pivotConfig.y)(row);
        }));
      }), unnest, uniq)(xGrouped); // eslint-disable-next-line no-unused-vars

      return xGrouped.map(function (_ref18) {
        var _ref19 = _slicedToArray(_ref18, 2),
            xValuesString = _ref19[0],
            rows = _ref19[1];

        var xValues = rows[0].xValues;
        var yGrouped = pipe(map(function (_ref20) {
          var row = _ref20.row;
          return _this2.axisValues(pivotConfig.y)(row).map(function (yValues) {
            return {
              yValues: yValues,
              row: row
            };
          });
        }), unnest, groupBy(function (_ref21) {
          var yValues = _ref21.yValues;
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
            }]).map(function (_ref22) {
              var row = _ref22.row;
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

      var validate = function validate(value) {
        if (_this3.parseDateMeasures && LocalDateRegex.test(value)) {
          return new Date(value);
        } else if (!Number.isNaN(Number.parseFloat(value))) {
          return Number.parseFloat(value);
        }

        return value;
      };

      return this.pivot(pivotConfig).map(function (_ref23) {
        var xValues = _ref23.xValues,
            yValuesArray = _ref23.yValuesArray;
        return _objectSpread2({
          category: _this3.axisValuesString(xValues, ', '),
          // TODO deprecated
          x: _this3.axisValuesString(xValues, ', '),
          xValues: xValues
        }, yValuesArray.map(function (_ref24) {
          var _ref25 = _slicedToArray(_ref24, 2),
              yValues = _ref25[0],
              m = _ref25[1];

          return _defineProperty({}, _this3.axisValuesString(yValues, ', '), m && validate(m));
        }).reduce(function (a, b) {
          return Object.assign(a, b);
        }, {}));
      });
    }
  }, {
    key: "tablePivot",
    value: function tablePivot(pivotConfig) {
      var normalizedPivotConfig = this.normalizePivotConfig(pivotConfig || {});
      var isMeasuresPresent = normalizedPivotConfig.x.concat(normalizedPivotConfig.y).includes('measures');
      return this.pivot(normalizedPivotConfig).map(function (_ref27) {
        var xValues = _ref27.xValues,
            yValuesArray = _ref27.yValuesArray;
        return fromPairs(normalizedPivotConfig.x.map(function (key, index) {
          return [key, xValues[index]];
        }).concat(isMeasuresPresent ? yValuesArray.map(function (_ref28) {
          var _ref29 = _slicedToArray(_ref28, 2),
              yValues = _ref29[0],
              measure = _ref29[1];

          return [yValues.length ? yValues.join('.') : 'value', measure];
        }) : []));
      });
    }
  }, {
    key: "tableColumns",
    value: function tableColumns(pivotConfig) {
      var _this4 = this;

      var normalizedPivotConfig = this.normalizePivotConfig(pivotConfig);
      var schema = {};

      var extractFields = function extractFields(key) {
        var flatMeta = Object.values(_this4.loadResponse.annotation).reduce(function (a, b) {
          return _objectSpread2({}, a, {}, b);
        }, {});

        var _ref30 = flatMeta[key] || {},
            title = _ref30.title,
            shortTitle = _ref30.shortTitle,
            type = _ref30.type,
            format = _ref30.format,
            meta = _ref30.meta;

        return {
          key: key,
          title: title,
          shortTitle: shortTitle,
          type: type,
          format: format,
          meta: meta
        };
      };

      var pivot = this.pivot(normalizedPivotConfig);
      (pivot[0] && pivot[0].yValuesArray || []).forEach(function (_ref31) {
        var _ref32 = _slicedToArray(_ref31, 1),
            yValues = _ref32[0];

        if (yValues.length > 0) {
          var currentItem = schema;
          yValues.forEach(function (value, index) {
            currentItem[value] = {
              key: value,
              memberId: normalizedPivotConfig.y[index] === 'measures' ? value : normalizedPivotConfig.y[index],
              children: currentItem[value] && currentItem[value].children || {}
            };
            currentItem = currentItem[value].children;
          });
        }
      });

      var toColumns = function toColumns() {
        var item = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {};
        var path = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : [];

        if (Object.keys(item).length === 0) {
          return [];
        }

        return Object.values(item).map(function (_ref33) {
          var key = _ref33.key,
              currentItem = _objectWithoutProperties(_ref33, ["key"]);

          var children = toColumns(currentItem.children, [].concat(_toConsumableArray(path), [key]));

          var _extractFields = extractFields(currentItem.memberId),
              title = _extractFields.title,
              shortTitle = _extractFields.shortTitle,
              fields = _objectWithoutProperties(_extractFields, ["title", "shortTitle"]);

          var dimensionValue = key !== currentItem.memberId ? key : '';

          if (!children.length) {
            return _objectSpread2({}, fields, {
              key: key,
              dataIndex: [].concat(_toConsumableArray(path), [key]).join('.'),
              title: [title, dimensionValue].join(' ').trim(),
              shortTitle: dimensionValue || shortTitle
            });
          }

          return _objectSpread2({}, fields, {
            key: key,
            title: [title, dimensionValue].join(' ').trim(),
            shortTitle: dimensionValue || shortTitle,
            children: children
          });
        });
      };

      var otherColumns = [];

      if (!pivot.length && normalizedPivotConfig.y.includes('measures')) {
        otherColumns = (this.query().measures || []).map(function (key) {
          return _objectSpread2({}, extractFields(key), {
            dataIndex: key
          });
        });
      } // Syntatic column to display the measure value


      if (!normalizedPivotConfig.y.length && normalizedPivotConfig.x.includes('measures')) {
        otherColumns.push({
          key: 'value',
          dataIndex: 'value',
          title: 'Value',
          shortTitle: 'Value',
          type: 'string'
        });
      }

      return normalizedPivotConfig.x.map(function (key) {
        if (key === 'measures') {
          return {
            key: 'measures',
            dataIndex: 'measures',
            title: 'Measures',
            shortTitle: 'Measures',
            type: 'string'
          };
        }

        return _objectSpread2({}, extractFields(key), {
          dataIndex: key
        });
      }).concat(toColumns(schema)).concat(otherColumns);
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
      return pipe(map(this.axisValues(pivotConfig.y)), unnest, uniq)(this.timeDimensionBackwardCompatibleData()).map(function (axisValues) {
        return {
          title: _this5.axisValuesString(pivotConfig.y.find(function (d) {
            return d === 'measures';
          }) ? dropLast(1, axisValues).concat(_this5.loadResponse.annotation.measures[ResultSet.measureFromAxis(axisValues)].title) : axisValues, ', '),
          key: _this5.axisValuesString(axisValues),
          yValues: axisValues
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
  }, {
    key: "timeDimensionBackwardCompatibleData",
    value: function timeDimensionBackwardCompatibleData() {
      if (!this.backwardCompatibleData) {
        var query = this.loadResponse.query;
        var timeDimensions = (query.timeDimensions || []).filter(function (td) {
          return !!td.granularity;
        });
        this.backwardCompatibleData = this.loadResponse.data.map(function (row) {
          return _objectSpread2({}, row, {}, Object.keys(row).filter(function (field) {
            return timeDimensions.find(function (d) {
              return d.dimension === field;
            }) && !row[ResultSet.timeDimensionMember(timeDimensions.find(function (d) {
              return d.dimension === field;
            }))];
          }).map(function (field) {
            return _defineProperty({}, ResultSet.timeDimensionMember(timeDimensions.find(function (d) {
              return d.dimension === field;
            })), row[field]);
          }).reduce(function (a, b) {
            return _objectSpread2({}, a, {}, b);
          }, {}));
        });
      }

      return this.backwardCompatibleData;
    }
  }], [{
    key: "timeDimensionMember",
    value: function timeDimensionMember(td) {
      return "".concat(td.dimension, ".").concat(td.granularity);
    }
  }, {
    key: "getNormalizedPivotConfig",
    value: function getNormalizedPivotConfig(query) {
      var pivotConfig = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : null;
      var timeDimensions = (query.timeDimensions || []).filter(function (td) {
        return !!td.granularity;
      });
      var dimensions = query.dimensions || [];
      pivotConfig = pivotConfig || (timeDimensions.length ? {
        x: timeDimensions.map(function (td) {
          return ResultSet.timeDimensionMember(td);
        }),
        y: dimensions
      } : {
        x: dimensions,
        y: []
      });
      pivotConfig = _objectSpread2({}, pivotConfig, {
        x: _toConsumableArray(pivotConfig.x || []),
        y: _toConsumableArray(pivotConfig.y || [])
      });

      var substituteTimeDimensionMembers = function substituteTimeDimensionMembers(axis) {
        return axis.map(function (subDim) {
          return timeDimensions.find(function (td) {
            return td.dimension === subDim;
          }) && !dimensions.find(function (d) {
            return d === subDim;
          }) ? ResultSet.timeDimensionMember(query.timeDimensions.find(function (td) {
            return td.dimension === subDim;
          })) : subDim;
        });
      };

      pivotConfig.x = substituteTimeDimensionMembers(pivotConfig.x || []);
      pivotConfig.y = substituteTimeDimensionMembers(pivotConfig.y || []);
      var allIncludedDimensions = pivotConfig.x.concat(pivotConfig.y);
      var allDimensions = timeDimensions.map(function (td) {
        return ResultSet.timeDimensionMember(td);
      }).concat(dimensions);

      var dimensionFilter = function dimensionFilter(key) {
        return key === 'measures' || key !== 'measures' && allDimensions.includes(key);
      };

      pivotConfig.x = pivotConfig.x.concat(allDimensions.filter(function (d) {
        return !allIncludedDimensions.includes(d);
      })).filter(dimensionFilter);
      pivotConfig.y = pivotConfig.y.filter(dimensionFilter);

      if (!pivotConfig.x.concat(pivotConfig.y).find(function (d) {
        return d === 'measures';
      })) {
        pivotConfig.y.push('measures');
      }

      if (!(query.measures || []).length) {
        pivotConfig.x = pivotConfig.x.filter(function (d) {
          return d !== 'measures';
        });
        pivotConfig.y = pivotConfig.y.filter(function (d) {
          return d !== 'measures';
        });
      }

      if (pivotConfig.fillMissingDates == null) {
        pivotConfig.fillMissingDates = true;
      }

      return pivotConfig;
    }
  }, {
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
/**
 * Contains information about available cubes and it's members.
 */

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
  /**
   * Get all members of specific type for a given query.
   * If empty query is provided no filtering is done based on query context and all available members are retrieved.
   * @param query - context query to provide filtering of members available to add to this query
   * @param memberType - `measures`, `dimensions` or `segments`
   */


  _createClass(Meta, [{
    key: "membersForQuery",
    value: function membersForQuery(query, memberType) {
      return unnest(this.cubes.map(function (c) {
        return c[memberType];
      }));
    }
    /**
     * Get meta information for member of a cube
     * Member meta information contains:
     * ```javascript
     * {
     *   name,
     *   title,
     *   shortTitle,
     *   type,
     *   description,
     *   format
     * }
     * ```
     * @param memberName - Fully qualified member name in a form `Cube.memberName`
     * @param memberType - `measures`, `dimensions` or `segments`
     * @return {Object} containing meta information about member
     */

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

var HttpTransport =
/*#__PURE__*/
function () {
  function HttpTransport(_ref) {
    var authorization = _ref.authorization,
        apiUrl = _ref.apiUrl,
        _ref$headers = _ref.headers,
        headers = _ref$headers === void 0 ? {} : _ref$headers,
        credentials = _ref.credentials;

    _classCallCheck(this, HttpTransport);

    this.authorization = authorization;
    this.apiUrl = apiUrl;
    this.headers = headers;
    this.credentials = credentials;
  }

  _createClass(HttpTransport, [{
    key: "request",
    value: function request(method, _ref2) {
      var _this = this;

      var baseRequestId = _ref2.baseRequestId,
          params = _objectWithoutProperties(_ref2, ["baseRequestId"]);

      var searchParams = new URLSearchParams(params && Object.keys(params).map(function (k) {
        return _defineProperty({}, k, _typeof(params[k]) === 'object' ? JSON.stringify(params[k]) : params[k]);
      }).reduce(function (a, b) {
        return _objectSpread2({}, a, {}, b);
      }, {}));
      var spanCounter = 1; // Currently, all methods make GET requests. If a method makes a request with a body payload,
      // remember to add a 'Content-Type' header.

      var runRequest = function runRequest() {
        return fetch("".concat(_this.apiUrl, "/").concat(method).concat(searchParams.toString().length ? "?".concat(searchParams) : ''), {
          headers: _objectSpread2({
            Authorization: _this.authorization,
            'x-request-id': baseRequestId && "".concat(baseRequestId, "-span-").concat(spanCounter++)
          }, _this.headers),
          credentials: _this.credentials
        });
      };

      return {
        subscribe: function subscribe(callback) {
          var _this2 = this;

          return _asyncToGenerator(
          /*#__PURE__*/
          _regeneratorRuntime.mark(function _callee() {
            var result;
            return _regeneratorRuntime.wrap(function _callee$(_context) {
              while (1) {
                switch (_context.prev = _context.next) {
                  case 0:
                    _context.next = 2;
                    return runRequest();

                  case 2:
                    result = _context.sent;
                    return _context.abrupt("return", callback(result, function () {
                      return _this2.subscribe(callback);
                    }));

                  case 4:
                  case "end":
                    return _context.stop();
                }
              }
            }, _callee);
          }))();
        }
      };
    }
  }]);

  return HttpTransport;
}();

var RequestError =
/*#__PURE__*/
function (_Error) {
  _inherits(RequestError, _Error);

  function RequestError(message, response) {
    var _this;

    _classCallCheck(this, RequestError);

    _this = _possibleConstructorReturn(this, _getPrototypeOf(RequestError).call(this, message));
    _this.response = response;
    return _this;
  }

  return RequestError;
}(_wrapNativeSuper(Error));

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

    if (_typeof(apiToken) === 'object') {
      options = apiToken;
      apiToken = undefined;
    }

    options = options || {};
    this.apiToken = apiToken;
    this.apiUrl = options.apiUrl || API_URL;
    this.headers = options.headers || {};
    this.credentials = options.credentials;
    this.transport = options.transport || new HttpTransport({
      authorization: typeof apiToken === 'function' ? undefined : apiToken,
      apiUrl: this.apiUrl,
      headers: this.headers,
      credentials: this.credentials
    });
    this.pollInterval = options.pollInterval || 5;
    this.parseDateMeasures = options.parseDateMeasures;
  }

  _createClass(CubejsApi, [{
    key: "request",
    value: function request(method, params) {
      return this.transport.request(method, _objectSpread2({
        baseRequestId: uuid()
      }, params));
    }
    /**
     * Base method used to perform all API calls.
     * Shouldn't be used directly.
     * @param request - function that invoked to perform actual request using `transport.request()` method.
     * @param toResult - function that maps results of invocation to method return result
     * @param [options] - options object
     * @param options.mutexObj - object to use to store MUTEX
     * @param [options.mutexKey='default'] - key to use to store current request MUTEX inside `mutexObj`.
     * MUTEX object is used to reject orphaned queries results when new queries are sent.
     * For example if two queries are sent with same `mutexKey` only last one will return results.
     * @param options.subscribe - pass `true` to use continuous fetch behavior.
     * @param {Function} options.progressCallback - function that receives `ProgressResult` on each
     * `Continue wait` message.
     * @param [callback] - if passed `callback` function will be called instead of `Promise` returned
     * @return {{unsubscribe: function()}}
     */

  }, {
    key: "loadMethod",
    value: function loadMethod(request, toResult, options, callback) {
      var _this = this;

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

      var requestPromise = this.updateTransportAuthorization().then(function () {
        return request();
      });
      var unsubscribed = false;

      var checkMutex =
      /*#__PURE__*/
      function () {
        var _ref = _asyncToGenerator(
        /*#__PURE__*/
        _regeneratorRuntime.mark(function _callee() {
          var requestInstance;
          return _regeneratorRuntime.wrap(function _callee$(_context) {
            while (1) {
              switch (_context.prev = _context.next) {
                case 0:
                  _context.next = 2;
                  return requestPromise;

                case 2:
                  requestInstance = _context.sent;

                  if (!(options.mutexObj && options.mutexObj[mutexKey] !== mutexValue)) {
                    _context.next = 9;
                    break;
                  }

                  unsubscribed = true;

                  if (!requestInstance.unsubscribe) {
                    _context.next = 8;
                    break;
                  }

                  _context.next = 8;
                  return requestInstance.unsubscribe();

                case 8:
                  throw MUTEX_ERROR;

                case 9:
                case "end":
                  return _context.stop();
              }
            }
          }, _callee);
        }));

        return function checkMutex() {
          return _ref.apply(this, arguments);
        };
      }();

      var loadImpl =
      /*#__PURE__*/
      function () {
        var _ref2 = _asyncToGenerator(
        /*#__PURE__*/
        _regeneratorRuntime.mark(function _callee4(response, next) {
          var requestInstance, subscribeNext, continueWait, body, error, result;
          return _regeneratorRuntime.wrap(function _callee4$(_context4) {
            while (1) {
              switch (_context4.prev = _context4.next) {
                case 0:
                  _context4.next = 2;
                  return requestPromise;

                case 2:
                  requestInstance = _context4.sent;

                  subscribeNext =
                  /*#__PURE__*/
                  function () {
                    var _ref3 = _asyncToGenerator(
                    /*#__PURE__*/
                    _regeneratorRuntime.mark(function _callee2() {
                      return _regeneratorRuntime.wrap(function _callee2$(_context2) {
                        while (1) {
                          switch (_context2.prev = _context2.next) {
                            case 0:
                              if (!(options.subscribe && !unsubscribed)) {
                                _context2.next = 8;
                                break;
                              }

                              if (!requestInstance.unsubscribe) {
                                _context2.next = 5;
                                break;
                              }

                              return _context2.abrupt("return", next());

                            case 5:
                              _context2.next = 7;
                              return new Promise(function (resolve) {
                                return setTimeout(function () {
                                  return resolve();
                                }, _this.pollInterval * 1000);
                              });

                            case 7:
                              return _context2.abrupt("return", next());

                            case 8:
                              return _context2.abrupt("return", null);

                            case 9:
                            case "end":
                              return _context2.stop();
                          }
                        }
                      }, _callee2);
                    }));

                    return function subscribeNext() {
                      return _ref3.apply(this, arguments);
                    };
                  }();

                  continueWait =
                  /*#__PURE__*/
                  function () {
                    var _ref4 = _asyncToGenerator(
                    /*#__PURE__*/
                    _regeneratorRuntime.mark(function _callee3(wait) {
                      return _regeneratorRuntime.wrap(function _callee3$(_context3) {
                        while (1) {
                          switch (_context3.prev = _context3.next) {
                            case 0:
                              if (unsubscribed) {
                                _context3.next = 5;
                                break;
                              }

                              if (!wait) {
                                _context3.next = 4;
                                break;
                              }

                              _context3.next = 4;
                              return new Promise(function (resolve) {
                                return setTimeout(function () {
                                  return resolve();
                                }, _this.pollInterval * 1000);
                              });

                            case 4:
                              return _context3.abrupt("return", next());

                            case 5:
                              return _context3.abrupt("return", null);

                            case 6:
                            case "end":
                              return _context3.stop();
                          }
                        }
                      }, _callee3);
                    }));

                    return function continueWait(_x3) {
                      return _ref4.apply(this, arguments);
                    };
                  }();

                  _context4.next = 7;
                  return _this.updateTransportAuthorization();

                case 7:
                  if (!(response.status === 502)) {
                    _context4.next = 11;
                    break;
                  }

                  _context4.next = 10;
                  return checkMutex();

                case 10:
                  return _context4.abrupt("return", continueWait(true));

                case 11:
                  _context4.next = 13;
                  return response.json();

                case 13:
                  body = _context4.sent;

                  if (!(body.error === 'Continue wait')) {
                    _context4.next = 19;
                    break;
                  }

                  _context4.next = 17;
                  return checkMutex();

                case 17:
                  if (options.progressCallback) {
                    options.progressCallback(new ProgressResult(body));
                  }

                  return _context4.abrupt("return", continueWait());

                case 19:
                  if (!(response.status !== 200)) {
                    _context4.next = 32;
                    break;
                  }

                  _context4.next = 22;
                  return checkMutex();

                case 22:
                  if (!(!options.subscribe && requestInstance.unsubscribe)) {
                    _context4.next = 25;
                    break;
                  }

                  _context4.next = 25;
                  return requestInstance.unsubscribe();

                case 25:
                  error = new RequestError(body.error, body); // TODO error class

                  if (!callback) {
                    _context4.next = 30;
                    break;
                  }

                  callback(error);
                  _context4.next = 31;
                  break;

                case 30:
                  throw error;

                case 31:
                  return _context4.abrupt("return", subscribeNext());

                case 32:
                  _context4.next = 34;
                  return checkMutex();

                case 34:
                  if (!(!options.subscribe && requestInstance.unsubscribe)) {
                    _context4.next = 37;
                    break;
                  }

                  _context4.next = 37;
                  return requestInstance.unsubscribe();

                case 37:
                  result = toResult(body);

                  if (!callback) {
                    _context4.next = 42;
                    break;
                  }

                  callback(null, result);
                  _context4.next = 43;
                  break;

                case 42:
                  return _context4.abrupt("return", result);

                case 43:
                  return _context4.abrupt("return", subscribeNext());

                case 44:
                case "end":
                  return _context4.stop();
              }
            }
          }, _callee4);
        }));

        return function loadImpl(_x, _x2) {
          return _ref2.apply(this, arguments);
        };
      }();

      var promise = requestPromise.then(function (requestInstance) {
        return mutexPromise(requestInstance.subscribe(loadImpl));
      });

      if (callback) {
        return {
          unsubscribe: function () {
            var _unsubscribe = _asyncToGenerator(
            /*#__PURE__*/
            _regeneratorRuntime.mark(function _callee5() {
              var requestInstance;
              return _regeneratorRuntime.wrap(function _callee5$(_context5) {
                while (1) {
                  switch (_context5.prev = _context5.next) {
                    case 0:
                      _context5.next = 2;
                      return requestPromise;

                    case 2:
                      requestInstance = _context5.sent;
                      unsubscribed = true;

                      if (!requestInstance.unsubscribe) {
                        _context5.next = 6;
                        break;
                      }

                      return _context5.abrupt("return", requestInstance.unsubscribe());

                    case 6:
                      return _context5.abrupt("return", null);

                    case 7:
                    case "end":
                      return _context5.stop();
                  }
                }
              }, _callee5);
            }));

            function unsubscribe() {
              return _unsubscribe.apply(this, arguments);
            }

            return unsubscribe;
          }()
        };
      } else {
        return promise;
      }
    }
  }, {
    key: "updateTransportAuthorization",
    value: function () {
      var _updateTransportAuthorization = _asyncToGenerator(
      /*#__PURE__*/
      _regeneratorRuntime.mark(function _callee6() {
        var token;
        return _regeneratorRuntime.wrap(function _callee6$(_context6) {
          while (1) {
            switch (_context6.prev = _context6.next) {
              case 0:
                if (!(typeof this.apiToken === 'function')) {
                  _context6.next = 5;
                  break;
                }

                _context6.next = 3;
                return this.apiToken();

              case 3:
                token = _context6.sent;

                if (this.transport.authorization !== token) {
                  this.transport.authorization = token;
                }

              case 5:
              case "end":
                return _context6.stop();
            }
          }
        }, _callee6, this);
      }));

      function updateTransportAuthorization() {
        return _updateTransportAuthorization.apply(this, arguments);
      }

      return updateTransportAuthorization;
    }()
  }, {
    key: "load",
    value: function load(query, options, callback) {
      var _this2 = this;

      return this.loadMethod(function () {
        return _this2.request('load', {
          query: query
        });
      }, function (body) {
        return new ResultSet(body, {
          parseDateMeasures: _this2.parseDateMeasures
        });
      }, options, callback);
    }
  }, {
    key: "sql",
    value: function sql(query, options, callback) {
      var _this3 = this;

      return this.loadMethod(function () {
        return _this3.request('sql', {
          query: query
        });
      }, function (body) {
        return new SqlQuery(body);
      }, options, callback);
    }
  }, {
    key: "meta",
    value: function meta(options, callback) {
      var _this4 = this;

      return this.loadMethod(function () {
        return _this4.request('meta');
      }, function (body) {
        return new Meta(body);
      }, options, callback);
    }
  }, {
    key: "subscribe",
    value: function subscribe(query, options, callback) {
      var _this5 = this;

      return this.loadMethod(function () {
        return _this5.request('subscribe', {
          query: query
        });
      }, function (body) {
        return new ResultSet(body, {
          parseDateMeasures: _this5.parseDateMeasures
        });
      }, _objectSpread2({}, options, {
        subscribe: true
      }), callback);
    }
  }]);

  return CubejsApi;
}();

var index = (function (apiToken, options) {
  return new CubejsApi(apiToken, options);
});

export default index;
export { HttpTransport, ResultSet };
