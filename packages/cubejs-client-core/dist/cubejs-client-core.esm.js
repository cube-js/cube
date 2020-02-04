import _objectSpread from '@babel/runtime/helpers/objectSpread';
import _regeneratorRuntime from '@babel/runtime/regenerator';
import 'regenerator-runtime/runtime';
import _asyncToGenerator from '@babel/runtime/helpers/asyncToGenerator';
import _typeof from '@babel/runtime/helpers/typeof';
import _classCallCheck from '@babel/runtime/helpers/classCallCheck';
import _createClass from '@babel/runtime/helpers/createClass';
import 'core-js/modules/es6.promise';
import 'core-js/modules/es6.number.constructor';
import 'core-js/modules/es6.number.parse-float';
import 'core-js/modules/web.dom.iterable';
import 'core-js/modules/es6.array.iterator';
import 'core-js/modules/es6.object.keys';
import _slicedToArray from '@babel/runtime/helpers/slicedToArray';
import 'core-js/modules/es6.object.assign';
import _defineProperty from '@babel/runtime/helpers/defineProperty';
import 'core-js/modules/es6.array.reduce';
import 'core-js/modules/es6.regexp.match';
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
import 'core-js/modules/es6.regexp.to-string';
import 'core-js/modules/es6.date.to-string';
import fetch from 'cross-fetch';
import 'url-search-params-polyfill';

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
/**
 * Provides a convenient interface for data manipulation.
 */

var ResultSet =
/*#__PURE__*/
function () {
  function ResultSet(loadResponse) {
    _classCallCheck(this, ResultSet);

    this.loadResponse = loadResponse;
  }
  /**
   * Returns an array of series with key, title and series data.
   *
   * ```js
   * // For query
   * {
   *   measures: ['Stories.count'],
   *   timeDimensions: [{
   *     dimension: 'Stories.time',
   *     dateRange: ['2015-01-01', '2015-12-31'],
   *     granularity: 'month'
   *   }]
   * }
   *
   * // ResultSet.series() will return
   * [
   *   {
   *     "key":"Stories.count",
   *     "title": "Stories Count",
   *     "series": [
   *       { "x":"2015-01-01T00:00:00", "value": 27120 },
   *       { "x":"2015-02-01T00:00:00", "value": 25861 },
   *       { "x": "2015-03-01T00:00:00", "value": 29661 },
   *       //...
   *     ]
   *   }
   * ]
   * ```
   * @param pivotConfig
   * @returns {Array}
   */


  _createClass(ResultSet, [{
    key: "series",
    value: function series(pivotConfig) {
      var _this = this;

      return this.seriesNames(pivotConfig).map(function (_ref) {
        var title = _ref.title,
            key = _ref.key;
        return {
          title: title,
          key: key,
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
      pivotConfig.x = pivotConfig.x.concat(allDimensions.filter(function (d) {
        return allIncludedDimensions.indexOf(d) === -1;
      }));

      if (!pivotConfig.x.concat(pivotConfig.y).find(function (d) {
        return d === 'measures';
      })) {
        pivotConfig.y = pivotConfig.y.concat(['measures']);
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
      }) : ['hour', 'minute', 'second'].indexOf(timeDimension.granularity) === -1;
      var start = moment(dateRange[0]).format(padToDay ? 'YYYY-MM-DDT00:00:00.000' : moment.HTML5_FMT.DATETIME_LOCAL_MS);
      var end = moment(dateRange[1]).format(padToDay ? 'YYYY-MM-DDT23:59:59.999' : moment.HTML5_FMT.DATETIME_LOCAL_MS);
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
        return ResultSet.timeDimensionMember(td);
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
      }), unnest, groupByXAxis, toPairs)(this.timeDimensionBackwardCompatibleData());
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
    /**
     * Returns normalized query result data in the following format.
     *
     * ```js
     * // For query
     * {
     *   measures: ['Stories.count'],
     *   timeDimensions: [{
     *     dimension: 'Stories.time',
     *     dateRange: ['2015-01-01', '2015-12-31'],
     *     granularity: 'month'
     *   }]
     * }
     *
     * // ResultSet.chartPivot() will return
     * [
     *   { "x":"2015-01-01T00:00:00", "Stories.count": 27120 },
     *   { "x":"2015-02-01T00:00:00", "Stories.count": 25861 },
     *   { "x": "2015-03-01T00:00:00", "Stories.count": 29661 },
     *   //...
     * ]
     * ```
     * @param pivotConfig
     */

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
    /**
     * Returns normalized query result data prepared for visualization in the table format.
     *
     * For example
     *
     * ```js
     * // For query
     * {
     *   measures: ['Stories.count'],
     *   timeDimensions: [{
     *     dimension: 'Stories.time',
     *     dateRange: ['2015-01-01', '2015-12-31'],
     *     granularity: 'month'
     *   }]
     * }
     *
     * // ResultSet.tablePivot() will return
     * [
     *   { "Stories.time": "2015-01-01T00:00:00", "Stories.count": 27120 },
     *   { "Stories.time": "2015-02-01T00:00:00", "Stories.count": 25861 },
     *   { "Stories.time": "2015-03-01T00:00:00", "Stories.count": 29661 },
     *   //...
     * ]
     * ```
     * @param pivotConfig
     * @returns {Array} of pivoted rows
     */

  }, {
    key: "tablePivot",
    value: function tablePivot(pivotConfig) {
      var normalizedPivotConfig = this.normalizePivotConfig(pivotConfig || {});

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
    /**
     * Returns array of column definitions for `tablePivot`.
     *
     * For example
     *
     * ```js
     * // For query
     * {
     *   measures: ['Stories.count'],
     *   timeDimensions: [{
     *     dimension: 'Stories.time',
     *     dateRange: ['2015-01-01', '2015-12-31'],
     *     granularity: 'month'
     *   }]
     * }
     *
     * // ResultSet.tableColumns() will return
     * [
     *   { key: "Stories.time", title: "Stories Time", shortTitle: "Time" },
     *   { key: "Stories.count", title: "Stories Count", shortTitle: "Count" },
     *   //...
     * ]
     * ```
     * @param pivotConfig
     * @returns {Array} of columns
     */

  }, {
    key: "tableColumns",
    value: function tableColumns(pivotConfig) {
      var _this4 = this;

      var normalizedPivotConfig = this.normalizePivotConfig(pivotConfig);

      var column = function column(field) {
        return field === 'measures' ? (_this4.query().measures || []).map(function (m) {
          return {
            key: m,
            title: _this4.loadResponse.annotation.measures[m].title,
            shortTitle: _this4.loadResponse.annotation.measures[m].shortTitle
          };
        }) : [{
          key: field,
          title: (_this4.loadResponse.annotation.dimensions[field] || _this4.loadResponse.annotation.timeDimensions[field]).title,
          shortTitle: (_this4.loadResponse.annotation.dimensions[field] || _this4.loadResponse.annotation.timeDimensions[field]).shortTitle
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
    /**
     * Returns the array of series objects, containing `key` and `title` parameters.
     *
     * ```js
     * // For query
     * {
     *   measures: ['Stories.count'],
     *   timeDimensions: [{
     *     dimension: 'Stories.time',
     *     dateRange: ['2015-01-01', '2015-12-31'],
     *     granularity: 'month'
     *   }]
     * }
     *
     * // ResultSet.seriesNames() will return
     * [
     * { "key":"Stories.count", "title": "Stories Count" }
     * ]
     * ```
     * @param pivotConfig
     * @returns {Array} of series names
     */

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
  }, {
    key: "timeDimensionBackwardCompatibleData",
    value: function timeDimensionBackwardCompatibleData() {
      if (!this.backwardCompatibleData) {
        var query = this.loadResponse.query;
        var timeDimensions = (query.timeDimensions || []).filter(function (td) {
          return !!td.granularity;
        });
        this.backwardCompatibleData = this.loadResponse.data.map(function (row) {
          return _objectSpread({}, row, Object.keys(row).filter(function (field) {
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
            return _objectSpread({}, a, b);
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

var HttpTransport =
/*#__PURE__*/
function () {
  function HttpTransport(_ref) {
    var authorization = _ref.authorization,
        apiUrl = _ref.apiUrl,
        _ref$headers = _ref.headers,
        headers = _ref$headers === void 0 ? {} : _ref$headers;

    _classCallCheck(this, HttpTransport);

    this.authorization = authorization;
    this.apiUrl = apiUrl;
    this.headers = headers;
  }

  _createClass(HttpTransport, [{
    key: "request",
    value: function request(method, params) {
      var _this = this;

      var searchParams = new URLSearchParams(params && Object.keys(params).map(function (k) {
        return _defineProperty({}, k, _typeof(params[k]) === 'object' ? JSON.stringify(params[k]) : params[k]);
      }).reduce(function (a, b) {
        return _objectSpread({}, a, b);
      }, {}));

      var runRequest = function runRequest() {
        return fetch("".concat(_this.apiUrl, "/").concat(method).concat(searchParams.toString().length ? "?".concat(searchParams) : ''), {
          headers: Object.assign({
            Authorization: _this.authorization,
            'Content-Type': 'application/json'
          }, _this.headers)
        });
      };

      return {
        subscribe: function () {
          var _subscribe = _asyncToGenerator(
          /*#__PURE__*/
          _regeneratorRuntime.mark(function _callee(callback) {
            var _this2 = this;

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
            }, _callee, this);
          }));

          return function subscribe(_x) {
            return _subscribe.apply(this, arguments);
          };
        }()
      };
    }
  }]);

  return HttpTransport;
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
/**
 * Main class for accessing Cube.js API
 * @order -5
 */


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
    this.transport = options.transport || new HttpTransport({
      authorization: typeof apiToken === 'function' ? undefined : apiToken,
      apiUrl: this.apiUrl,
      headers: this.headers
    });
    this.pollInterval = options.pollInterval || 5;
  }

  _createClass(CubejsApi, [{
    key: "request",
    value: function request(method, params) {
      return this.transport.request(method, params);
    }
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
          }, _callee, this);
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
                      }, _callee2, this);
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
                      }, _callee3, this);
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
                  error = new Error(body.error); // TODO error class

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
          }, _callee4, this);
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
              }, _callee5, this);
            }));

            return function unsubscribe() {
              return _unsubscribe.apply(this, arguments);
            };
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

      return function updateTransportAuthorization() {
        return _updateTransportAuthorization.apply(this, arguments);
      };
    }()
    /**
     * Fetch data for passed `query`.
     *
     * ```js
     * import cubejs from '@cubejs-client/core';
     * import Chart from 'chart.js';
     * import chartjsConfig from './toChartjsData';
     *
     * const cubejsApi = cubejs('CUBEJS_TOKEN');
     *
     * const resultSet = await cubejsApi.load({
     *  measures: ['Stories.count'],
     *  timeDimensions: [{
     *    dimension: 'Stories.time',
     *    dateRange: ['2015-01-01', '2015-12-31'],
     *    granularity: 'month'
     *   }]
     * });
     *
     * const context = document.getElementById('myChart');
     * new Chart(context, chartjsConfig(resultSet));
     * ```
     * @param query - [Query object](query-format)
     * @param options
     * @param callback
     * @returns {Promise} for {@link ResultSet} if `callback` isn't passed
     */

  }, {
    key: "load",
    value: function load(query, options, callback) {
      var _this2 = this;

      return this.loadMethod(function () {
        return _this2.request("load", {
          query: query
        });
      }, function (body) {
        return new ResultSet(body);
      }, options, callback);
    }
    /**
     * Get generated SQL string for given `query`.
     * @param query - [Query object](query-format)
     * @param options
     * @param callback
     * @return {Promise} for {@link SqlQuery} if `callback` isn't passed
     */

  }, {
    key: "sql",
    value: function sql(query, options, callback) {
      var _this3 = this;

      return this.loadMethod(function () {
        return _this3.request("sql", {
          query: query
        });
      }, function (body) {
        return new SqlQuery(body);
      }, options, callback);
    }
    /**
     * Get meta description of cubes available for querying.
     * @param options
     * @param callback
     * @return {Promise} for {@link Meta} if `callback` isn't passed
     */

  }, {
    key: "meta",
    value: function meta(options, callback) {
      var _this4 = this;

      return this.loadMethod(function () {
        return _this4.request("meta");
      }, function (body) {
        return new Meta(body);
      }, options, callback);
    }
  }, {
    key: "subscribe",
    value: function subscribe(query, options, callback) {
      var _this5 = this;

      return this.loadMethod(function () {
        return _this5.request("subscribe", {
          query: query
        });
      }, function (body) {
        return new ResultSet(body);
      }, _objectSpread({}, options, {
        subscribe: true
      }), callback);
    }
  }]);

  return CubejsApi;
}();
/**
 * Create instance of `CubejsApi`.
 * API entry point.
 *
 * ```javascript
 import cubejs from '@cubejs-client/core';

 const cubejsApi = cubejs(
 'CUBEJS-API-TOKEN',
 { apiUrl: 'http://localhost:4000/cubejs-api/v1' }
 );
 ```
 * @name cubejs
 * @param apiToken - [API token](security) is used to authorize requests and determine SQL database you're accessing.
 * In the development mode, Cube.js Backend will print the API token to the console on on startup.
 * Can be an async function without arguments that returns API token.
 * @param options - options object.
 * @param options.apiUrl - URL of your Cube.js Backend.
 * By default, in the development environment it is `http://localhost:4000/cubejs-api/v1`.
 * @returns {CubejsApi}
 * @order -10
 */


var index = (function (apiToken, options) {
  return new CubejsApi(apiToken, options);
});

export default index;
export { HttpTransport };
