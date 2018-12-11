(function (global, factory) {
	typeof exports === 'object' && typeof module !== 'undefined' ? module.exports = factory() :
	typeof define === 'function' && define.amd ? define(factory) :
	(global.cubejs = factory());
}(this, (function () { 'use strict';

	var require$$0 = /*#__PURE__*/Object.freeze({

	});
	var require$$0$1 = /*#__PURE__*/Object.freeze({

	});
	var require$$0$2 = /*#__PURE__*/Object.freeze({

	});
	var $keys$1 = /*#__PURE__*/Object.freeze({

	});
	var require$$0$3 = /*#__PURE__*/Object.freeze({

	});
	var require$$0$4 = /*#__PURE__*/Object.freeze({

	});

	function createCommonjsModule(fn, module) {
		return module = { exports: {} }, fn(module, module.exports), module.exports;
	}

	var _global = createCommonjsModule(function (module) {
	// https://github.com/zloirock/core-js/issues/86#issuecomment-115759028
	var global = module.exports = typeof window != 'undefined' && window.Math == Math ? window : typeof self != 'undefined' && self.Math == Math ? self // eslint-disable-next-line no-new-func
	: Function('return this')();
	if (typeof __g == 'number') __g = global; // eslint-disable-line no-undef
	});

	function _typeof(obj) {
	  if (typeof Symbol === "function" && typeof Symbol.iterator === "symbol") {
	    _typeof = function (obj) {
	      return typeof obj;
	    };
	  } else {
	    _typeof = function (obj) {
	      return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj;
	    };
	  }

	  return _typeof(obj);
	}

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

	module.exports = function (it) {
	  return _typeof(it) === 'object' ? it !== null : typeof it === 'function';
	};

	var isObject = /*#__PURE__*/Object.freeze({

	});

	var global$1 = require('./_global');

	var core = require('./_core');

	var hide = require('./_hide');

	var redefine = require('./_redefine');

	var ctx = require('./_ctx');

	var PROTOTYPE = 'prototype';

	var $export = function $export(type, name, source) {
	  var IS_FORCED = type & $export.F;
	  var IS_GLOBAL = type & $export.G;
	  var IS_STATIC = type & $export.S;
	  var IS_PROTO = type & $export.P;
	  var IS_BIND = type & $export.B;
	  var target = IS_GLOBAL ? global$1 : IS_STATIC ? global$1[name] || (global$1[name] = {}) : (global$1[name] || {})[PROTOTYPE];
	  var exports = IS_GLOBAL ? core : core[name] || (core[name] = {});
	  var expProto = exports[PROTOTYPE] || (exports[PROTOTYPE] = {});
	  var key, own, out, exp;
	  if (IS_GLOBAL) source = name;

	  for (key in source) {
	    // contains in native
	    own = !IS_FORCED && target && target[key] !== undefined; // export native or passed

	    out = (own ? target : source)[key]; // bind timers to global for call from export context

	    exp = IS_BIND && own ? ctx(out, global$1) : IS_PROTO && typeof out == 'function' ? ctx(Function.call, out) : out; // extend global

	    if (target) redefine(target, key, out, type & $export.U); // export

	    if (exports[key] != out) hide(exports, key, exp);
	    if (IS_PROTO && expProto[key] != out) expProto[key] = out;
	  }
	};

	global$1.core = core; // type bitmap

	$export.F = 1; // forced

	$export.G = 2; // global

	$export.S = 4; // static

	$export.P = 8; // proto

	$export.B = 16; // bind

	$export.W = 32; // wrap

	$export.U = 64; // safe

	$export.R = 128; // real proto method for `library`

	module.exports = $export;

	var $export$1 = /*#__PURE__*/Object.freeze({

	});

	// 19.1.3.19 Object.setPrototypeOf(O, proto)


	$export$1($export$1.S, 'Object', {
	  setPrototypeOf: require$$0.set
	});

	// Works with __proto__ only. Old v8 can't work with null proto objects.

	/* eslint-disable no-proto */
	var isObject$1 = require('./_is-object');

	var anObject = require('./_an-object');

	var check = function check(O, proto) {
	  anObject(O);
	  if (!isObject$1(proto) && proto !== null) throw TypeError(proto + ": can't set as prototype!");
	};

	module.exports = {
	  set: Object.setPrototypeOf || ('__proto__' in {} ? // eslint-disable-line
	  function (test, buggy, set) {
	    try {
	      set = require('./_ctx')(Function.call, require('./_object-gopd').f(Object.prototype, '__proto__').set, 2);
	      set(test, []);
	      buggy = !(test instanceof Array);
	    } catch (e) {
	      buggy = true;
	    }

	    return function setPrototypeOf(O, proto) {
	      check(O, proto);
	      if (buggy) O.__proto__ = proto;else set(O, proto);
	      return O;
	    };
	  }({}, false) : undefined),
	  check: check
	};

	var setPrototypeOf = require$$0.set;

	var _inheritIfRequired = function (that, target, C) {
	  var S = target.constructor;
	  var P;

	  if (S !== C && typeof S == 'function' && (P = S.prototype) !== C.prototype && isObject(P) && setPrototypeOf) {
	    setPrototypeOf(that, P);
	  }

	  return that;
	};

	// Thank's IE8 for his funny defineProperty
	module.exports = !require('./_fails')(function () {
	  return Object.defineProperty({}, 'a', {
	    get: function get() {
	      return 7;
	    }
	  }).a != 7;
	});

	var DESCRIPTORS = /*#__PURE__*/Object.freeze({

	});

	// 19.1.2.4 / 15.2.3.6 Object.defineProperty(O, P, Attributes)


	$export$1($export$1.S + $export$1.F * !DESCRIPTORS, 'Object', {
	  defineProperty: require$$0$1.f
	});

	var anObject$1 = require('./_an-object');

	var IE8_DOM_DEFINE = require('./_ie8-dom-define');

	var toPrimitive = require('./_to-primitive');

	var dP = Object.defineProperty;
	exports.f = require('./_descriptors') ? Object.defineProperty : function defineProperty(O, P, Attributes) {
	  anObject$1(O);
	  P = toPrimitive(P, true);
	  anObject$1(Attributes);
	  if (IE8_DOM_DEFINE) try {
	    return dP(O, P, Attributes);
	  } catch (e) {
	    /* empty */
	  }
	  if ('get' in Attributes || 'set' in Attributes) throw TypeError('Accessors not supported!');
	  if ('value' in Attributes) O[P] = Attributes.value;
	  return O;
	};

	var hasOwnProperty = {}.hasOwnProperty;

	var _has = function (it, key) {
	  return hasOwnProperty.call(it, key);
	};

	var hide$1 = require('./_hide');

	var redefine$1 = require('./_redefine');

	var fails = require('./_fails');

	var defined = require('./_defined');

	var wks = require('./_wks');

	module.exports = function (KEY, length, exec) {
	  var SYMBOL = wks(KEY);
	  var fns = exec(defined, SYMBOL, ''[KEY]);
	  var strfn = fns[0];
	  var rxfn = fns[1];

	  if (fails(function () {
	    var O = {};

	    O[SYMBOL] = function () {
	      return 7;
	    };

	    return ''[KEY](O) != 7;
	  })) {
	    redefine$1(String.prototype, KEY, strfn);
	    hide$1(RegExp.prototype, SYMBOL, length == 2 // 21.2.5.8 RegExp.prototype[@@replace](string, replaceValue)
	    // 21.2.5.11 RegExp.prototype[@@split](string, limit)
	    ? function (string, arg) {
	      return rxfn.call(string, this, arg);
	    } // 21.2.5.6 RegExp.prototype[@@match](string)
	    // 21.2.5.9 RegExp.prototype[@@search](string)
	    : function (string) {
	      return rxfn.call(string, this);
	    });
	  }
	};

	var _fixReWks = /*#__PURE__*/Object.freeze({

	});

	// @@replace logic
	_fixReWks('replace', 2, function (defined, REPLACE, $replace) {
	  // 21.1.3.14 String.prototype.replace(searchValue, replaceValue)
	  return [function replace(searchValue, replaceValue) {

	    var O = defined(this);
	    var fn = searchValue == undefined ? undefined : searchValue[REPLACE];
	    return fn !== undefined ? fn.call(searchValue, O, replaceValue) : $replace.call(String(O), searchValue, replaceValue);
	  }, $replace];
	});

	// @@split logic
	require('./_fix-re-wks')('split', 2, function (defined, SPLIT, $split) {

	  var isRegExp = require('./_is-regexp');

	  var _split = $split;
	  var $push = [].push;
	  var $SPLIT = 'split';
	  var LENGTH = 'length';
	  var LAST_INDEX = 'lastIndex';

	  if ('abbc'[$SPLIT](/(b)*/)[1] == 'c' || 'test'[$SPLIT](/(?:)/, -1)[LENGTH] != 4 || 'ab'[$SPLIT](/(?:ab)*/)[LENGTH] != 2 || '.'[$SPLIT](/(.?)(.?)/)[LENGTH] != 4 || '.'[$SPLIT](/()()/)[LENGTH] > 1 || ''[$SPLIT](/.?/)[LENGTH]) {
	    var NPCG = /()??/.exec('')[1] === undefined; // nonparticipating capturing group
	    // based on es5-shim implementation, need to rework it

	    $split = function $split(separator, limit) {
	      var string = String(this);
	      if (separator === undefined && limit === 0) return []; // If `separator` is not a regex, use native split

	      if (!isRegExp(separator)) return _split.call(string, separator, limit);
	      var output = [];
	      var flags = (separator.ignoreCase ? 'i' : '') + (separator.multiline ? 'm' : '') + (separator.unicode ? 'u' : '') + (separator.sticky ? 'y' : '');
	      var lastLastIndex = 0;
	      var splitLimit = limit === undefined ? 4294967295 : limit >>> 0; // Make `global` and avoid `lastIndex` issues by working with a copy

	      var separatorCopy = new RegExp(separator.source, flags + 'g');
	      var separator2, match, lastIndex, lastLength, i; // Doesn't need flags gy, but they don't hurt

	      if (!NPCG) separator2 = new RegExp('^' + separatorCopy.source + '$(?!\\s)', flags);

	      while (match = separatorCopy.exec(string)) {
	        // `separatorCopy.lastIndex` is not reliable cross-browser
	        lastIndex = match.index + match[0][LENGTH];

	        if (lastIndex > lastLastIndex) {
	          output.push(string.slice(lastLastIndex, match.index)); // Fix browsers whose `exec` methods don't consistently return `undefined` for NPCG
	          // eslint-disable-next-line no-loop-func

	          if (!NPCG && match[LENGTH] > 1) match[0].replace(separator2, function () {
	            for (i = 1; i < arguments[LENGTH] - 2; i++) {
	              if (arguments[i] === undefined) match[i] = undefined;
	            }
	          });
	          if (match[LENGTH] > 1 && match.index < string[LENGTH]) $push.apply(output, match.slice(1));
	          lastLength = match[0][LENGTH];
	          lastLastIndex = lastIndex;
	          if (output[LENGTH] >= splitLimit) break;
	        }

	        if (separatorCopy[LAST_INDEX] === match.index) separatorCopy[LAST_INDEX]++; // Avoid an infinite loop
	      }

	      if (lastLastIndex === string[LENGTH]) {
	        if (lastLength || !separatorCopy.test('')) output.push('');
	      } else output.push(string.slice(lastLastIndex));

	      return output[LENGTH] > splitLimit ? output.slice(0, splitLimit) : output;
	    }; // Chakra, V8

	  } else if ('0'[$SPLIT](undefined, 0)[LENGTH]) {
	    $split = function $split(separator, limit) {
	      return separator === undefined && limit === 0 ? [] : _split.call(this, separator, limit);
	    };
	  } // 21.1.3.17 String.prototype.split(separator, limit)


	  return [function split(separator, limit) {
	    var O = defined(this);
	    var fn = separator == undefined ? undefined : separator[SPLIT];
	    return fn !== undefined ? fn.call(separator, O, limit) : $split.call(String(O), separator, limit);
	  }, $split];
	});

	// fallback for non-array-like ES3 and non-enumerable old V8 strings
	var cof = require('./_cof'); // eslint-disable-next-line no-prototype-builtins


	module.exports = Object('z').propertyIsEnumerable(0) ? Object : function (it) {
	  return cof(it) == 'String' ? it.split('') : Object(it);
	};

	var _iobject = /*#__PURE__*/Object.freeze({

	});

	// 7.2.1 RequireObjectCoercible(argument)
	var _defined = function (it) {
	  if (it == undefined) throw TypeError("Can't call method on  " + it);
	  return it;
	};

	// to indexed object, toObject with fallback for non-array-like ES3 strings




	var _toIobject = function (it) {
	  return _iobject(_defined(it));
	};

	// 7.1.4 ToInteger
	var ceil = Math.ceil;
	var floor = Math.floor;

	var _toInteger = function (it) {
	  return isNaN(it = +it) ? 0 : (it > 0 ? floor : ceil)(it);
	};

	// 7.1.15 ToLength


	var min = Math.min;

	var _toLength = function (it) {
	  return it > 0 ? min(_toInteger(it), 0x1fffffffffffff) : 0; // pow(2, 53) - 1 == 9007199254740991
	};

	var max = Math.max;
	var min$1 = Math.min;

	var _toAbsoluteIndex = function (index, length) {
	  index = _toInteger(index);
	  return index < 0 ? max(index + length, 0) : min$1(index, length);
	};

	// false -> Array#indexOf
	// true  -> Array#includes






	var _arrayIncludes = function (IS_INCLUDES) {
	  return function ($this, el, fromIndex) {
	    var O = _toIobject($this);
	    var length = _toLength(O.length);
	    var index = _toAbsoluteIndex(fromIndex, length);
	    var value; // Array#includes uses SameValueZero equality algorithm
	    // eslint-disable-next-line no-self-compare

	    if (IS_INCLUDES && el != el) while (length > index) {
	      value = O[index++]; // eslint-disable-next-line no-self-compare

	      if (value != value) return true; // Array#indexOf ignores holes, Array#includes - not
	    } else for (; length > index; index++) {
	      if (IS_INCLUDES || index in O) {
	        if (O[index] === el) return IS_INCLUDES || index || 0;
	      }
	    }
	    return !IS_INCLUDES && -1;
	  };
	};

	var _core = createCommonjsModule(function (module) {
	var core = module.exports = {
	  version: '2.5.7'
	};
	if (typeof __e == 'number') __e = core; // eslint-disable-line no-undef
	});
	var _core_1 = _core.version;

	var _library = false;

	var _shared = createCommonjsModule(function (module) {
	var SHARED = '__core-js_shared__';
	var store = _global[SHARED] || (_global[SHARED] = {});
	(module.exports = function (key, value) {
	  return store[key] || (store[key] = value !== undefined ? value : {});
	})('versions', []).push({
	  version: _core.version,
	  mode: 'global',
	  copyright: '© 2018 Denis Pushkarev (zloirock.ru)'
	});
	});

	// 21.2.5.3 get RegExp.prototype.flags()
	if (require('./_descriptors') && /./g.flags != 'g') require('./_object-dp').f(RegExp.prototype, 'flags', {
	  configurable: true,
	  get: require('./_flags')
	});

	var DateProto = Date.prototype;
	var INVALID_DATE = 'Invalid Date';
	var TO_STRING = 'toString';
	var $toString = DateProto[TO_STRING];
	var getTime = DateProto.getTime;

	if (new Date(NaN) + '' != INVALID_DATE) {
	  require('./_redefine')(DateProto, TO_STRING, function toString() {
	    var value = getTime.call(this); // eslint-disable-next-line no-self-compare

	    return value === value ? $toString.call(this) : INVALID_DATE;
	  });
	}

	require('./es6.regexp.flags');

	var anObject$2 = require('./_an-object');

	var $flags = require('./_flags');

	var DESCRIPTORS$1 = require('./_descriptors');

	var TO_STRING$1 = 'toString';
	var $toString$1 = /./[TO_STRING$1];

	var define$1 = function define(fn) {
	  require('./_redefine')(RegExp.prototype, TO_STRING$1, fn, true);
	}; // 21.2.5.14 RegExp.prototype.toString()


	if (require('./_fails')(function () {
	  return $toString$1.call({
	    source: 'a',
	    flags: 'b'
	  }) != '/a/b';
	})) {
	  define$1(function toString() {
	    var R = anObject$2(this);
	    return '/'.concat(R.source, '/', 'flags' in R ? R.flags : !DESCRIPTORS$1 && R instanceof RegExp ? $flags.call(R) : undefined);
	  }); // FF44- RegExp#toString has a wrong name
	} else if ($toString$1.name != TO_STRING$1) {
	  define$1(function toString() {
	    return $toString$1.call(this);
	  });
	}

	var id$1 = 0;
	var px = Math.random();

	module.exports = function (key) {
	  return 'Symbol('.concat(key === undefined ? '' : key, ')_', (++id$1 + px).toString(36));
	};

	var _uid = /*#__PURE__*/Object.freeze({

	});

	var shared = _shared('keys');



	var _sharedKey = function (key) {
	  return shared[key] || (shared[key] = _uid(key));
	};

	var arrayIndexOf = _arrayIncludes(false);

	var IE_PROTO = _sharedKey('IE_PROTO');

	var _objectKeysInternal = function (object, names) {
	  var O = _toIobject(object);
	  var i = 0;
	  var result = [];
	  var key;

	  for (key in O) {
	    if (key != IE_PROTO) _has(O, key) && result.push(key);
	  } // Don't enum bug & hidden keys


	  while (names.length > i) {
	    if (_has(O, key = names[i++])) {
	      ~arrayIndexOf(result, key) || result.push(key);
	    }
	  }

	  return result;
	};

	// IE 8- don't enum bug keys
	module.exports = 'constructor,hasOwnProperty,isPrototypeOf,propertyIsEnumerable,toLocaleString,toString,valueOf'.split(',');

	var _enumBugKeys = /*#__PURE__*/Object.freeze({

	});

	// 19.1.2.7 / 15.2.3.4 Object.getOwnPropertyNames(O)


	var hiddenKeys = _enumBugKeys.concat('length', 'prototype');

	var f = Object.getOwnPropertyNames || function getOwnPropertyNames(O) {
	  return _objectKeysInternal(O, hiddenKeys);
	};

	var _objectGopn = {
		f: f
	};

	var toString = {}.toString;

	module.exports = function (it) {
	  return toString.call(it).slice(8, -1);
	};

	var cof$1 = /*#__PURE__*/Object.freeze({

	});

	// 7.2.8 IsRegExp(argument)




	var MATCH = require$$0$2('match');

	var _isRegexp = function (it) {
	  var isRegExp;
	  return isObject(it) && ((isRegExp = it[MATCH]) !== undefined ? !!isRegExp : cof$1(it) == 'RegExp');
	};

	var _anObject = function (it) {
	  if (!isObject(it)) throw TypeError(it + ' is not an object!');
	  return it;
	};

	var _flags = function () {
	  var that = _anObject(this);
	  var result = '';
	  if (that.global) result += 'g';
	  if (that.ignoreCase) result += 'i';
	  if (that.multiline) result += 'm';
	  if (that.unicode) result += 'u';
	  if (that.sticky) result += 'y';
	  return result;
	};

	var _fails = function (exec) {
	  try {
	    return !!exec();
	  } catch (e) {
	    return true;
	  }
	};

	var global$2 = require('./_global');

	var hide$2 = require('./_hide');

	var has = require('./_has');

	var SRC = require('./_uid')('src');

	var TO_STRING$2 = 'toString';
	var $toString$2 = Function[TO_STRING$2];
	var TPL = ('' + $toString$2).split(TO_STRING$2);

	require('./_core').inspectSource = function (it) {
	  return $toString$2.call(it);
	};

	(module.exports = function (O, key, val, safe) {
	  var isFunction = typeof val == 'function';
	  if (isFunction) has(val, 'name') || hide$2(val, 'name', key);
	  if (O[key] === val) return;
	  if (isFunction) has(val, SRC) || hide$2(val, SRC, O[key] ? '' + O[key] : TPL.join(String(key)));

	  if (O === global$2) {
	    O[key] = val;
	  } else if (!safe) {
	    delete O[key];
	    hide$2(O, key, val);
	  } else if (O[key]) {
	    O[key] = val;
	  } else {
	    hide$2(O, key, val);
	  } // add fake Function#toString for correct work wrapped methods / constructors with methods like LoDash isNative

	})(Function.prototype, TO_STRING$2, function toString() {
	  return typeof this == 'function' && this[SRC] || $toString$2.call(this);
	});

	var redefine$2 = /*#__PURE__*/Object.freeze({

	});

	var SPECIES = require$$0$2('species');

	var _setSpecies = function (KEY) {
	  var C = _global[KEY];
	  if (DESCRIPTORS && C && !C[SPECIES]) require$$0$1.f(C, SPECIES, {
	    configurable: true,
	    get: function get() {
	      return this;
	    }
	  });
	};

	var dP$1 = require$$0$1.f;

	var gOPN = _objectGopn.f;





	var $RegExp = _global.RegExp;
	var Base = $RegExp;
	var proto = $RegExp.prototype;
	var re1 = /a/g;
	var re2 = /a/g; // "new" creates a new object, old webkit buggy here

	var CORRECT_NEW = new $RegExp(re1) !== re1;

	if (DESCRIPTORS && (!CORRECT_NEW || _fails(function () {
	  re2[require$$0$2('match')] = false; // RegExp constructor can alter flags and IsRegExp works correct with @@match

	  return $RegExp(re1) != re1 || $RegExp(re2) == re2 || $RegExp(re1, 'i') != '/a/i';
	}))) {
	  $RegExp = function RegExp(p, f) {
	    var tiRE = this instanceof $RegExp;
	    var piRE = _isRegexp(p);
	    var fiU = f === undefined;
	    return !tiRE && piRE && p.constructor === $RegExp && fiU ? p : _inheritIfRequired(CORRECT_NEW ? new Base(piRE && !fiU ? p.source : p, f) : Base((piRE = p instanceof $RegExp) ? p.source : p, piRE && fiU ? _flags.call(p) : f), tiRE ? this : proto, $RegExp);
	  };

	  var proxy = function proxy(key) {
	    key in $RegExp || dP$1($RegExp, key, {
	      configurable: true,
	      get: function get() {
	        return Base[key];
	      },
	      set: function set(it) {
	        Base[key] = it;
	      }
	    });
	  };

	  for (var keys = gOPN(Base), i = 0; keys.length > i;) {
	    proxy(keys[i++]);
	  }

	  proto.constructor = $RegExp;
	  $RegExp.prototype = proto;

	  redefine$2(_global, 'RegExp', $RegExp);
	}

	_setSpecies('RegExp');

	// @@match logic
	require('./_fix-re-wks')('match', 1, function (defined, MATCH, $match) {
	  // 21.1.3.11 String.prototype.match(regexp)
	  return [function match(regexp) {

	    var O = defined(this);
	    var fn = regexp == undefined ? undefined : regexp[MATCH];
	    return fn !== undefined ? fn.call(regexp, O) : new RegExp(regexp)[MATCH](String(O));
	  }, $match];
	});

	var dP$2 = require('./_object-dp').f;

	var FProto = Function.prototype;
	var nameRE = /^\s*function ([^ (]*)/;
	var NAME = 'name'; // 19.2.4.2 name

	NAME in FProto || require('./_descriptors') && dP$2(FProto, NAME, {
	  configurable: true,
	  get: function get() {
	    try {
	      return ('' + this).match(nameRE)[1];
	    } catch (e) {
	      return '';
	    }
	  }
	});

	var store = require('./_shared')('wks');

	var uid = require('./_uid');

	var _Symbol = require('./_global').Symbol;

	var USE_SYMBOL = typeof _Symbol == 'function';

	var $exports = module.exports = function (name) {
	  return store[name] || (store[name] = USE_SYMBOL && _Symbol[name] || (USE_SYMBOL ? _Symbol : uid)('Symbol.' + name));
	};

	$exports.store = store;

	var _propertyDesc = function (bitmap, value) {
	  return {
	    enumerable: !(bitmap & 1),
	    configurable: !(bitmap & 2),
	    writable: !(bitmap & 4),
	    value: value
	  };
	};

	var _hide = DESCRIPTORS ? function (object, key, value) {
	  return require$$0$1.f(object, key, _propertyDesc(1, value));
	} : function (object, key, value) {
	  object[key] = value;
	  return object;
	};

	// 22.1.3.31 Array.prototype[@@unscopables]
	var UNSCOPABLES = require$$0$2('unscopables');

	var ArrayProto = Array.prototype;
	if (ArrayProto[UNSCOPABLES] == undefined) _hide(ArrayProto, UNSCOPABLES, {});

	var _addToUnscopables = function (key) {
	  ArrayProto[UNSCOPABLES][key] = true;
	};

	var _iterStep = function (done, value) {
	  return {
	    value: value,
	    done: !!done
	  };
	};

	var _iterators = {};

	var LIBRARY = require('./_library');

	var $export$2 = require('./_export');

	var redefine$3 = require('./_redefine');

	var hide$3 = require('./_hide');

	var Iterators = require('./_iterators');

	var $iterCreate = require('./_iter-create');

	var setToStringTag = require('./_set-to-string-tag');

	var getPrototypeOf = require('./_object-gpo');

	var ITERATOR = require('./_wks')('iterator');

	var BUGGY = !([].keys && 'next' in [].keys()); // Safari has buggy iterators w/o `next`

	var FF_ITERATOR = '@@iterator';
	var KEYS = 'keys';
	var VALUES = 'values';

	var returnThis = function returnThis() {
	  return this;
	};

	module.exports = function (Base, NAME, Constructor, next, DEFAULT, IS_SET, FORCED) {
	  $iterCreate(Constructor, NAME, next);

	  var getMethod = function getMethod(kind) {
	    if (!BUGGY && kind in proto) return proto[kind];

	    switch (kind) {
	      case KEYS:
	        return function keys() {
	          return new Constructor(this, kind);
	        };

	      case VALUES:
	        return function values() {
	          return new Constructor(this, kind);
	        };
	    }

	    return function entries() {
	      return new Constructor(this, kind);
	    };
	  };

	  var TAG = NAME + ' Iterator';
	  var DEF_VALUES = DEFAULT == VALUES;
	  var VALUES_BUG = false;
	  var proto = Base.prototype;
	  var $native = proto[ITERATOR] || proto[FF_ITERATOR] || DEFAULT && proto[DEFAULT];
	  var $default = $native || getMethod(DEFAULT);
	  var $entries = DEFAULT ? !DEF_VALUES ? $default : getMethod('entries') : undefined;
	  var $anyNative = NAME == 'Array' ? proto.entries || $native : $native;
	  var methods, key, IteratorPrototype; // Fix native

	  if ($anyNative) {
	    IteratorPrototype = getPrototypeOf($anyNative.call(new Base()));

	    if (IteratorPrototype !== Object.prototype && IteratorPrototype.next) {
	      // Set @@toStringTag to native iterators
	      setToStringTag(IteratorPrototype, TAG, true); // fix for some old engines

	      if (!LIBRARY && typeof IteratorPrototype[ITERATOR] != 'function') hide$3(IteratorPrototype, ITERATOR, returnThis);
	    }
	  } // fix Array#{values, @@iterator}.name in V8 / FF


	  if (DEF_VALUES && $native && $native.name !== VALUES) {
	    VALUES_BUG = true;

	    $default = function values() {
	      return $native.call(this);
	    };
	  } // Define iterator


	  if ((!LIBRARY || FORCED) && (BUGGY || VALUES_BUG || !proto[ITERATOR])) {
	    hide$3(proto, ITERATOR, $default);
	  } // Plug for library


	  Iterators[NAME] = $default;
	  Iterators[TAG] = returnThis;

	  if (DEFAULT) {
	    methods = {
	      values: DEF_VALUES ? $default : getMethod(VALUES),
	      keys: IS_SET ? $default : getMethod(KEYS),
	      entries: $entries
	    };
	    if (FORCED) for (key in methods) {
	      if (!(key in proto)) redefine$3(proto, key, methods[key]);
	    } else $export$2($export$2.P + $export$2.F * (BUGGY || VALUES_BUG), NAME, methods);
	  }

	  return methods;
	};

	var $iterDefine = /*#__PURE__*/Object.freeze({

	});

	// 22.1.3.4 Array.prototype.entries()
	// 22.1.3.13 Array.prototype.keys()
	// 22.1.3.29 Array.prototype.values()
	// 22.1.3.30 Array.prototype[@@iterator]()


	var es6_array_iterator = $iterDefine(Array, 'Array', function (iterated, kind) {
	  this._t = _toIobject(iterated); // target

	  this._i = 0; // next index

	  this._k = kind; // kind
	  // 22.1.5.2.1 %ArrayIteratorPrototype%.next()
	}, function () {
	  var O = this._t;
	  var kind = this._k;
	  var index = this._i++;

	  if (!O || index >= O.length) {
	    this._t = undefined;
	    return _iterStep(1);
	  }

	  if (kind == 'keys') return _iterStep(0, index);
	  if (kind == 'values') return _iterStep(0, O[index]);
	  return _iterStep(0, [index, O[index]]);
	}, 'values'); // argumentsList[@@iterator] is %ArrayProto_values% (9.4.4.6, 9.4.4.7)

	_iterators.Arguments = _iterators.Array;
	_addToUnscopables('keys');
	_addToUnscopables('values');
	_addToUnscopables('entries');

	// 7.1.13 ToObject(argument)


	var _toObject = function (it) {
	  return Object(_defined(it));
	};

	// most Object methods by ES6 should accept primitives






	var _objectSap = function (KEY, exec) {
	  var fn = (_core.Object || {})[KEY] || Object[KEY];
	  var exp = {};
	  exp[KEY] = exec(fn);
	  $export$1($export$1.S + $export$1.F * _fails(function () {
	    fn(1);
	  }), 'Object', exp);
	};

	// 19.1.2.14 Object.keys(O)




	_objectSap('keys', function () {
	  return function keys(it) {
	    return $keys$1(_toObject(it));
	  };
	});

	// 19.1.2.14 / 15.2.3.14 Object.keys(O)
	var $keys = require('./_object-keys-internal');

	var enumBugKeys = require('./_enum-bug-keys');

	module.exports = Object.keys || function keys(O) {
	  return $keys(O, enumBugKeys);
	};

	var ITERATOR$1 = require$$0$2('iterator');
	var TO_STRING_TAG = require$$0$2('toStringTag');
	var ArrayValues = _iterators.Array;
	var DOMIterables = {
	  CSSRuleList: true,
	  // TODO: Not spec compliant, should be false.
	  CSSStyleDeclaration: false,
	  CSSValueList: false,
	  ClientRectList: false,
	  DOMRectList: false,
	  DOMStringList: false,
	  DOMTokenList: true,
	  DataTransferItemList: false,
	  FileList: false,
	  HTMLAllCollection: false,
	  HTMLCollection: false,
	  HTMLFormElement: false,
	  HTMLSelectElement: false,
	  MediaList: true,
	  // TODO: Not spec compliant, should be false.
	  MimeTypeArray: false,
	  NamedNodeMap: false,
	  NodeList: true,
	  PaintRequestList: false,
	  Plugin: false,
	  PluginArray: false,
	  SVGLengthList: false,
	  SVGNumberList: false,
	  SVGPathSegList: false,
	  SVGPointList: false,
	  SVGStringList: false,
	  SVGTransformList: false,
	  SourceBufferList: false,
	  StyleSheetList: true,
	  // TODO: Not spec compliant, should be false.
	  TextTrackCueList: false,
	  TextTrackList: false,
	  TouchList: false
	};

	for (var collections = $keys$1(DOMIterables), i$1 = 0; i$1 < collections.length; i$1++) {
	  var NAME$1 = collections[i$1];
	  var explicit = DOMIterables[NAME$1];
	  var Collection = _global[NAME$1];
	  var proto$1 = Collection && Collection.prototype;
	  var key;

	  if (proto$1) {
	    if (!proto$1[ITERATOR$1]) _hide(proto$1, ITERATOR$1, ArrayValues);
	    if (!proto$1[TO_STRING_TAG]) _hide(proto$1, TO_STRING_TAG, NAME$1);
	    _iterators[NAME$1] = ArrayValues;
	    if (explicit) for (key in es6_array_iterator) {
	      if (!proto$1[key]) redefine$2(proto$1, key, es6_array_iterator[key], true);
	    }
	  }
	}

	var $export$3 = require('./_export');

	var $indexOf = require('./_array-includes')(false);

	var $native = [].indexOf;
	var NEGATIVE_ZERO = !!$native && 1 / [1].indexOf(1, -0) < 0;
	$export$3($export$3.P + $export$3.F * (NEGATIVE_ZERO || !require('./_strict-method')($native)), 'Array', {
	  // 22.1.3.11 / 15.4.4.14 Array.prototype.indexOf(searchElement [, fromIndex])
	  indexOf: function indexOf(searchElement
	  /* , fromIndex = 0 */
	  ) {
	    return NEGATIVE_ZERO // convert -0 to +0
	    ? $native.apply(this, arguments) || 0 : $indexOf(this, searchElement, arguments[1]);
	  }
	});

	var LIBRARY$1 = require('./_library');

	var global$3 = require('./_global');

	var ctx$1 = require('./_ctx');

	var classof = require('./_classof');

	var $export$4 = require('./_export');

	var isObject$2 = require('./_is-object');

	var aFunction = require('./_a-function');

	var anInstance = require('./_an-instance');

	var forOf = require('./_for-of');

	var speciesConstructor = require('./_species-constructor');

	var task = require('./_task').set;

	var microtask = require('./_microtask')();

	var newPromiseCapabilityModule = require('./_new-promise-capability');

	var perform = require('./_perform');

	var userAgent = require('./_user-agent');

	var promiseResolve = require('./_promise-resolve');

	var PROMISE = 'Promise';
	var TypeError$1 = global$3.TypeError;
	var process = global$3.process;
	var versions = process && process.versions;
	var v8 = versions && versions.v8 || '';
	var $Promise = global$3[PROMISE];
	var isNode = classof(process) == 'process';

	var empty = function empty() {
	  /* empty */
	};

	var Internal, newGenericPromiseCapability, OwnPromiseCapability, Wrapper;
	var newPromiseCapability = newGenericPromiseCapability = newPromiseCapabilityModule.f;
	var USE_NATIVE = !!function () {
	  try {
	    // correct subclassing with @@species support
	    var promise = $Promise.resolve(1);

	    var FakePromise = (promise.constructor = {})[require('./_wks')('species')] = function (exec) {
	      exec(empty, empty);
	    }; // unhandled rejections tracking support, NodeJS Promise without it fails @@species test


	    return (isNode || typeof PromiseRejectionEvent == 'function') && promise.then(empty) instanceof FakePromise // v8 6.6 (Node 10 and Chrome 66) have a bug with resolving custom thenables
	    // https://bugs.chromium.org/p/chromium/issues/detail?id=830565
	    // we can't detect it synchronously, so just check versions
	    && v8.indexOf('6.6') !== 0 && userAgent.indexOf('Chrome/66') === -1;
	  } catch (e) {
	    /* empty */
	  }
	}(); // helpers

	var isThenable = function isThenable(it) {
	  var then;
	  return isObject$2(it) && typeof (then = it.then) == 'function' ? then : false;
	};

	var notify = function notify(promise, isReject) {
	  if (promise._n) return;
	  promise._n = true;
	  var chain = promise._c;
	  microtask(function () {
	    var value = promise._v;
	    var ok = promise._s == 1;
	    var i = 0;

	    var run = function run(reaction) {
	      var handler = ok ? reaction.ok : reaction.fail;
	      var resolve = reaction.resolve;
	      var reject = reaction.reject;
	      var domain = reaction.domain;
	      var result, then, exited;

	      try {
	        if (handler) {
	          if (!ok) {
	            if (promise._h == 2) onHandleUnhandled(promise);
	            promise._h = 1;
	          }

	          if (handler === true) result = value;else {
	            if (domain) domain.enter();
	            result = handler(value); // may throw

	            if (domain) {
	              domain.exit();
	              exited = true;
	            }
	          }

	          if (result === reaction.promise) {
	            reject(TypeError$1('Promise-chain cycle'));
	          } else if (then = isThenable(result)) {
	            then.call(result, resolve, reject);
	          } else resolve(result);
	        } else reject(value);
	      } catch (e) {
	        if (domain && !exited) domain.exit();
	        reject(e);
	      }
	    };

	    while (chain.length > i) {
	      run(chain[i++]);
	    } // variable length - can't use forEach


	    promise._c = [];
	    promise._n = false;
	    if (isReject && !promise._h) onUnhandled(promise);
	  });
	};

	var onUnhandled = function onUnhandled(promise) {
	  task.call(global$3, function () {
	    var value = promise._v;
	    var unhandled = isUnhandled(promise);
	    var result, handler, console;

	    if (unhandled) {
	      result = perform(function () {
	        if (isNode) {
	          process.emit('unhandledRejection', value, promise);
	        } else if (handler = global$3.onunhandledrejection) {
	          handler({
	            promise: promise,
	            reason: value
	          });
	        } else if ((console = global$3.console) && console.error) {
	          console.error('Unhandled promise rejection', value);
	        }
	      }); // Browsers should not trigger `rejectionHandled` event if it was handled here, NodeJS - should

	      promise._h = isNode || isUnhandled(promise) ? 2 : 1;
	    }

	    promise._a = undefined;
	    if (unhandled && result.e) throw result.v;
	  });
	};

	var isUnhandled = function isUnhandled(promise) {
	  return promise._h !== 1 && (promise._a || promise._c).length === 0;
	};

	var onHandleUnhandled = function onHandleUnhandled(promise) {
	  task.call(global$3, function () {
	    var handler;

	    if (isNode) {
	      process.emit('rejectionHandled', promise);
	    } else if (handler = global$3.onrejectionhandled) {
	      handler({
	        promise: promise,
	        reason: promise._v
	      });
	    }
	  });
	};

	var $reject = function $reject(value) {
	  var promise = this;
	  if (promise._d) return;
	  promise._d = true;
	  promise = promise._w || promise; // unwrap

	  promise._v = value;
	  promise._s = 2;
	  if (!promise._a) promise._a = promise._c.slice();
	  notify(promise, true);
	};

	var $resolve = function $resolve(value) {
	  var promise = this;
	  var then;
	  if (promise._d) return;
	  promise._d = true;
	  promise = promise._w || promise; // unwrap

	  try {
	    if (promise === value) throw TypeError$1("Promise can't be resolved itself");

	    if (then = isThenable(value)) {
	      microtask(function () {
	        var wrapper = {
	          _w: promise,
	          _d: false
	        }; // wrap

	        try {
	          then.call(value, ctx$1($resolve, wrapper, 1), ctx$1($reject, wrapper, 1));
	        } catch (e) {
	          $reject.call(wrapper, e);
	        }
	      });
	    } else {
	      promise._v = value;
	      promise._s = 1;
	      notify(promise, false);
	    }
	  } catch (e) {
	    $reject.call({
	      _w: promise,
	      _d: false
	    }, e); // wrap
	  }
	}; // constructor polyfill


	if (!USE_NATIVE) {
	  // 25.4.3.1 Promise(executor)
	  $Promise = function Promise(executor) {
	    anInstance(this, $Promise, PROMISE, '_h');
	    aFunction(executor);
	    Internal.call(this);

	    try {
	      executor(ctx$1($resolve, this, 1), ctx$1($reject, this, 1));
	    } catch (err) {
	      $reject.call(this, err);
	    }
	  }; // eslint-disable-next-line no-unused-vars


	  Internal = function Promise(executor) {
	    this._c = []; // <- awaiting reactions

	    this._a = undefined; // <- checked in isUnhandled reactions

	    this._s = 0; // <- state

	    this._d = false; // <- done

	    this._v = undefined; // <- value

	    this._h = 0; // <- rejection state, 0 - default, 1 - handled, 2 - unhandled

	    this._n = false; // <- notify
	  };

	  Internal.prototype = require('./_redefine-all')($Promise.prototype, {
	    // 25.4.5.3 Promise.prototype.then(onFulfilled, onRejected)
	    then: function then(onFulfilled, onRejected) {
	      var reaction = newPromiseCapability(speciesConstructor(this, $Promise));
	      reaction.ok = typeof onFulfilled == 'function' ? onFulfilled : true;
	      reaction.fail = typeof onRejected == 'function' && onRejected;
	      reaction.domain = isNode ? process.domain : undefined;

	      this._c.push(reaction);

	      if (this._a) this._a.push(reaction);
	      if (this._s) notify(this, false);
	      return reaction.promise;
	    },
	    // 25.4.5.1 Promise.prototype.catch(onRejected)
	    'catch': function _catch(onRejected) {
	      return this.then(undefined, onRejected);
	    }
	  });

	  OwnPromiseCapability = function OwnPromiseCapability() {
	    var promise = new Internal();
	    this.promise = promise;
	    this.resolve = ctx$1($resolve, promise, 1);
	    this.reject = ctx$1($reject, promise, 1);
	  };

	  newPromiseCapabilityModule.f = newPromiseCapability = function newPromiseCapability(C) {
	    return C === $Promise || C === Wrapper ? new OwnPromiseCapability(C) : newGenericPromiseCapability(C);
	  };
	}

	$export$4($export$4.G + $export$4.W + $export$4.F * !USE_NATIVE, {
	  Promise: $Promise
	});

	require('./_set-to-string-tag')($Promise, PROMISE);

	require('./_set-species')(PROMISE);

	Wrapper = require('./_core')[PROMISE]; // statics

	$export$4($export$4.S + $export$4.F * !USE_NATIVE, PROMISE, {
	  // 25.4.4.5 Promise.reject(r)
	  reject: function reject(r) {
	    var capability = newPromiseCapability(this);
	    var $$reject = capability.reject;
	    $$reject(r);
	    return capability.promise;
	  }
	});
	$export$4($export$4.S + $export$4.F * (LIBRARY$1 || !USE_NATIVE), PROMISE, {
	  // 25.4.4.6 Promise.resolve(x)
	  resolve: function resolve(x) {
	    return promiseResolve(LIBRARY$1 && this === Wrapper ? $Promise : this, x);
	  }
	});
	$export$4($export$4.S + $export$4.F * !(USE_NATIVE && require('./_iter-detect')(function (iter) {
	  $Promise.all(iter)['catch'](empty);
	})), PROMISE, {
	  // 25.4.4.1 Promise.all(iterable)
	  all: function all(iterable) {
	    var C = this;
	    var capability = newPromiseCapability(C);
	    var resolve = capability.resolve;
	    var reject = capability.reject;
	    var result = perform(function () {
	      var values = [];
	      var index = 0;
	      var remaining = 1;
	      forOf(iterable, false, function (promise) {
	        var $index = index++;
	        var alreadyCalled = false;
	        values.push(undefined);
	        remaining++;
	        C.resolve(promise).then(function (value) {
	          if (alreadyCalled) return;
	          alreadyCalled = true;
	          values[$index] = value;
	          --remaining || resolve(values);
	        }, reject);
	      });
	      --remaining || resolve(values);
	    });
	    if (result.e) reject(result.v);
	    return capability.promise;
	  },
	  // 25.4.4.4 Promise.race(iterable)
	  race: function race(iterable) {
	    var C = this;
	    var capability = newPromiseCapability(C);
	    var reject = capability.reject;
	    var result = perform(function () {
	      forOf(iterable, false, function (promise) {
	        C.resolve(promise).then(capability.resolve, reject);
	      });
	    });
	    if (result.e) reject(result.v);
	    return capability.promise;
	  }
	});

	var $export$5 = require('./_export');

	var $forEach = require('./_array-methods')(0);

	var STRICT = require('./_strict-method')([].forEach, true);

	$export$5($export$5.P + $export$5.F * !STRICT, 'Array', {
	  // 22.1.3.10 / 15.4.4.18 Array.prototype.forEach(callbackfn [, thisArg])
	  forEach: function forEach(callbackfn
	  /* , thisArg */
	  ) {
	    return $forEach(this, callbackfn, arguments[1]);
	  }
	});

	// 19.1.2.2 / 15.2.3.5 Object.create(O [, Properties])
	var anObject$3 = require('./_an-object');

	var dPs = require('./_object-dps');

	var enumBugKeys$1 = require('./_enum-bug-keys');

	var IE_PROTO$1 = require('./_shared-key')('IE_PROTO');

	var Empty = function Empty() {
	  /* empty */
	};

	var PROTOTYPE$1 = 'prototype'; // Create object with fake `null` prototype: use iframe Object with cleared prototype

	var _createDict = function createDict() {
	  // Thrash, waste and sodomy: IE GC bug
	  var iframe = require('./_dom-create')('iframe');

	  var i = enumBugKeys$1.length;
	  var lt = '<';
	  var gt = '>';
	  var iframeDocument;
	  iframe.style.display = 'none';

	  require('./_html').appendChild(iframe);

	  iframe.src = 'javascript:'; // eslint-disable-line no-script-url
	  // createDict = iframe.contentWindow.Object;
	  // html.removeChild(iframe);

	  iframeDocument = iframe.contentWindow.document;
	  iframeDocument.open();
	  iframeDocument.write(lt + 'script' + gt + 'document.F=Object' + lt + '/script' + gt);
	  iframeDocument.close();
	  _createDict = iframeDocument.F;

	  while (i--) {
	    delete _createDict[PROTOTYPE$1][enumBugKeys$1[i]];
	  }

	  return _createDict();
	};

	module.exports = Object.create || function create(O, Properties) {
	  var result;

	  if (O !== null) {
	    Empty[PROTOTYPE$1] = anObject$3(O);
	    result = new Empty();
	    Empty[PROTOTYPE$1] = null; // add "__proto__" for Object.getPrototypeOf polyfill

	    result[IE_PROTO$1] = O;
	  } else result = _createDict();

	  return Properties === undefined ? result : dPs(result, Properties);
	};

	var create = /*#__PURE__*/Object.freeze({

	});

	// 19.1.2.2 / 15.2.3.5 Object.create(O [, Properties])


	$export$1($export$1.S, 'Object', {
	  create: create
	});

	var f$1 = require$$0$2;

	var _wksExt = {
		f: f$1
	};

	var defineProperty = require$$0$1.f;

	var _wksDefine = function (name) {
	  var $Symbol = _core.Symbol || (_core.Symbol = _library ? {} : _global.Symbol || {});
	  if (name.charAt(0) != '_' && !(name in $Symbol)) defineProperty($Symbol, name, {
	    value: _wksExt.f(name)
	  });
	};

	_wksDefine('asyncIterator');

	var global$4 = require('./_global');

	var has$1 = require('./_has');

	var DESCRIPTORS$2 = require('./_descriptors');

	var $export$6 = require('./_export');

	var redefine$4 = require('./_redefine');

	var META = require('./_meta').KEY;

	var $fails = require('./_fails');

	var shared$1 = require('./_shared');

	var setToStringTag$1 = require('./_set-to-string-tag');

	var uid$1 = require('./_uid');

	var wks$1 = require('./_wks');

	var wksExt = require('./_wks-ext');

	var wksDefine = require('./_wks-define');

	var enumKeys = require('./_enum-keys');

	var isArray = require('./_is-array');

	var anObject$4 = require('./_an-object');

	var isObject$3 = require('./_is-object');

	var toIObject = require('./_to-iobject');

	var toPrimitive$1 = require('./_to-primitive');

	var createDesc = require('./_property-desc');

	var _create = require('./_object-create');

	var gOPNExt = require('./_object-gopn-ext');

	var $GOPD = require('./_object-gopd');

	var $DP = require('./_object-dp');

	var $keys$2 = require('./_object-keys');

	var gOPD = $GOPD.f;
	var dP$3 = $DP.f;
	var gOPN$1 = gOPNExt.f;
	var $Symbol = global$4.Symbol;
	var $JSON = global$4.JSON;

	var _stringify = $JSON && $JSON.stringify;

	var PROTOTYPE$2 = 'prototype';
	var HIDDEN = wks$1('_hidden');
	var TO_PRIMITIVE = wks$1('toPrimitive');
	var isEnum = {}.propertyIsEnumerable;
	var SymbolRegistry = shared$1('symbol-registry');
	var AllSymbols = shared$1('symbols');
	var OPSymbols = shared$1('op-symbols');
	var ObjectProto = Object[PROTOTYPE$2];
	var USE_NATIVE$1 = typeof $Symbol == 'function';
	var QObject = global$4.QObject; // Don't use setters in Qt Script, https://github.com/zloirock/core-js/issues/173

	var setter = !QObject || !QObject[PROTOTYPE$2] || !QObject[PROTOTYPE$2].findChild; // fallback for old Android, https://code.google.com/p/v8/issues/detail?id=687

	var setSymbolDesc = DESCRIPTORS$2 && $fails(function () {
	  return _create(dP$3({}, 'a', {
	    get: function get() {
	      return dP$3(this, 'a', {
	        value: 7
	      }).a;
	    }
	  })).a != 7;
	}) ? function (it, key, D) {
	  var protoDesc = gOPD(ObjectProto, key);
	  if (protoDesc) delete ObjectProto[key];
	  dP$3(it, key, D);
	  if (protoDesc && it !== ObjectProto) dP$3(ObjectProto, key, protoDesc);
	} : dP$3;

	var wrap = function wrap(tag) {
	  var sym = AllSymbols[tag] = _create($Symbol[PROTOTYPE$2]);

	  sym._k = tag;
	  return sym;
	};

	var isSymbol = USE_NATIVE$1 && _typeof($Symbol.iterator) == 'symbol' ? function (it) {
	  return _typeof(it) == 'symbol';
	} : function (it) {
	  return it instanceof $Symbol;
	};

	var $defineProperty = function defineProperty(it, key, D) {
	  if (it === ObjectProto) $defineProperty(OPSymbols, key, D);
	  anObject$4(it);
	  key = toPrimitive$1(key, true);
	  anObject$4(D);

	  if (has$1(AllSymbols, key)) {
	    if (!D.enumerable) {
	      if (!has$1(it, HIDDEN)) dP$3(it, HIDDEN, createDesc(1, {}));
	      it[HIDDEN][key] = true;
	    } else {
	      if (has$1(it, HIDDEN) && it[HIDDEN][key]) it[HIDDEN][key] = false;
	      D = _create(D, {
	        enumerable: createDesc(0, false)
	      });
	    }

	    return setSymbolDesc(it, key, D);
	  }

	  return dP$3(it, key, D);
	};

	var $defineProperties = function defineProperties(it, P) {
	  anObject$4(it);
	  var keys = enumKeys(P = toIObject(P));
	  var i = 0;
	  var l = keys.length;
	  var key;

	  while (l > i) {
	    $defineProperty(it, key = keys[i++], P[key]);
	  }

	  return it;
	};

	var $create = function create(it, P) {
	  return P === undefined ? _create(it) : $defineProperties(_create(it), P);
	};

	var $propertyIsEnumerable = function propertyIsEnumerable(key) {
	  var E = isEnum.call(this, key = toPrimitive$1(key, true));
	  if (this === ObjectProto && has$1(AllSymbols, key) && !has$1(OPSymbols, key)) return false;
	  return E || !has$1(this, key) || !has$1(AllSymbols, key) || has$1(this, HIDDEN) && this[HIDDEN][key] ? E : true;
	};

	var $getOwnPropertyDescriptor = function getOwnPropertyDescriptor(it, key) {
	  it = toIObject(it);
	  key = toPrimitive$1(key, true);
	  if (it === ObjectProto && has$1(AllSymbols, key) && !has$1(OPSymbols, key)) return;
	  var D = gOPD(it, key);
	  if (D && has$1(AllSymbols, key) && !(has$1(it, HIDDEN) && it[HIDDEN][key])) D.enumerable = true;
	  return D;
	};

	var $getOwnPropertyNames = function getOwnPropertyNames(it) {
	  var names = gOPN$1(toIObject(it));
	  var result = [];
	  var i = 0;
	  var key;

	  while (names.length > i) {
	    if (!has$1(AllSymbols, key = names[i++]) && key != HIDDEN && key != META) result.push(key);
	  }

	  return result;
	};

	var $getOwnPropertySymbols = function getOwnPropertySymbols(it) {
	  var IS_OP = it === ObjectProto;
	  var names = gOPN$1(IS_OP ? OPSymbols : toIObject(it));
	  var result = [];
	  var i = 0;
	  var key;

	  while (names.length > i) {
	    if (has$1(AllSymbols, key = names[i++]) && (IS_OP ? has$1(ObjectProto, key) : true)) result.push(AllSymbols[key]);
	  }

	  return result;
	}; // 19.4.1.1 Symbol([description])


	if (!USE_NATIVE$1) {
	  $Symbol = function _Symbol() {
	    if (this instanceof $Symbol) throw TypeError('Symbol is not a constructor!');
	    var tag = uid$1(arguments.length > 0 ? arguments[0] : undefined);

	    var $set = function $set(value) {
	      if (this === ObjectProto) $set.call(OPSymbols, value);
	      if (has$1(this, HIDDEN) && has$1(this[HIDDEN], tag)) this[HIDDEN][tag] = false;
	      setSymbolDesc(this, tag, createDesc(1, value));
	    };

	    if (DESCRIPTORS$2 && setter) setSymbolDesc(ObjectProto, tag, {
	      configurable: true,
	      set: $set
	    });
	    return wrap(tag);
	  };

	  redefine$4($Symbol[PROTOTYPE$2], 'toString', function toString() {
	    return this._k;
	  });
	  $GOPD.f = $getOwnPropertyDescriptor;
	  $DP.f = $defineProperty;
	  require('./_object-gopn').f = gOPNExt.f = $getOwnPropertyNames;
	  require('./_object-pie').f = $propertyIsEnumerable;
	  require('./_object-gops').f = $getOwnPropertySymbols;

	  if (DESCRIPTORS$2 && !require('./_library')) {
	    redefine$4(ObjectProto, 'propertyIsEnumerable', $propertyIsEnumerable, true);
	  }

	  wksExt.f = function (name) {
	    return wrap(wks$1(name));
	  };
	}

	$export$6($export$6.G + $export$6.W + $export$6.F * !USE_NATIVE$1, {
	  Symbol: $Symbol
	});

	for (var es6Symbols = // 19.4.2.2, 19.4.2.3, 19.4.2.4, 19.4.2.6, 19.4.2.8, 19.4.2.9, 19.4.2.10, 19.4.2.11, 19.4.2.12, 19.4.2.13, 19.4.2.14
	'hasInstance,isConcatSpreadable,iterator,match,replace,search,species,split,toPrimitive,toStringTag,unscopables'.split(','), j = 0; es6Symbols.length > j;) {
	  wks$1(es6Symbols[j++]);
	}

	for (var wellKnownSymbols = $keys$2(wks$1.store), k = 0; wellKnownSymbols.length > k;) {
	  wksDefine(wellKnownSymbols[k++]);
	}

	$export$6($export$6.S + $export$6.F * !USE_NATIVE$1, 'Symbol', {
	  // 19.4.2.1 Symbol.for(key)
	  'for': function _for(key) {
	    return has$1(SymbolRegistry, key += '') ? SymbolRegistry[key] : SymbolRegistry[key] = $Symbol(key);
	  },
	  // 19.4.2.5 Symbol.keyFor(sym)
	  keyFor: function keyFor(sym) {
	    if (!isSymbol(sym)) throw TypeError(sym + ' is not a symbol!');

	    for (var key in SymbolRegistry) {
	      if (SymbolRegistry[key] === sym) return key;
	    }
	  },
	  useSetter: function useSetter() {
	    setter = true;
	  },
	  useSimple: function useSimple() {
	    setter = false;
	  }
	});
	$export$6($export$6.S + $export$6.F * !USE_NATIVE$1, 'Object', {
	  // 19.1.2.2 Object.create(O [, Properties])
	  create: $create,
	  // 19.1.2.4 Object.defineProperty(O, P, Attributes)
	  defineProperty: $defineProperty,
	  // 19.1.2.3 Object.defineProperties(O, Properties)
	  defineProperties: $defineProperties,
	  // 19.1.2.6 Object.getOwnPropertyDescriptor(O, P)
	  getOwnPropertyDescriptor: $getOwnPropertyDescriptor,
	  // 19.1.2.7 Object.getOwnPropertyNames(O)
	  getOwnPropertyNames: $getOwnPropertyNames,
	  // 19.1.2.8 Object.getOwnPropertySymbols(O)
	  getOwnPropertySymbols: $getOwnPropertySymbols
	}); // 24.3.2 JSON.stringify(value [, replacer [, space]])

	$JSON && $export$6($export$6.S + $export$6.F * (!USE_NATIVE$1 || $fails(function () {
	  var S = $Symbol(); // MS Edge converts symbol values to JSON as {}
	  // WebKit converts symbol values to JSON as null
	  // V8 throws on boxed symbols

	  return _stringify([S]) != '[null]' || _stringify({
	    a: S
	  }) != '{}' || _stringify(Object(S)) != '{}';
	})), 'JSON', {
	  stringify: function stringify(it) {
	    var args = [it];
	    var i = 1;
	    var replacer, $replacer;

	    while (arguments.length > i) {
	      args.push(arguments[i++]);
	    }

	    $replacer = replacer = args[1];
	    if (!isObject$3(replacer) && it === undefined || isSymbol(it)) return; // IE8 returns string on undefined

	    if (!isArray(replacer)) replacer = function replacer(key, value) {
	      if (typeof $replacer == 'function') value = $replacer.call(this, key, value);
	      if (!isSymbol(value)) return value;
	    };
	    args[1] = replacer;
	    return _stringify.apply($JSON, args);
	  }
	}); // 19.4.3.4 Symbol.prototype[@@toPrimitive](hint)

	$Symbol[PROTOTYPE$2][TO_PRIMITIVE] || require('./_hide')($Symbol[PROTOTYPE$2], TO_PRIMITIVE, $Symbol[PROTOTYPE$2].valueOf); // 19.4.3.5 Symbol.prototype[@@toStringTag]

	setToStringTag$1($Symbol, 'Symbol'); // 20.2.1.9 Math[@@toStringTag]

	setToStringTag$1(Math, 'Math', true); // 24.3.3 JSON[@@toStringTag]

	setToStringTag$1(global$4.JSON, 'JSON', true);

	/**
	 * Copyright (c) 2014-present, Facebook, Inc.
	 *
	 * This source code is licensed under the MIT license found in the
	 * LICENSE file in the root directory of this source tree.
	 */
	!function (global) {

	  var Op = Object.prototype;
	  var hasOwn = Op.hasOwnProperty;
	  var undefined; // More compressible than void 0.

	  var $Symbol = typeof Symbol === "function" ? Symbol : {};
	  var iteratorSymbol = $Symbol.iterator || "@@iterator";
	  var asyncIteratorSymbol = $Symbol.asyncIterator || "@@asyncIterator";
	  var toStringTagSymbol = $Symbol.toStringTag || "@@toStringTag";
	  var inModule = (typeof module === "undefined" ? "undefined" : _typeof(module)) === "object";
	  var runtime = global.regeneratorRuntime;

	  if (runtime) {
	    if (inModule) {
	      // If regeneratorRuntime is defined globally and we're in a module,
	      // make the exports object identical to regeneratorRuntime.
	      module.exports = runtime;
	    } // Don't bother evaluating the rest of this file if the runtime was
	    // already defined globally.


	    return;
	  } // Define the runtime globally (as expected by generated code) as either
	  // module.exports (if we're in a module) or a new, empty object.


	  runtime = global.regeneratorRuntime = inModule ? module.exports : {};

	  function wrap(innerFn, outerFn, self, tryLocsList) {
	    // If outerFn provided and outerFn.prototype is a Generator, then outerFn.prototype instanceof Generator.
	    var protoGenerator = outerFn && outerFn.prototype instanceof Generator ? outerFn : Generator;
	    var generator = Object.create(protoGenerator.prototype);
	    var context = new Context(tryLocsList || []); // The ._invoke method unifies the implementations of the .next,
	    // .throw, and .return methods.

	    generator._invoke = makeInvokeMethod(innerFn, self, context);
	    return generator;
	  }

	  runtime.wrap = wrap; // Try/catch helper to minimize deoptimizations. Returns a completion
	  // record like context.tryEntries[i].completion. This interface could
	  // have been (and was previously) designed to take a closure to be
	  // invoked without arguments, but in all the cases we care about we
	  // already have an existing method we want to call, so there's no need
	  // to create a new function object. We can even get away with assuming
	  // the method takes exactly one argument, since that happens to be true
	  // in every case, so we don't have to touch the arguments object. The
	  // only additional allocation required is the completion record, which
	  // has a stable shape and so hopefully should be cheap to allocate.

	  function tryCatch(fn, obj, arg) {
	    try {
	      return {
	        type: "normal",
	        arg: fn.call(obj, arg)
	      };
	    } catch (err) {
	      return {
	        type: "throw",
	        arg: err
	      };
	    }
	  }

	  var GenStateSuspendedStart = "suspendedStart";
	  var GenStateSuspendedYield = "suspendedYield";
	  var GenStateExecuting = "executing";
	  var GenStateCompleted = "completed"; // Returning this object from the innerFn has the same effect as
	  // breaking out of the dispatch switch statement.

	  var ContinueSentinel = {}; // Dummy constructor functions that we use as the .constructor and
	  // .constructor.prototype properties for functions that return Generator
	  // objects. For full spec compliance, you may wish to configure your
	  // minifier not to mangle the names of these two functions.

	  function Generator() {}

	  function GeneratorFunction() {}

	  function GeneratorFunctionPrototype() {} // This is a polyfill for %IteratorPrototype% for environments that
	  // don't natively support it.


	  var IteratorPrototype = {};

	  IteratorPrototype[iteratorSymbol] = function () {
	    return this;
	  };

	  var getProto = Object.getPrototypeOf;
	  var NativeIteratorPrototype = getProto && getProto(getProto(values([])));

	  if (NativeIteratorPrototype && NativeIteratorPrototype !== Op && hasOwn.call(NativeIteratorPrototype, iteratorSymbol)) {
	    // This environment has a native %IteratorPrototype%; use it instead
	    // of the polyfill.
	    IteratorPrototype = NativeIteratorPrototype;
	  }

	  var Gp = GeneratorFunctionPrototype.prototype = Generator.prototype = Object.create(IteratorPrototype);
	  GeneratorFunction.prototype = Gp.constructor = GeneratorFunctionPrototype;
	  GeneratorFunctionPrototype.constructor = GeneratorFunction;
	  GeneratorFunctionPrototype[toStringTagSymbol] = GeneratorFunction.displayName = "GeneratorFunction"; // Helper for defining the .next, .throw, and .return methods of the
	  // Iterator interface in terms of a single ._invoke method.

	  function defineIteratorMethods(prototype) {
	    ["next", "throw", "return"].forEach(function (method) {
	      prototype[method] = function (arg) {
	        return this._invoke(method, arg);
	      };
	    });
	  }

	  runtime.isGeneratorFunction = function (genFun) {
	    var ctor = typeof genFun === "function" && genFun.constructor;
	    return ctor ? ctor === GeneratorFunction || // For the native GeneratorFunction constructor, the best we can
	    // do is to check its .name property.
	    (ctor.displayName || ctor.name) === "GeneratorFunction" : false;
	  };

	  runtime.mark = function (genFun) {
	    if (Object.setPrototypeOf) {
	      Object.setPrototypeOf(genFun, GeneratorFunctionPrototype);
	    } else {
	      genFun.__proto__ = GeneratorFunctionPrototype;

	      if (!(toStringTagSymbol in genFun)) {
	        genFun[toStringTagSymbol] = "GeneratorFunction";
	      }
	    }

	    genFun.prototype = Object.create(Gp);
	    return genFun;
	  }; // Within the body of any async function, `await x` is transformed to
	  // `yield regeneratorRuntime.awrap(x)`, so that the runtime can test
	  // `hasOwn.call(value, "__await")` to determine if the yielded value is
	  // meant to be awaited.


	  runtime.awrap = function (arg) {
	    return {
	      __await: arg
	    };
	  };

	  function AsyncIterator(generator) {
	    function invoke(method, arg, resolve, reject) {
	      var record = tryCatch(generator[method], generator, arg);

	      if (record.type === "throw") {
	        reject(record.arg);
	      } else {
	        var result = record.arg;
	        var value = result.value;

	        if (value && _typeof(value) === "object" && hasOwn.call(value, "__await")) {
	          return Promise.resolve(value.__await).then(function (value) {
	            invoke("next", value, resolve, reject);
	          }, function (err) {
	            invoke("throw", err, resolve, reject);
	          });
	        }

	        return Promise.resolve(value).then(function (unwrapped) {
	          // When a yielded Promise is resolved, its final value becomes
	          // the .value of the Promise<{value,done}> result for the
	          // current iteration.
	          result.value = unwrapped;
	          resolve(result);
	        }, function (error) {
	          // If a rejected Promise was yielded, throw the rejection back
	          // into the async generator function so it can be handled there.
	          return invoke("throw", error, resolve, reject);
	        });
	      }
	    }

	    var previousPromise;

	    function enqueue(method, arg) {
	      function callInvokeWithMethodAndArg() {
	        return new Promise(function (resolve, reject) {
	          invoke(method, arg, resolve, reject);
	        });
	      }

	      return previousPromise = // If enqueue has been called before, then we want to wait until
	      // all previous Promises have been resolved before calling invoke,
	      // so that results are always delivered in the correct order. If
	      // enqueue has not been called before, then it is important to
	      // call invoke immediately, without waiting on a callback to fire,
	      // so that the async generator function has the opportunity to do
	      // any necessary setup in a predictable way. This predictability
	      // is why the Promise constructor synchronously invokes its
	      // executor callback, and why async functions synchronously
	      // execute code before the first await. Since we implement simple
	      // async functions in terms of async generators, it is especially
	      // important to get this right, even though it requires care.
	      previousPromise ? previousPromise.then(callInvokeWithMethodAndArg, // Avoid propagating failures to Promises returned by later
	      // invocations of the iterator.
	      callInvokeWithMethodAndArg) : callInvokeWithMethodAndArg();
	    } // Define the unified helper method that is used to implement .next,
	    // .throw, and .return (see defineIteratorMethods).


	    this._invoke = enqueue;
	  }

	  defineIteratorMethods(AsyncIterator.prototype);

	  AsyncIterator.prototype[asyncIteratorSymbol] = function () {
	    return this;
	  };

	  runtime.AsyncIterator = AsyncIterator; // Note that simple async functions are implemented on top of
	  // AsyncIterator objects; they just return a Promise for the value of
	  // the final result produced by the iterator.

	  runtime.async = function (innerFn, outerFn, self, tryLocsList) {
	    var iter = new AsyncIterator(wrap(innerFn, outerFn, self, tryLocsList));
	    return runtime.isGeneratorFunction(outerFn) ? iter // If outerFn is a generator, return the full iterator.
	    : iter.next().then(function (result) {
	      return result.done ? result.value : iter.next();
	    });
	  };

	  function makeInvokeMethod(innerFn, self, context) {
	    var state = GenStateSuspendedStart;
	    return function invoke(method, arg) {
	      if (state === GenStateExecuting) {
	        throw new Error("Generator is already running");
	      }

	      if (state === GenStateCompleted) {
	        if (method === "throw") {
	          throw arg;
	        } // Be forgiving, per 25.3.3.3.3 of the spec:
	        // https://people.mozilla.org/~jorendorff/es6-draft.html#sec-generatorresume


	        return doneResult();
	      }

	      context.method = method;
	      context.arg = arg;

	      while (true) {
	        var delegate = context.delegate;

	        if (delegate) {
	          var delegateResult = maybeInvokeDelegate(delegate, context);

	          if (delegateResult) {
	            if (delegateResult === ContinueSentinel) continue;
	            return delegateResult;
	          }
	        }

	        if (context.method === "next") {
	          // Setting context._sent for legacy support of Babel's
	          // function.sent implementation.
	          context.sent = context._sent = context.arg;
	        } else if (context.method === "throw") {
	          if (state === GenStateSuspendedStart) {
	            state = GenStateCompleted;
	            throw context.arg;
	          }

	          context.dispatchException(context.arg);
	        } else if (context.method === "return") {
	          context.abrupt("return", context.arg);
	        }

	        state = GenStateExecuting;
	        var record = tryCatch(innerFn, self, context);

	        if (record.type === "normal") {
	          // If an exception is thrown from innerFn, we leave state ===
	          // GenStateExecuting and loop back for another invocation.
	          state = context.done ? GenStateCompleted : GenStateSuspendedYield;

	          if (record.arg === ContinueSentinel) {
	            continue;
	          }

	          return {
	            value: record.arg,
	            done: context.done
	          };
	        } else if (record.type === "throw") {
	          state = GenStateCompleted; // Dispatch the exception by looping back around to the
	          // context.dispatchException(context.arg) call above.

	          context.method = "throw";
	          context.arg = record.arg;
	        }
	      }
	    };
	  } // Call delegate.iterator[context.method](context.arg) and handle the
	  // result, either by returning a { value, done } result from the
	  // delegate iterator, or by modifying context.method and context.arg,
	  // setting context.delegate to null, and returning the ContinueSentinel.


	  function maybeInvokeDelegate(delegate, context) {
	    var method = delegate.iterator[context.method];

	    if (method === undefined) {
	      // A .throw or .return when the delegate iterator has no .throw
	      // method always terminates the yield* loop.
	      context.delegate = null;

	      if (context.method === "throw") {
	        if (delegate.iterator.return) {
	          // If the delegate iterator has a return method, give it a
	          // chance to clean up.
	          context.method = "return";
	          context.arg = undefined;
	          maybeInvokeDelegate(delegate, context);

	          if (context.method === "throw") {
	            // If maybeInvokeDelegate(context) changed context.method from
	            // "return" to "throw", let that override the TypeError below.
	            return ContinueSentinel;
	          }
	        }

	        context.method = "throw";
	        context.arg = new TypeError("The iterator does not provide a 'throw' method");
	      }

	      return ContinueSentinel;
	    }

	    var record = tryCatch(method, delegate.iterator, context.arg);

	    if (record.type === "throw") {
	      context.method = "throw";
	      context.arg = record.arg;
	      context.delegate = null;
	      return ContinueSentinel;
	    }

	    var info = record.arg;

	    if (!info) {
	      context.method = "throw";
	      context.arg = new TypeError("iterator result is not an object");
	      context.delegate = null;
	      return ContinueSentinel;
	    }

	    if (info.done) {
	      // Assign the result of the finished delegate to the temporary
	      // variable specified by delegate.resultName (see delegateYield).
	      context[delegate.resultName] = info.value; // Resume execution at the desired location (see delegateYield).

	      context.next = delegate.nextLoc; // If context.method was "throw" but the delegate handled the
	      // exception, let the outer generator proceed normally. If
	      // context.method was "next", forget context.arg since it has been
	      // "consumed" by the delegate iterator. If context.method was
	      // "return", allow the original .return call to continue in the
	      // outer generator.

	      if (context.method !== "return") {
	        context.method = "next";
	        context.arg = undefined;
	      }
	    } else {
	      // Re-yield the result returned by the delegate method.
	      return info;
	    } // The delegate iterator is finished, so forget it and continue with
	    // the outer generator.


	    context.delegate = null;
	    return ContinueSentinel;
	  } // Define Generator.prototype.{next,throw,return} in terms of the
	  // unified ._invoke helper method.


	  defineIteratorMethods(Gp);
	  Gp[toStringTagSymbol] = "Generator"; // A Generator should always return itself as the iterator object when the
	  // @@iterator function is called on it. Some browsers' implementations of the
	  // iterator prototype chain incorrectly implement this, causing the Generator
	  // object to not be returned from this call. This ensures that doesn't happen.
	  // See https://github.com/facebook/regenerator/issues/274 for more details.

	  Gp[iteratorSymbol] = function () {
	    return this;
	  };

	  Gp.toString = function () {
	    return "[object Generator]";
	  };

	  function pushTryEntry(locs) {
	    var entry = {
	      tryLoc: locs[0]
	    };

	    if (1 in locs) {
	      entry.catchLoc = locs[1];
	    }

	    if (2 in locs) {
	      entry.finallyLoc = locs[2];
	      entry.afterLoc = locs[3];
	    }

	    this.tryEntries.push(entry);
	  }

	  function resetTryEntry(entry) {
	    var record = entry.completion || {};
	    record.type = "normal";
	    delete record.arg;
	    entry.completion = record;
	  }

	  function Context(tryLocsList) {
	    // The root entry object (effectively a try statement without a catch
	    // or a finally block) gives us a place to store values thrown from
	    // locations where there is no enclosing try statement.
	    this.tryEntries = [{
	      tryLoc: "root"
	    }];
	    tryLocsList.forEach(pushTryEntry, this);
	    this.reset(true);
	  }

	  runtime.keys = function (object) {
	    var keys = [];

	    for (var key in object) {
	      keys.push(key);
	    }

	    keys.reverse(); // Rather than returning an object with a next method, we keep
	    // things simple and return the next function itself.

	    return function next() {
	      while (keys.length) {
	        var key = keys.pop();

	        if (key in object) {
	          next.value = key;
	          next.done = false;
	          return next;
	        }
	      } // To avoid creating an additional object, we just hang the .value
	      // and .done properties off the next function object itself. This
	      // also ensures that the minifier will not anonymize the function.


	      next.done = true;
	      return next;
	    };
	  };

	  function values(iterable) {
	    if (iterable) {
	      var iteratorMethod = iterable[iteratorSymbol];

	      if (iteratorMethod) {
	        return iteratorMethod.call(iterable);
	      }

	      if (typeof iterable.next === "function") {
	        return iterable;
	      }

	      if (!isNaN(iterable.length)) {
	        var i = -1,
	            next = function next() {
	          while (++i < iterable.length) {
	            if (hasOwn.call(iterable, i)) {
	              next.value = iterable[i];
	              next.done = false;
	              return next;
	            }
	          }

	          next.value = undefined;
	          next.done = true;
	          return next;
	        };

	        return next.next = next;
	      }
	    } // Return an iterator with no values.


	    return {
	      next: doneResult
	    };
	  }

	  runtime.values = values;

	  function doneResult() {
	    return {
	      value: undefined,
	      done: true
	    };
	  }

	  Context.prototype = {
	    constructor: Context,
	    reset: function reset(skipTempReset) {
	      this.prev = 0;
	      this.next = 0; // Resetting context._sent for legacy support of Babel's
	      // function.sent implementation.

	      this.sent = this._sent = undefined;
	      this.done = false;
	      this.delegate = null;
	      this.method = "next";
	      this.arg = undefined;
	      this.tryEntries.forEach(resetTryEntry);

	      if (!skipTempReset) {
	        for (var name in this) {
	          // Not sure about the optimal order of these conditions:
	          if (name.charAt(0) === "t" && hasOwn.call(this, name) && !isNaN(+name.slice(1))) {
	            this[name] = undefined;
	          }
	        }
	      }
	    },
	    stop: function stop() {
	      this.done = true;
	      var rootEntry = this.tryEntries[0];
	      var rootRecord = rootEntry.completion;

	      if (rootRecord.type === "throw") {
	        throw rootRecord.arg;
	      }

	      return this.rval;
	    },
	    dispatchException: function dispatchException(exception) {
	      if (this.done) {
	        throw exception;
	      }

	      var context = this;

	      function handle(loc, caught) {
	        record.type = "throw";
	        record.arg = exception;
	        context.next = loc;

	        if (caught) {
	          // If the dispatched exception was caught by a catch block,
	          // then let that catch block handle the exception normally.
	          context.method = "next";
	          context.arg = undefined;
	        }

	        return !!caught;
	      }

	      for (var i = this.tryEntries.length - 1; i >= 0; --i) {
	        var entry = this.tryEntries[i];
	        var record = entry.completion;

	        if (entry.tryLoc === "root") {
	          // Exception thrown outside of any try block that could handle
	          // it, so set the completion value of the entire function to
	          // throw the exception.
	          return handle("end");
	        }

	        if (entry.tryLoc <= this.prev) {
	          var hasCatch = hasOwn.call(entry, "catchLoc");
	          var hasFinally = hasOwn.call(entry, "finallyLoc");

	          if (hasCatch && hasFinally) {
	            if (this.prev < entry.catchLoc) {
	              return handle(entry.catchLoc, true);
	            } else if (this.prev < entry.finallyLoc) {
	              return handle(entry.finallyLoc);
	            }
	          } else if (hasCatch) {
	            if (this.prev < entry.catchLoc) {
	              return handle(entry.catchLoc, true);
	            }
	          } else if (hasFinally) {
	            if (this.prev < entry.finallyLoc) {
	              return handle(entry.finallyLoc);
	            }
	          } else {
	            throw new Error("try statement without catch or finally");
	          }
	        }
	      }
	    },
	    abrupt: function abrupt(type, arg) {
	      for (var i = this.tryEntries.length - 1; i >= 0; --i) {
	        var entry = this.tryEntries[i];

	        if (entry.tryLoc <= this.prev && hasOwn.call(entry, "finallyLoc") && this.prev < entry.finallyLoc) {
	          var finallyEntry = entry;
	          break;
	        }
	      }

	      if (finallyEntry && (type === "break" || type === "continue") && finallyEntry.tryLoc <= arg && arg <= finallyEntry.finallyLoc) {
	        // Ignore the finally entry if control is not jumping to a
	        // location outside the try/catch block.
	        finallyEntry = null;
	      }

	      var record = finallyEntry ? finallyEntry.completion : {};
	      record.type = type;
	      record.arg = arg;

	      if (finallyEntry) {
	        this.method = "next";
	        this.next = finallyEntry.finallyLoc;
	        return ContinueSentinel;
	      }

	      return this.complete(record);
	    },
	    complete: function complete(record, afterLoc) {
	      if (record.type === "throw") {
	        throw record.arg;
	      }

	      if (record.type === "break" || record.type === "continue") {
	        this.next = record.arg;
	      } else if (record.type === "return") {
	        this.rval = this.arg = record.arg;
	        this.method = "return";
	        this.next = "end";
	      } else if (record.type === "normal" && afterLoc) {
	        this.next = afterLoc;
	      }

	      return ContinueSentinel;
	    },
	    finish: function finish(finallyLoc) {
	      for (var i = this.tryEntries.length - 1; i >= 0; --i) {
	        var entry = this.tryEntries[i];

	        if (entry.finallyLoc === finallyLoc) {
	          this.complete(entry.completion, entry.afterLoc);
	          resetTryEntry(entry);
	          return ContinueSentinel;
	        }
	      }
	    },
	    "catch": function _catch(tryLoc) {
	      for (var i = this.tryEntries.length - 1; i >= 0; --i) {
	        var entry = this.tryEntries[i];

	        if (entry.tryLoc === tryLoc) {
	          var record = entry.completion;

	          if (record.type === "throw") {
	            var thrown = record.arg;
	            resetTryEntry(entry);
	          }

	          return thrown;
	        }
	      } // The context.catch method must only be called with a location
	      // argument that corresponds to a known catch block.


	      throw new Error("illegal catch attempt");
	    },
	    delegateYield: function delegateYield(iterable, resultName, nextLoc) {
	      this.delegate = {
	        iterator: values(iterable),
	        resultName: resultName,
	        nextLoc: nextLoc
	      };

	      if (this.method === "next") {
	        // Deliberately forget the last sent value so that we don't
	        // accidentally pass it on to the delegate.
	        this.arg = undefined;
	      }

	      return ContinueSentinel;
	    }
	  };
	}( // In sloppy mode, unbound `this` refers to the global object, fallback to
	// Function constructor if we're in global strict mode. That is sadly a form
	// of indirect eval which violates Content Security Policy.
	function () {
	  return this || (typeof self === "undefined" ? "undefined" : _typeof(self)) === "object" && self;
	}() || Function("return this")());

	var getKeys = require('./_object-keys');

	var gOPS = require('./_object-gops');

	var pIE = require('./_object-pie');

	var toObject = require('./_to-object');

	var IObject = require('./_iobject');

	var $assign = Object.assign; // should work with symbols and should have deterministic property order (V8 bug)

	module.exports = !$assign || require('./_fails')(function () {
	  var A = {};
	  var B = {}; // eslint-disable-next-line no-undef

	  var S = Symbol();
	  var K = 'abcdefghijklmnopqrst';
	  A[S] = 7;
	  K.split('').forEach(function (k) {
	    B[k] = k;
	  });
	  return $assign({}, A)[S] != 7 || Object.keys($assign({}, B)).join('') != K;
	}) ? function assign(target, source) {
	  // eslint-disable-line no-unused-vars
	  var T = toObject(target);
	  var aLen = arguments.length;
	  var index = 1;
	  var getSymbols = gOPS.f;
	  var isEnum = pIE.f;

	  while (aLen > index) {
	    var S = IObject(arguments[index++]);
	    var keys = getSymbols ? getKeys(S).concat(getSymbols(S)) : getKeys(S);
	    var length = keys.length;
	    var j = 0;
	    var key;

	    while (length > j) {
	      if (isEnum.call(S, key = keys[j++])) T[key] = S[key];
	    }
	  }

	  return T;
	} : $assign;

	var _objectAssign = /*#__PURE__*/Object.freeze({

	});

	// 19.1.3.1 Object.assign(target, source)


	$export$1($export$1.S + $export$1.F, 'Object', {
	  assign: _objectAssign
	});

	var $export$7 = require('./_export');

	var defined$1 = require('./_defined');

	var fails$1 = require('./_fails');

	var spaces = require('./_string-ws');

	var space = '[' + spaces + ']';
	var non = "\u200B\x85";
	var ltrim = RegExp('^' + space + space + '*');
	var rtrim = RegExp(space + space + '*$');

	var exporter = function exporter(KEY, exec, ALIAS) {
	  var exp = {};
	  var FORCE = fails$1(function () {
	    return !!spaces[KEY]() || non[KEY]() != non;
	  });
	  var fn = exp[KEY] = FORCE ? exec(trim) : spaces[KEY];
	  if (ALIAS) exp[ALIAS] = fn;
	  $export$7($export$7.P + $export$7.F * FORCE, 'String', exp);
	}; // 1 -> String#trimLeft
	// 2 -> String#trimRight
	// 3 -> String#trim


	var trim = exporter.trim = function (string, TYPE) {
	  string = String(defined$1(string));
	  if (TYPE & 1) string = string.replace(ltrim, '');
	  if (TYPE & 2) string = string.replace(rtrim, '');
	  return string;
	};

	module.exports = exporter;

	var _stringTrim = /*#__PURE__*/Object.freeze({

	});

	_stringTrim('trim', function ($trim) {
	  return function trim() {
	    return $trim(this, 3);
	  };
	});

	require$$0$3('Uint16', 2, function (init) {
	  return function Uint16Array(data, byteOffset, length) {
	    return init(this, data, byteOffset, length);
	  };
	});

	var $export$8 = require('./_export');

	var aFunction$1 = require('./_a-function');

	var toObject$1 = require('./_to-object');

	var fails$2 = require('./_fails');

	var $sort = [].sort;
	var test = [1, 2, 3];
	$export$8($export$8.P + $export$8.F * (fails$2(function () {
	  // IE8-
	  test.sort(undefined);
	}) || !fails$2(function () {
	  // V8 bug
	  test.sort(null); // Old WebKit
	}) || !require('./_strict-method')($sort)), 'Array', {
	  // 22.1.3.25 Array.prototype.sort(comparefn)
	  sort: function sort(comparefn) {
	    return comparefn === undefined ? $sort.call(toObject$1(this)) : $sort.call(toObject$1(this), aFunction$1(comparefn));
	  }
	});

	var $export$9 = require('./_export');

	var $reduce = require('./_array-reduce');

	$export$9($export$9.P + $export$9.F * !require('./_strict-method')([].reduceRight, true), 'Array', {
	  // 22.1.3.19 / 15.4.4.22 Array.prototype.reduceRight(callbackfn [, initialValue])
	  reduceRight: function reduceRight(callbackfn
	  /* , initialValue */
	  ) {
	    return $reduce(this, callbackfn, arguments.length, arguments[1], true);
	  }
	});

	var $export$a = require('./_export');

	var $reduce$1 = require('./_array-reduce');

	$export$a($export$a.P + $export$a.F * !require('./_strict-method')([].reduce, true), 'Array', {
	  // 22.1.3.18 / 15.4.4.21 Array.prototype.reduce(callbackfn [, initialValue])
	  reduce: function reduce(callbackfn
	  /* , initialValue */
	  ) {
	    return $reduce$1(this, callbackfn, arguments.length, arguments[1], false);
	  }
	});

	var $export$b = require('./_export');

	var toIObject$1 = require('./_to-iobject');

	var toInteger = require('./_to-integer');

	var toLength = require('./_to-length');

	var $native$1 = [].lastIndexOf;
	var NEGATIVE_ZERO$1 = !!$native$1 && 1 / [1].lastIndexOf(1, -0) < 0;
	$export$b($export$b.P + $export$b.F * (NEGATIVE_ZERO$1 || !require('./_strict-method')($native$1)), 'Array', {
	  // 22.1.3.14 / 15.4.4.15 Array.prototype.lastIndexOf(searchElement [, fromIndex])
	  lastIndexOf: function lastIndexOf(searchElement
	  /* , fromIndex = @[*-1] */
	  ) {
	    // convert -0 to +0
	    if (NEGATIVE_ZERO$1) return $native$1.apply(this, arguments) || 0;
	    var O = toIObject$1(this);
	    var length = toLength(O.length);
	    var index = length - 1;
	    if (arguments.length > 1) index = Math.min(index, toInteger(arguments[1]));
	    if (index < 0) index = length + index;

	    for (; index >= 0; index--) {
	      if (index in O) if (O[index] === searchElement) return index || 0;
	    }

	    return -1;
	  }
	});

	if (require('./_descriptors')) {
	  var LIBRARY$2 = require('./_library');

	  var global$5 = require('./_global');

	  var fails$3 = require('./_fails');

	  var $export$c = require('./_export');

	  var $typed = require('./_typed');

	  var $buffer = require('./_typed-buffer');

	  var ctx$2 = require('./_ctx');

	  var anInstance$1 = require('./_an-instance');

	  var propertyDesc = require('./_property-desc');

	  var hide$4 = require('./_hide');

	  var redefineAll = require('./_redefine-all');

	  var toInteger$1 = require('./_to-integer');

	  var toLength$1 = require('./_to-length');

	  var toIndex = require('./_to-index');

	  var toAbsoluteIndex = require('./_to-absolute-index');

	  var toPrimitive$2 = require('./_to-primitive');

	  var has$2 = require('./_has');

	  var classof$1 = require('./_classof');

	  var isObject$4 = require('./_is-object');

	  var toObject$2 = require('./_to-object');

	  var isArrayIter = require('./_is-array-iter');

	  var create$1 = require('./_object-create');

	  var getPrototypeOf$1 = require('./_object-gpo');

	  var gOPN$2 = require('./_object-gopn').f;

	  var getIterFn = require('./core.get-iterator-method');

	  var uid$2 = require('./_uid');

	  var wks$2 = require('./_wks');

	  var createArrayMethod = require('./_array-methods');

	  var createArrayIncludes = require('./_array-includes');

	  var speciesConstructor$1 = require('./_species-constructor');

	  var ArrayIterators = require('./es6.array.iterator');

	  var Iterators$1 = require('./_iterators');

	  var $iterDetect = require('./_iter-detect');

	  var setSpecies = require('./_set-species');

	  var arrayFill = require('./_array-fill');

	  var arrayCopyWithin = require('./_array-copy-within');

	  var $DP$1 = require('./_object-dp');

	  var $GOPD$1 = require('./_object-gopd');

	  var dP$4 = $DP$1.f;
	  var gOPD$1 = $GOPD$1.f;
	  var RangeError$1 = global$5.RangeError;
	  var TypeError$2 = global$5.TypeError;
	  var Uint8Array$1 = global$5.Uint8Array;
	  var ARRAY_BUFFER = 'ArrayBuffer';
	  var SHARED_BUFFER = 'Shared' + ARRAY_BUFFER;
	  var BYTES_PER_ELEMENT = 'BYTES_PER_ELEMENT';
	  var PROTOTYPE$3 = 'prototype';
	  var ArrayProto$1 = Array[PROTOTYPE$3];
	  var $ArrayBuffer = $buffer.ArrayBuffer;
	  var $DataView = $buffer.DataView;
	  var arrayForEach = createArrayMethod(0);
	  var arrayFilter = createArrayMethod(2);
	  var arraySome = createArrayMethod(3);
	  var arrayEvery = createArrayMethod(4);
	  var arrayFind = createArrayMethod(5);
	  var arrayFindIndex = createArrayMethod(6);
	  var arrayIncludes = createArrayIncludes(true);
	  var arrayIndexOf$1 = createArrayIncludes(false);
	  var arrayValues = ArrayIterators.values;
	  var arrayKeys = ArrayIterators.keys;
	  var arrayEntries = ArrayIterators.entries;
	  var arrayLastIndexOf = ArrayProto$1.lastIndexOf;
	  var arrayReduce = ArrayProto$1.reduce;
	  var arrayReduceRight = ArrayProto$1.reduceRight;
	  var arrayJoin = ArrayProto$1.join;
	  var arraySort = ArrayProto$1.sort;
	  var arraySlice = ArrayProto$1.slice;
	  var arrayToString = ArrayProto$1.toString;
	  var arrayToLocaleString = ArrayProto$1.toLocaleString;
	  var ITERATOR$2 = wks$2('iterator');
	  var TAG = wks$2('toStringTag');
	  var TYPED_CONSTRUCTOR = uid$2('typed_constructor');
	  var DEF_CONSTRUCTOR = uid$2('def_constructor');
	  var ALL_CONSTRUCTORS = $typed.CONSTR;
	  var TYPED_ARRAY = $typed.TYPED;
	  var VIEW = $typed.VIEW;
	  var WRONG_LENGTH = 'Wrong length!';
	  var $map = createArrayMethod(1, function (O, length) {
	    return allocate(speciesConstructor$1(O, O[DEF_CONSTRUCTOR]), length);
	  });
	  var LITTLE_ENDIAN = fails$3(function () {
	    // eslint-disable-next-line no-undef
	    return new Uint8Array$1(new Uint16Array([1]).buffer)[0] === 1;
	  });
	  var FORCED_SET = !!Uint8Array$1 && !!Uint8Array$1[PROTOTYPE$3].set && fails$3(function () {
	    new Uint8Array$1(1).set({});
	  });

	  var toOffset = function toOffset(it, BYTES) {
	    var offset = toInteger$1(it);
	    if (offset < 0 || offset % BYTES) throw RangeError$1('Wrong offset!');
	    return offset;
	  };

	  var validate = function validate(it) {
	    if (isObject$4(it) && TYPED_ARRAY in it) return it;
	    throw TypeError$2(it + ' is not a typed array!');
	  };

	  var allocate = function allocate(C, length) {
	    if (!(isObject$4(C) && TYPED_CONSTRUCTOR in C)) {
	      throw TypeError$2('It is not a typed array constructor!');
	    }

	    return new C(length);
	  };

	  var speciesFromList = function speciesFromList(O, list) {
	    return fromList(speciesConstructor$1(O, O[DEF_CONSTRUCTOR]), list);
	  };

	  var fromList = function fromList(C, list) {
	    var index = 0;
	    var length = list.length;
	    var result = allocate(C, length);

	    while (length > index) {
	      result[index] = list[index++];
	    }

	    return result;
	  };

	  var addGetter = function addGetter(it, key, internal) {
	    dP$4(it, key, {
	      get: function get() {
	        return this._d[internal];
	      }
	    });
	  };

	  var $from = function from(source
	  /* , mapfn, thisArg */
	  ) {
	    var O = toObject$2(source);
	    var aLen = arguments.length;
	    var mapfn = aLen > 1 ? arguments[1] : undefined;
	    var mapping = mapfn !== undefined;
	    var iterFn = getIterFn(O);
	    var i, length, values, result, step, iterator;

	    if (iterFn != undefined && !isArrayIter(iterFn)) {
	      for (iterator = iterFn.call(O), values = [], i = 0; !(step = iterator.next()).done; i++) {
	        values.push(step.value);
	      }

	      O = values;
	    }

	    if (mapping && aLen > 2) mapfn = ctx$2(mapfn, arguments[2], 2);

	    for (i = 0, length = toLength$1(O.length), result = allocate(this, length); length > i; i++) {
	      result[i] = mapping ? mapfn(O[i], i) : O[i];
	    }

	    return result;
	  };

	  var $of = function of()
	  /* ...items */
	  {
	    var index = 0;
	    var length = arguments.length;
	    var result = allocate(this, length);

	    while (length > index) {
	      result[index] = arguments[index++];
	    }

	    return result;
	  }; // iOS Safari 6.x fails here


	  var TO_LOCALE_BUG = !!Uint8Array$1 && fails$3(function () {
	    arrayToLocaleString.call(new Uint8Array$1(1));
	  });

	  var $toLocaleString = function toLocaleString() {
	    return arrayToLocaleString.apply(TO_LOCALE_BUG ? arraySlice.call(validate(this)) : validate(this), arguments);
	  };

	  var proto$2 = {
	    copyWithin: function copyWithin(target, start
	    /* , end */
	    ) {
	      return arrayCopyWithin.call(validate(this), target, start, arguments.length > 2 ? arguments[2] : undefined);
	    },
	    every: function every(callbackfn
	    /* , thisArg */
	    ) {
	      return arrayEvery(validate(this), callbackfn, arguments.length > 1 ? arguments[1] : undefined);
	    },
	    fill: function fill(value
	    /* , start, end */
	    ) {
	      // eslint-disable-line no-unused-vars
	      return arrayFill.apply(validate(this), arguments);
	    },
	    filter: function filter(callbackfn
	    /* , thisArg */
	    ) {
	      return speciesFromList(this, arrayFilter(validate(this), callbackfn, arguments.length > 1 ? arguments[1] : undefined));
	    },
	    find: function find(predicate
	    /* , thisArg */
	    ) {
	      return arrayFind(validate(this), predicate, arguments.length > 1 ? arguments[1] : undefined);
	    },
	    findIndex: function findIndex(predicate
	    /* , thisArg */
	    ) {
	      return arrayFindIndex(validate(this), predicate, arguments.length > 1 ? arguments[1] : undefined);
	    },
	    forEach: function forEach(callbackfn
	    /* , thisArg */
	    ) {
	      arrayForEach(validate(this), callbackfn, arguments.length > 1 ? arguments[1] : undefined);
	    },
	    indexOf: function indexOf(searchElement
	    /* , fromIndex */
	    ) {
	      return arrayIndexOf$1(validate(this), searchElement, arguments.length > 1 ? arguments[1] : undefined);
	    },
	    includes: function includes(searchElement
	    /* , fromIndex */
	    ) {
	      return arrayIncludes(validate(this), searchElement, arguments.length > 1 ? arguments[1] : undefined);
	    },
	    join: function join(separator) {
	      // eslint-disable-line no-unused-vars
	      return arrayJoin.apply(validate(this), arguments);
	    },
	    lastIndexOf: function lastIndexOf(searchElement
	    /* , fromIndex */
	    ) {
	      // eslint-disable-line no-unused-vars
	      return arrayLastIndexOf.apply(validate(this), arguments);
	    },
	    map: function map(mapfn
	    /* , thisArg */
	    ) {
	      return $map(validate(this), mapfn, arguments.length > 1 ? arguments[1] : undefined);
	    },
	    reduce: function reduce(callbackfn
	    /* , initialValue */
	    ) {
	      // eslint-disable-line no-unused-vars
	      return arrayReduce.apply(validate(this), arguments);
	    },
	    reduceRight: function reduceRight(callbackfn
	    /* , initialValue */
	    ) {
	      // eslint-disable-line no-unused-vars
	      return arrayReduceRight.apply(validate(this), arguments);
	    },
	    reverse: function reverse() {
	      var that = this;
	      var length = validate(that).length;
	      var middle = Math.floor(length / 2);
	      var index = 0;
	      var value;

	      while (index < middle) {
	        value = that[index];
	        that[index++] = that[--length];
	        that[length] = value;
	      }

	      return that;
	    },
	    some: function some(callbackfn
	    /* , thisArg */
	    ) {
	      return arraySome(validate(this), callbackfn, arguments.length > 1 ? arguments[1] : undefined);
	    },
	    sort: function sort(comparefn) {
	      return arraySort.call(validate(this), comparefn);
	    },
	    subarray: function subarray(begin, end) {
	      var O = validate(this);
	      var length = O.length;
	      var $begin = toAbsoluteIndex(begin, length);
	      return new (speciesConstructor$1(O, O[DEF_CONSTRUCTOR]))(O.buffer, O.byteOffset + $begin * O.BYTES_PER_ELEMENT, toLength$1((end === undefined ? length : toAbsoluteIndex(end, length)) - $begin));
	    }
	  };

	  var $slice = function slice(start, end) {
	    return speciesFromList(this, arraySlice.call(validate(this), start, end));
	  };

	  var $set = function set(arrayLike
	  /* , offset */
	  ) {
	    validate(this);
	    var offset = toOffset(arguments[1], 1);
	    var length = this.length;
	    var src = toObject$2(arrayLike);
	    var len = toLength$1(src.length);
	    var index = 0;
	    if (len + offset > length) throw RangeError$1(WRONG_LENGTH);

	    while (index < len) {
	      this[offset + index] = src[index++];
	    }
	  };

	  var $iterators = {
	    entries: function entries() {
	      return arrayEntries.call(validate(this));
	    },
	    keys: function keys() {
	      return arrayKeys.call(validate(this));
	    },
	    values: function values() {
	      return arrayValues.call(validate(this));
	    }
	  };

	  var isTAIndex = function isTAIndex(target, key) {
	    return isObject$4(target) && target[TYPED_ARRAY] && _typeof(key) != 'symbol' && key in target && String(+key) == String(key);
	  };

	  var $getDesc = function getOwnPropertyDescriptor(target, key) {
	    return isTAIndex(target, key = toPrimitive$2(key, true)) ? propertyDesc(2, target[key]) : gOPD$1(target, key);
	  };

	  var $setDesc = function defineProperty(target, key, desc) {
	    if (isTAIndex(target, key = toPrimitive$2(key, true)) && isObject$4(desc) && has$2(desc, 'value') && !has$2(desc, 'get') && !has$2(desc, 'set') // TODO: add validation descriptor w/o calling accessors
	    && !desc.configurable && (!has$2(desc, 'writable') || desc.writable) && (!has$2(desc, 'enumerable') || desc.enumerable)) {
	      target[key] = desc.value;
	      return target;
	    }

	    return dP$4(target, key, desc);
	  };

	  if (!ALL_CONSTRUCTORS) {
	    $GOPD$1.f = $getDesc;
	    $DP$1.f = $setDesc;
	  }

	  $export$c($export$c.S + $export$c.F * !ALL_CONSTRUCTORS, 'Object', {
	    getOwnPropertyDescriptor: $getDesc,
	    defineProperty: $setDesc
	  });

	  if (fails$3(function () {
	    arrayToString.call({});
	  })) {
	    arrayToString = arrayToLocaleString = function toString() {
	      return arrayJoin.call(this);
	    };
	  }

	  var $TypedArrayPrototype$ = redefineAll({}, proto$2);
	  redefineAll($TypedArrayPrototype$, $iterators);
	  hide$4($TypedArrayPrototype$, ITERATOR$2, $iterators.values);
	  redefineAll($TypedArrayPrototype$, {
	    slice: $slice,
	    set: $set,
	    constructor: function constructor() {
	      /* noop */
	    },
	    toString: arrayToString,
	    toLocaleString: $toLocaleString
	  });
	  addGetter($TypedArrayPrototype$, 'buffer', 'b');
	  addGetter($TypedArrayPrototype$, 'byteOffset', 'o');
	  addGetter($TypedArrayPrototype$, 'byteLength', 'l');
	  addGetter($TypedArrayPrototype$, 'length', 'e');
	  dP$4($TypedArrayPrototype$, TAG, {
	    get: function get() {
	      return this[TYPED_ARRAY];
	    }
	  }); // eslint-disable-next-line max-statements

	  module.exports = function (KEY, BYTES, wrapper, CLAMPED) {
	    CLAMPED = !!CLAMPED;
	    var NAME = KEY + (CLAMPED ? 'Clamped' : '') + 'Array';
	    var GETTER = 'get' + KEY;
	    var SETTER = 'set' + KEY;
	    var TypedArray = global$5[NAME];
	    var Base = TypedArray || {};
	    var TAC = TypedArray && getPrototypeOf$1(TypedArray);
	    var FORCED = !TypedArray || !$typed.ABV;
	    var O = {};
	    var TypedArrayPrototype = TypedArray && TypedArray[PROTOTYPE$3];

	    var getter = function getter(that, index) {
	      var data = that._d;
	      return data.v[GETTER](index * BYTES + data.o, LITTLE_ENDIAN);
	    };

	    var setter = function setter(that, index, value) {
	      var data = that._d;
	      if (CLAMPED) value = (value = Math.round(value)) < 0 ? 0 : value > 0xff ? 0xff : value & 0xff;
	      data.v[SETTER](index * BYTES + data.o, value, LITTLE_ENDIAN);
	    };

	    var addElement = function addElement(that, index) {
	      dP$4(that, index, {
	        get: function get() {
	          return getter(this, index);
	        },
	        set: function set(value) {
	          return setter(this, index, value);
	        },
	        enumerable: true
	      });
	    };

	    if (FORCED) {
	      TypedArray = wrapper(function (that, data, $offset, $length) {
	        anInstance$1(that, TypedArray, NAME, '_d');
	        var index = 0;
	        var offset = 0;
	        var buffer, byteLength, length, klass;

	        if (!isObject$4(data)) {
	          length = toIndex(data);
	          byteLength = length * BYTES;
	          buffer = new $ArrayBuffer(byteLength);
	        } else if (data instanceof $ArrayBuffer || (klass = classof$1(data)) == ARRAY_BUFFER || klass == SHARED_BUFFER) {
	          buffer = data;
	          offset = toOffset($offset, BYTES);
	          var $len = data.byteLength;

	          if ($length === undefined) {
	            if ($len % BYTES) throw RangeError$1(WRONG_LENGTH);
	            byteLength = $len - offset;
	            if (byteLength < 0) throw RangeError$1(WRONG_LENGTH);
	          } else {
	            byteLength = toLength$1($length) * BYTES;
	            if (byteLength + offset > $len) throw RangeError$1(WRONG_LENGTH);
	          }

	          length = byteLength / BYTES;
	        } else if (TYPED_ARRAY in data) {
	          return fromList(TypedArray, data);
	        } else {
	          return $from.call(TypedArray, data);
	        }

	        hide$4(that, '_d', {
	          b: buffer,
	          o: offset,
	          l: byteLength,
	          e: length,
	          v: new $DataView(buffer)
	        });

	        while (index < length) {
	          addElement(that, index++);
	        }
	      });
	      TypedArrayPrototype = TypedArray[PROTOTYPE$3] = create$1($TypedArrayPrototype$);
	      hide$4(TypedArrayPrototype, 'constructor', TypedArray);
	    } else if (!fails$3(function () {
	      TypedArray(1);
	    }) || !fails$3(function () {
	      new TypedArray(-1); // eslint-disable-line no-new
	    }) || !$iterDetect(function (iter) {
	      new TypedArray(); // eslint-disable-line no-new

	      new TypedArray(null); // eslint-disable-line no-new

	      new TypedArray(1.5); // eslint-disable-line no-new

	      new TypedArray(iter); // eslint-disable-line no-new
	    }, true)) {
	      TypedArray = wrapper(function (that, data, $offset, $length) {
	        anInstance$1(that, TypedArray, NAME);
	        var klass; // `ws` module bug, temporarily remove validation length for Uint8Array
	        // https://github.com/websockets/ws/pull/645

	        if (!isObject$4(data)) return new Base(toIndex(data));

	        if (data instanceof $ArrayBuffer || (klass = classof$1(data)) == ARRAY_BUFFER || klass == SHARED_BUFFER) {
	          return $length !== undefined ? new Base(data, toOffset($offset, BYTES), $length) : $offset !== undefined ? new Base(data, toOffset($offset, BYTES)) : new Base(data);
	        }

	        if (TYPED_ARRAY in data) return fromList(TypedArray, data);
	        return $from.call(TypedArray, data);
	      });
	      arrayForEach(TAC !== Function.prototype ? gOPN$2(Base).concat(gOPN$2(TAC)) : gOPN$2(Base), function (key) {
	        if (!(key in TypedArray)) hide$4(TypedArray, key, Base[key]);
	      });
	      TypedArray[PROTOTYPE$3] = TypedArrayPrototype;
	      if (!LIBRARY$2) TypedArrayPrototype.constructor = TypedArray;
	    }

	    var $nativeIterator = TypedArrayPrototype[ITERATOR$2];
	    var CORRECT_ITER_NAME = !!$nativeIterator && ($nativeIterator.name == 'values' || $nativeIterator.name == undefined);
	    var $iterator = $iterators.values;
	    hide$4(TypedArray, TYPED_CONSTRUCTOR, true);
	    hide$4(TypedArrayPrototype, TYPED_ARRAY, NAME);
	    hide$4(TypedArrayPrototype, VIEW, true);
	    hide$4(TypedArrayPrototype, DEF_CONSTRUCTOR, TypedArray);

	    if (CLAMPED ? new TypedArray(1)[TAG] != NAME : !(TAG in TypedArrayPrototype)) {
	      dP$4(TypedArrayPrototype, TAG, {
	        get: function get() {
	          return NAME;
	        }
	      });
	    }

	    O[NAME] = TypedArray;
	    $export$c($export$c.G + $export$c.W + $export$c.F * (TypedArray != Base), O);
	    $export$c($export$c.S, NAME, {
	      BYTES_PER_ELEMENT: BYTES
	    });
	    $export$c($export$c.S + $export$c.F * fails$3(function () {
	      Base.of.call(TypedArray, 1);
	    }), NAME, {
	      from: $from,
	      of: $of
	    });
	    if (!(BYTES_PER_ELEMENT in TypedArrayPrototype)) hide$4(TypedArrayPrototype, BYTES_PER_ELEMENT, BYTES);
	    $export$c($export$c.P, NAME, proto$2);
	    setSpecies(NAME);
	    $export$c($export$c.P + $export$c.F * FORCED_SET, NAME, {
	      set: $set
	    });
	    $export$c($export$c.P + $export$c.F * !CORRECT_ITER_NAME, NAME, $iterators);
	    if (!LIBRARY$2 && TypedArrayPrototype.toString != arrayToString) TypedArrayPrototype.toString = arrayToString;
	    $export$c($export$c.P + $export$c.F * fails$3(function () {
	      new TypedArray(1).slice();
	    }), NAME, {
	      slice: $slice
	    });
	    $export$c($export$c.P + $export$c.F * (fails$3(function () {
	      return [1, 2].toLocaleString() != new TypedArray([1, 2]).toLocaleString();
	    }) || !fails$3(function () {
	      TypedArrayPrototype.toLocaleString.call([1, 2]);
	    })), NAME, {
	      toLocaleString: $toLocaleString
	    });
	    Iterators$1[NAME] = CORRECT_ITER_NAME ? $nativeIterator : $iterator;
	    if (!LIBRARY$2 && !CORRECT_ITER_NAME) hide$4(TypedArrayPrototype, ITERATOR$2, $iterator);
	  };
	} else module.exports = function () {
	  /* empty */
	};

	require$$0$3('Uint8', 1, function (init) {
	  return function Uint8Array(data, byteOffset, length) {
	    return init(this, data, byteOffset, length);
	  };
	});

	var $export$d = require('./_export');

	var $map$1 = require('./_array-methods')(1);

	$export$d($export$d.P + $export$d.F * !require('./_strict-method')([].map, true), 'Array', {
	  // 22.1.3.15 / 15.4.4.19 Array.prototype.map(callbackfn [, thisArg])
	  map: function map(callbackfn
	  /* , thisArg */
	  ) {
	    return $map$1(this, callbackfn, arguments[1]);
	  }
	});

	// 7.2.2 IsArray(argument)
	var cof$2 = require('./_cof');

	module.exports = Array.isArray || function isArray(arg) {
	  return cof$2(arg) == 'Array';
	};

	var _isArray = /*#__PURE__*/Object.freeze({

	});

	// 22.1.2.2 / 15.4.3.2 Array.isArray(arg)


	$export$1($export$1.S, 'Array', {
	  isArray: _isArray
	});

	var global$6 = require('./_global');

	var hide$5 = require('./_hide');

	var uid$3 = require('./_uid');

	var TYPED = uid$3('typed_array');
	var VIEW$1 = uid$3('view');
	var ABV = !!(global$6.ArrayBuffer && global$6.DataView);
	var CONSTR = ABV;
	var i$2 = 0;
	var l = 9;
	var Typed;
	var TypedArrayConstructors = 'Int8Array,Uint8Array,Uint8ClampedArray,Int16Array,Uint16Array,Int32Array,Uint32Array,Float32Array,Float64Array'.split(',');

	while (i$2 < l) {
	  if (Typed = global$6[TypedArrayConstructors[i$2++]]) {
	    hide$5(Typed.prototype, TYPED, true);
	    hide$5(Typed.prototype, VIEW$1, true);
	  } else CONSTR = false;
	}

	module.exports = {
	  ABV: ABV,
	  CONSTR: CONSTR,
	  TYPED: TYPED,
	  VIEW: VIEW$1
	};

	var _typed = /*#__PURE__*/Object.freeze({

	});

	var global$7 = require('./_global');

	var DESCRIPTORS$3 = require('./_descriptors');

	var LIBRARY$3 = require('./_library');

	var $typed$1 = require('./_typed');

	var hide$6 = require('./_hide');

	var redefineAll$1 = require('./_redefine-all');

	var fails$4 = require('./_fails');

	var anInstance$2 = require('./_an-instance');

	var toInteger$2 = require('./_to-integer');

	var toLength$2 = require('./_to-length');

	var toIndex$1 = require('./_to-index');

	var gOPN$3 = require('./_object-gopn').f;

	var dP$5 = require('./_object-dp').f;

	var arrayFill$1 = require('./_array-fill');

	var setToStringTag$2 = require('./_set-to-string-tag');

	var ARRAY_BUFFER$1 = 'ArrayBuffer';
	var DATA_VIEW = 'DataView';
	var PROTOTYPE$4 = 'prototype';
	var WRONG_LENGTH$1 = 'Wrong length!';
	var WRONG_INDEX = 'Wrong index!';
	var $ArrayBuffer$1 = global$7[ARRAY_BUFFER$1];
	var $DataView$1 = global$7[DATA_VIEW];
	var Math$1 = global$7.Math;
	var RangeError$2 = global$7.RangeError; // eslint-disable-next-line no-shadow-restricted-names

	var Infinity$1 = global$7.Infinity;
	var BaseBuffer = $ArrayBuffer$1;
	var abs = Math$1.abs;
	var pow = Math$1.pow;
	var floor$1 = Math$1.floor;
	var log = Math$1.log;
	var LN2 = Math$1.LN2;
	var BUFFER = 'buffer';
	var BYTE_LENGTH = 'byteLength';
	var BYTE_OFFSET = 'byteOffset';
	var $BUFFER = DESCRIPTORS$3 ? '_b' : BUFFER;
	var $LENGTH = DESCRIPTORS$3 ? '_l' : BYTE_LENGTH;
	var $OFFSET = DESCRIPTORS$3 ? '_o' : BYTE_OFFSET; // IEEE754 conversions based on https://github.com/feross/ieee754

	function packIEEE754(value, mLen, nBytes) {
	  var buffer = new Array(nBytes);
	  var eLen = nBytes * 8 - mLen - 1;
	  var eMax = (1 << eLen) - 1;
	  var eBias = eMax >> 1;
	  var rt = mLen === 23 ? pow(2, -24) - pow(2, -77) : 0;
	  var i = 0;
	  var s = value < 0 || value === 0 && 1 / value < 0 ? 1 : 0;
	  var e, m, c;
	  value = abs(value); // eslint-disable-next-line no-self-compare

	  if (value != value || value === Infinity$1) {
	    // eslint-disable-next-line no-self-compare
	    m = value != value ? 1 : 0;
	    e = eMax;
	  } else {
	    e = floor$1(log(value) / LN2);

	    if (value * (c = pow(2, -e)) < 1) {
	      e--;
	      c *= 2;
	    }

	    if (e + eBias >= 1) {
	      value += rt / c;
	    } else {
	      value += rt * pow(2, 1 - eBias);
	    }

	    if (value * c >= 2) {
	      e++;
	      c /= 2;
	    }

	    if (e + eBias >= eMax) {
	      m = 0;
	      e = eMax;
	    } else if (e + eBias >= 1) {
	      m = (value * c - 1) * pow(2, mLen);
	      e = e + eBias;
	    } else {
	      m = value * pow(2, eBias - 1) * pow(2, mLen);
	      e = 0;
	    }
	  }

	  for (; mLen >= 8; buffer[i++] = m & 255, m /= 256, mLen -= 8) {
	  }

	  e = e << mLen | m;
	  eLen += mLen;

	  for (; eLen > 0; buffer[i++] = e & 255, e /= 256, eLen -= 8) {
	  }

	  buffer[--i] |= s * 128;
	  return buffer;
	}

	function unpackIEEE754(buffer, mLen, nBytes) {
	  var eLen = nBytes * 8 - mLen - 1;
	  var eMax = (1 << eLen) - 1;
	  var eBias = eMax >> 1;
	  var nBits = eLen - 7;
	  var i = nBytes - 1;
	  var s = buffer[i--];
	  var e = s & 127;
	  var m;
	  s >>= 7;

	  for (; nBits > 0; e = e * 256 + buffer[i], i--, nBits -= 8) {
	  }

	  m = e & (1 << -nBits) - 1;
	  e >>= -nBits;
	  nBits += mLen;

	  for (; nBits > 0; m = m * 256 + buffer[i], i--, nBits -= 8) {
	  }

	  if (e === 0) {
	    e = 1 - eBias;
	  } else if (e === eMax) {
	    return m ? NaN : s ? -Infinity$1 : Infinity$1;
	  } else {
	    m = m + pow(2, mLen);
	    e = e - eBias;
	  }

	  return (s ? -1 : 1) * m * pow(2, e - mLen);
	}

	function unpackI32(bytes) {
	  return bytes[3] << 24 | bytes[2] << 16 | bytes[1] << 8 | bytes[0];
	}

	function packI8(it) {
	  return [it & 0xff];
	}

	function packI16(it) {
	  return [it & 0xff, it >> 8 & 0xff];
	}

	function packI32(it) {
	  return [it & 0xff, it >> 8 & 0xff, it >> 16 & 0xff, it >> 24 & 0xff];
	}

	function packF64(it) {
	  return packIEEE754(it, 52, 8);
	}

	function packF32(it) {
	  return packIEEE754(it, 23, 4);
	}

	function addGetter$1(C, key, internal) {
	  dP$5(C[PROTOTYPE$4], key, {
	    get: function get() {
	      return this[internal];
	    }
	  });
	}

	function get(view, bytes, index, isLittleEndian) {
	  var numIndex = +index;
	  var intIndex = toIndex$1(numIndex);
	  if (intIndex + bytes > view[$LENGTH]) throw RangeError$2(WRONG_INDEX);
	  var store = view[$BUFFER]._b;
	  var start = intIndex + view[$OFFSET];
	  var pack = store.slice(start, start + bytes);
	  return isLittleEndian ? pack : pack.reverse();
	}

	function set$1(view, bytes, index, conversion, value, isLittleEndian) {
	  var numIndex = +index;
	  var intIndex = toIndex$1(numIndex);
	  if (intIndex + bytes > view[$LENGTH]) throw RangeError$2(WRONG_INDEX);
	  var store = view[$BUFFER]._b;
	  var start = intIndex + view[$OFFSET];
	  var pack = conversion(+value);

	  for (var i = 0; i < bytes; i++) {
	    store[start + i] = pack[isLittleEndian ? i : bytes - i - 1];
	  }
	}

	if (!$typed$1.ABV) {
	  $ArrayBuffer$1 = function ArrayBuffer(length) {
	    anInstance$2(this, $ArrayBuffer$1, ARRAY_BUFFER$1);
	    var byteLength = toIndex$1(length);
	    this._b = arrayFill$1.call(new Array(byteLength), 0);
	    this[$LENGTH] = byteLength;
	  };

	  $DataView$1 = function DataView(buffer, byteOffset, byteLength) {
	    anInstance$2(this, $DataView$1, DATA_VIEW);
	    anInstance$2(buffer, $ArrayBuffer$1, DATA_VIEW);
	    var bufferLength = buffer[$LENGTH];
	    var offset = toInteger$2(byteOffset);
	    if (offset < 0 || offset > bufferLength) throw RangeError$2('Wrong offset!');
	    byteLength = byteLength === undefined ? bufferLength - offset : toLength$2(byteLength);
	    if (offset + byteLength > bufferLength) throw RangeError$2(WRONG_LENGTH$1);
	    this[$BUFFER] = buffer;
	    this[$OFFSET] = offset;
	    this[$LENGTH] = byteLength;
	  };

	  if (DESCRIPTORS$3) {
	    addGetter$1($ArrayBuffer$1, BYTE_LENGTH, '_l');
	    addGetter$1($DataView$1, BUFFER, '_b');
	    addGetter$1($DataView$1, BYTE_LENGTH, '_l');
	    addGetter$1($DataView$1, BYTE_OFFSET, '_o');
	  }

	  redefineAll$1($DataView$1[PROTOTYPE$4], {
	    getInt8: function getInt8(byteOffset) {
	      return get(this, 1, byteOffset)[0] << 24 >> 24;
	    },
	    getUint8: function getUint8(byteOffset) {
	      return get(this, 1, byteOffset)[0];
	    },
	    getInt16: function getInt16(byteOffset
	    /* , littleEndian */
	    ) {
	      var bytes = get(this, 2, byteOffset, arguments[1]);
	      return (bytes[1] << 8 | bytes[0]) << 16 >> 16;
	    },
	    getUint16: function getUint16(byteOffset
	    /* , littleEndian */
	    ) {
	      var bytes = get(this, 2, byteOffset, arguments[1]);
	      return bytes[1] << 8 | bytes[0];
	    },
	    getInt32: function getInt32(byteOffset
	    /* , littleEndian */
	    ) {
	      return unpackI32(get(this, 4, byteOffset, arguments[1]));
	    },
	    getUint32: function getUint32(byteOffset
	    /* , littleEndian */
	    ) {
	      return unpackI32(get(this, 4, byteOffset, arguments[1])) >>> 0;
	    },
	    getFloat32: function getFloat32(byteOffset
	    /* , littleEndian */
	    ) {
	      return unpackIEEE754(get(this, 4, byteOffset, arguments[1]), 23, 4);
	    },
	    getFloat64: function getFloat64(byteOffset
	    /* , littleEndian */
	    ) {
	      return unpackIEEE754(get(this, 8, byteOffset, arguments[1]), 52, 8);
	    },
	    setInt8: function setInt8(byteOffset, value) {
	      set$1(this, 1, byteOffset, packI8, value);
	    },
	    setUint8: function setUint8(byteOffset, value) {
	      set$1(this, 1, byteOffset, packI8, value);
	    },
	    setInt16: function setInt16(byteOffset, value
	    /* , littleEndian */
	    ) {
	      set$1(this, 2, byteOffset, packI16, value, arguments[2]);
	    },
	    setUint16: function setUint16(byteOffset, value
	    /* , littleEndian */
	    ) {
	      set$1(this, 2, byteOffset, packI16, value, arguments[2]);
	    },
	    setInt32: function setInt32(byteOffset, value
	    /* , littleEndian */
	    ) {
	      set$1(this, 4, byteOffset, packI32, value, arguments[2]);
	    },
	    setUint32: function setUint32(byteOffset, value
	    /* , littleEndian */
	    ) {
	      set$1(this, 4, byteOffset, packI32, value, arguments[2]);
	    },
	    setFloat32: function setFloat32(byteOffset, value
	    /* , littleEndian */
	    ) {
	      set$1(this, 4, byteOffset, packF32, value, arguments[2]);
	    },
	    setFloat64: function setFloat64(byteOffset, value
	    /* , littleEndian */
	    ) {
	      set$1(this, 8, byteOffset, packF64, value, arguments[2]);
	    }
	  });
	} else {
	  if (!fails$4(function () {
	    $ArrayBuffer$1(1);
	  }) || !fails$4(function () {
	    new $ArrayBuffer$1(-1); // eslint-disable-line no-new
	  }) || fails$4(function () {
	    new $ArrayBuffer$1(); // eslint-disable-line no-new

	    new $ArrayBuffer$1(1.5); // eslint-disable-line no-new

	    new $ArrayBuffer$1(NaN); // eslint-disable-line no-new

	    return $ArrayBuffer$1.name != ARRAY_BUFFER$1;
	  })) {
	    $ArrayBuffer$1 = function ArrayBuffer(length) {
	      anInstance$2(this, $ArrayBuffer$1);
	      return new BaseBuffer(toIndex$1(length));
	    };

	    var ArrayBufferProto = $ArrayBuffer$1[PROTOTYPE$4] = BaseBuffer[PROTOTYPE$4];

	    for (var keys$1 = gOPN$3(BaseBuffer), j$1 = 0, key$1; keys$1.length > j$1;) {
	      if (!((key$1 = keys$1[j$1++]) in $ArrayBuffer$1)) hide$6($ArrayBuffer$1, key$1, BaseBuffer[key$1]);
	    }

	    if (!LIBRARY$3) ArrayBufferProto.constructor = $ArrayBuffer$1;
	  } // iOS Safari 7.x bug


	  var view = new $DataView$1(new $ArrayBuffer$1(2));
	  var $setInt8 = $DataView$1[PROTOTYPE$4].setInt8;
	  view.setInt8(0, 2147483648);
	  view.setInt8(1, 2147483649);
	  if (view.getInt8(0) || !view.getInt8(1)) redefineAll$1($DataView$1[PROTOTYPE$4], {
	    setInt8: function setInt8(byteOffset, value) {
	      $setInt8.call(this, byteOffset, value << 24 >> 24);
	    },
	    setUint8: function setUint8(byteOffset, value) {
	      $setInt8.call(this, byteOffset, value << 24 >> 24);
	    }
	  }, true);
	}

	setToStringTag$2($ArrayBuffer$1, ARRAY_BUFFER$1);
	setToStringTag$2($DataView$1, DATA_VIEW);
	hide$6($DataView$1[PROTOTYPE$4], $typed$1.VIEW, true);
	exports[ARRAY_BUFFER$1] = $ArrayBuffer$1;
	exports[DATA_VIEW] = $DataView$1;

	var _typedBuffer = /*#__PURE__*/Object.freeze({

	});

	$export$1($export$1.G + $export$1.W + $export$1.F * !_typed.ABV, {
	  DataView: _typedBuffer.DataView
	});

	var support = {
	  searchParams: 'URLSearchParams' in self,
	  iterable: 'Symbol' in self && 'iterator' in Symbol,
	  blob: 'FileReader' in self && 'Blob' in self && function () {
	    try {
	      new Blob();
	      return true;
	    } catch (e) {
	      return false;
	    }
	  }(),
	  formData: 'FormData' in self,
	  arrayBuffer: 'ArrayBuffer' in self
	};

	function isDataView(obj) {
	  return obj && DataView.prototype.isPrototypeOf(obj);
	}

	if (support.arrayBuffer) {
	  var viewClasses = ['[object Int8Array]', '[object Uint8Array]', '[object Uint8ClampedArray]', '[object Int16Array]', '[object Uint16Array]', '[object Int32Array]', '[object Uint32Array]', '[object Float32Array]', '[object Float64Array]'];

	  var isArrayBufferView = ArrayBuffer.isView || function (obj) {
	    return obj && viewClasses.indexOf(Object.prototype.toString.call(obj)) > -1;
	  };
	}

	function normalizeName(name) {
	  if (typeof name !== 'string') {
	    name = String(name);
	  }

	  if (/[^a-z0-9\-#$%&'*+.^_`|~]/i.test(name)) {
	    throw new TypeError('Invalid character in header field name');
	  }

	  return name.toLowerCase();
	}

	function normalizeValue(value) {
	  if (typeof value !== 'string') {
	    value = String(value);
	  }

	  return value;
	} // Build a destructive iterator for the value list


	function iteratorFor(items) {
	  var iterator = {
	    next: function next() {
	      var value = items.shift();
	      return {
	        done: value === undefined,
	        value: value
	      };
	    }
	  };

	  if (support.iterable) {
	    iterator[Symbol.iterator] = function () {
	      return iterator;
	    };
	  }

	  return iterator;
	}

	function Headers(headers) {
	  this.map = {};

	  if (headers instanceof Headers) {
	    headers.forEach(function (value, name) {
	      this.append(name, value);
	    }, this);
	  } else if (Array.isArray(headers)) {
	    headers.forEach(function (header) {
	      this.append(header[0], header[1]);
	    }, this);
	  } else if (headers) {
	    Object.getOwnPropertyNames(headers).forEach(function (name) {
	      this.append(name, headers[name]);
	    }, this);
	  }
	}

	Headers.prototype.append = function (name, value) {
	  name = normalizeName(name);
	  value = normalizeValue(value);
	  var oldValue = this.map[name];
	  this.map[name] = oldValue ? oldValue + ', ' + value : value;
	};

	Headers.prototype['delete'] = function (name) {
	  delete this.map[normalizeName(name)];
	};

	Headers.prototype.get = function (name) {
	  name = normalizeName(name);
	  return this.has(name) ? this.map[name] : null;
	};

	Headers.prototype.has = function (name) {
	  return this.map.hasOwnProperty(normalizeName(name));
	};

	Headers.prototype.set = function (name, value) {
	  this.map[normalizeName(name)] = normalizeValue(value);
	};

	Headers.prototype.forEach = function (callback, thisArg) {
	  for (var name in this.map) {
	    if (this.map.hasOwnProperty(name)) {
	      callback.call(thisArg, this.map[name], name, this);
	    }
	  }
	};

	Headers.prototype.keys = function () {
	  var items = [];
	  this.forEach(function (value, name) {
	    items.push(name);
	  });
	  return iteratorFor(items);
	};

	Headers.prototype.values = function () {
	  var items = [];
	  this.forEach(function (value) {
	    items.push(value);
	  });
	  return iteratorFor(items);
	};

	Headers.prototype.entries = function () {
	  var items = [];
	  this.forEach(function (value, name) {
	    items.push([name, value]);
	  });
	  return iteratorFor(items);
	};

	if (support.iterable) {
	  Headers.prototype[Symbol.iterator] = Headers.prototype.entries;
	}

	function consumed(body) {
	  if (body.bodyUsed) {
	    return Promise.reject(new TypeError('Already read'));
	  }

	  body.bodyUsed = true;
	}

	function fileReaderReady(reader) {
	  return new Promise(function (resolve, reject) {
	    reader.onload = function () {
	      resolve(reader.result);
	    };

	    reader.onerror = function () {
	      reject(reader.error);
	    };
	  });
	}

	function readBlobAsArrayBuffer(blob) {
	  var reader = new FileReader();
	  var promise = fileReaderReady(reader);
	  reader.readAsArrayBuffer(blob);
	  return promise;
	}

	function readBlobAsText(blob) {
	  var reader = new FileReader();
	  var promise = fileReaderReady(reader);
	  reader.readAsText(blob);
	  return promise;
	}

	function readArrayBufferAsText(buf) {
	  var view = new Uint8Array(buf);
	  var chars = new Array(view.length);

	  for (var i = 0; i < view.length; i++) {
	    chars[i] = String.fromCharCode(view[i]);
	  }

	  return chars.join('');
	}

	function bufferClone(buf) {
	  if (buf.slice) {
	    return buf.slice(0);
	  } else {
	    var view = new Uint8Array(buf.byteLength);
	    view.set(new Uint8Array(buf));
	    return view.buffer;
	  }
	}

	function Body() {
	  this.bodyUsed = false;

	  this._initBody = function (body) {
	    this._bodyInit = body;

	    if (!body) {
	      this._bodyText = '';
	    } else if (typeof body === 'string') {
	      this._bodyText = body;
	    } else if (support.blob && Blob.prototype.isPrototypeOf(body)) {
	      this._bodyBlob = body;
	    } else if (support.formData && FormData.prototype.isPrototypeOf(body)) {
	      this._bodyFormData = body;
	    } else if (support.searchParams && URLSearchParams.prototype.isPrototypeOf(body)) {
	      this._bodyText = body.toString();
	    } else if (support.arrayBuffer && support.blob && isDataView(body)) {
	      this._bodyArrayBuffer = bufferClone(body.buffer); // IE 10-11 can't handle a DataView body.

	      this._bodyInit = new Blob([this._bodyArrayBuffer]);
	    } else if (support.arrayBuffer && (ArrayBuffer.prototype.isPrototypeOf(body) || isArrayBufferView(body))) {
	      this._bodyArrayBuffer = bufferClone(body);
	    } else {
	      this._bodyText = body = Object.prototype.toString.call(body);
	    }

	    if (!this.headers.get('content-type')) {
	      if (typeof body === 'string') {
	        this.headers.set('content-type', 'text/plain;charset=UTF-8');
	      } else if (this._bodyBlob && this._bodyBlob.type) {
	        this.headers.set('content-type', this._bodyBlob.type);
	      } else if (support.searchParams && URLSearchParams.prototype.isPrototypeOf(body)) {
	        this.headers.set('content-type', 'application/x-www-form-urlencoded;charset=UTF-8');
	      }
	    }
	  };

	  if (support.blob) {
	    this.blob = function () {
	      var rejected = consumed(this);

	      if (rejected) {
	        return rejected;
	      }

	      if (this._bodyBlob) {
	        return Promise.resolve(this._bodyBlob);
	      } else if (this._bodyArrayBuffer) {
	        return Promise.resolve(new Blob([this._bodyArrayBuffer]));
	      } else if (this._bodyFormData) {
	        throw new Error('could not read FormData body as blob');
	      } else {
	        return Promise.resolve(new Blob([this._bodyText]));
	      }
	    };

	    this.arrayBuffer = function () {
	      if (this._bodyArrayBuffer) {
	        return consumed(this) || Promise.resolve(this._bodyArrayBuffer);
	      } else {
	        return this.blob().then(readBlobAsArrayBuffer);
	      }
	    };
	  }

	  this.text = function () {
	    var rejected = consumed(this);

	    if (rejected) {
	      return rejected;
	    }

	    if (this._bodyBlob) {
	      return readBlobAsText(this._bodyBlob);
	    } else if (this._bodyArrayBuffer) {
	      return Promise.resolve(readArrayBufferAsText(this._bodyArrayBuffer));
	    } else if (this._bodyFormData) {
	      throw new Error('could not read FormData body as text');
	    } else {
	      return Promise.resolve(this._bodyText);
	    }
	  };

	  if (support.formData) {
	    this.formData = function () {
	      return this.text().then(decode);
	    };
	  }

	  this.json = function () {
	    return this.text().then(JSON.parse);
	  };

	  return this;
	} // HTTP methods whose capitalization should be normalized


	var methods = ['DELETE', 'GET', 'HEAD', 'OPTIONS', 'POST', 'PUT'];

	function normalizeMethod(method) {
	  var upcased = method.toUpperCase();
	  return methods.indexOf(upcased) > -1 ? upcased : method;
	}

	function Request(input, options) {
	  options = options || {};
	  var body = options.body;

	  if (input instanceof Request) {
	    if (input.bodyUsed) {
	      throw new TypeError('Already read');
	    }

	    this.url = input.url;
	    this.credentials = input.credentials;

	    if (!options.headers) {
	      this.headers = new Headers(input.headers);
	    }

	    this.method = input.method;
	    this.mode = input.mode;
	    this.signal = input.signal;

	    if (!body && input._bodyInit != null) {
	      body = input._bodyInit;
	      input.bodyUsed = true;
	    }
	  } else {
	    this.url = String(input);
	  }

	  this.credentials = options.credentials || this.credentials || 'same-origin';

	  if (options.headers || !this.headers) {
	    this.headers = new Headers(options.headers);
	  }

	  this.method = normalizeMethod(options.method || this.method || 'GET');
	  this.mode = options.mode || this.mode || null;
	  this.signal = options.signal || this.signal;
	  this.referrer = null;

	  if ((this.method === 'GET' || this.method === 'HEAD') && body) {
	    throw new TypeError('Body not allowed for GET or HEAD requests');
	  }

	  this._initBody(body);
	}

	Request.prototype.clone = function () {
	  return new Request(this, {
	    body: this._bodyInit
	  });
	};

	function decode(body) {
	  var form = new FormData();
	  body.trim().split('&').forEach(function (bytes) {
	    if (bytes) {
	      var split = bytes.split('=');
	      var name = split.shift().replace(/\+/g, ' ');
	      var value = split.join('=').replace(/\+/g, ' ');
	      form.append(decodeURIComponent(name), decodeURIComponent(value));
	    }
	  });
	  return form;
	}

	function parseHeaders(rawHeaders) {
	  var headers = new Headers(); // Replace instances of \r\n and \n followed by at least one space or horizontal tab with a space
	  // https://tools.ietf.org/html/rfc7230#section-3.2

	  var preProcessedHeaders = rawHeaders.replace(/\r?\n[\t ]+/g, ' ');
	  preProcessedHeaders.split(/\r?\n/).forEach(function (line) {
	    var parts = line.split(':');
	    var key = parts.shift().trim();

	    if (key) {
	      var value = parts.join(':').trim();
	      headers.append(key, value);
	    }
	  });
	  return headers;
	}

	Body.call(Request.prototype);
	function Response(bodyInit, options) {
	  if (!options) {
	    options = {};
	  }

	  this.type = 'default';
	  this.status = options.status === undefined ? 200 : options.status;
	  this.ok = this.status >= 200 && this.status < 300;
	  this.statusText = 'statusText' in options ? options.statusText : 'OK';
	  this.headers = new Headers(options.headers);
	  this.url = options.url || '';

	  this._initBody(bodyInit);
	}
	Body.call(Response.prototype);

	Response.prototype.clone = function () {
	  return new Response(this._bodyInit, {
	    status: this.status,
	    statusText: this.statusText,
	    headers: new Headers(this.headers),
	    url: this.url
	  });
	};

	Response.error = function () {
	  var response = new Response(null, {
	    status: 0,
	    statusText: ''
	  });
	  response.type = 'error';
	  return response;
	};

	var redirectStatuses = [301, 302, 303, 307, 308];

	Response.redirect = function (url, status) {
	  if (redirectStatuses.indexOf(status) === -1) {
	    throw new RangeError('Invalid status code');
	  }

	  return new Response(null, {
	    status: status,
	    headers: {
	      location: url
	    }
	  });
	};

	var DOMException = self.DOMException;

	try {
	  new DOMException();
	} catch (err) {
	  DOMException = function DOMException(message, name) {
	    this.message = message;
	    this.name = name;
	    var error = Error(message);
	    this.stack = error.stack;
	  };

	  DOMException.prototype = Object.create(Error.prototype);
	  DOMException.prototype.constructor = DOMException;
	}

	function fetch(input, init) {
	  return new Promise(function (resolve, reject) {
	    var request = new Request(input, init);

	    if (request.signal && request.signal.aborted) {
	      return reject(new DOMException('Aborted', 'AbortError'));
	    }

	    var xhr = new XMLHttpRequest();

	    function abortXhr() {
	      xhr.abort();
	    }

	    xhr.onload = function () {
	      var options = {
	        status: xhr.status,
	        statusText: xhr.statusText,
	        headers: parseHeaders(xhr.getAllResponseHeaders() || '')
	      };
	      options.url = 'responseURL' in xhr ? xhr.responseURL : options.headers.get('X-Request-URL');
	      var body = 'response' in xhr ? xhr.response : xhr.responseText;
	      resolve(new Response(body, options));
	    };

	    xhr.onerror = function () {
	      reject(new TypeError('Network request failed'));
	    };

	    xhr.ontimeout = function () {
	      reject(new TypeError('Network request failed'));
	    };

	    xhr.onabort = function () {
	      reject(new DOMException('Aborted', 'AbortError'));
	    };

	    xhr.open(request.method, request.url, true);

	    if (request.credentials === 'include') {
	      xhr.withCredentials = true;
	    } else if (request.credentials === 'omit') {
	      xhr.withCredentials = false;
	    }

	    if ('responseType' in xhr && support.blob) {
	      xhr.responseType = 'blob';
	    }

	    request.headers.forEach(function (value, name) {
	      xhr.setRequestHeader(name, value);
	    });

	    if (request.signal) {
	      request.signal.addEventListener('abort', abortXhr);

	      xhr.onreadystatechange = function () {
	        // DONE (success or failure)
	        if (xhr.readyState === 4) {
	          request.signal.removeEventListener('abort', abortXhr);
	        }
	      };
	    }

	    xhr.send(typeof request._bodyInit === 'undefined' ? null : request._bodyInit);
	  });
	}
	fetch.polyfill = true;

	if (!self.fetch) {
	  self.fetch = fetch;
	  self.Headers = Headers;
	  self.Request = Request;
	  self.Response = Response;
	}

	var global$8 = require('./_global');

	var has$3 = require('./_has');

	var cof$3 = require('./_cof');

	var inheritIfRequired = require('./_inherit-if-required');

	var toPrimitive$3 = require('./_to-primitive');

	var fails$5 = require('./_fails');

	var gOPN$4 = require('./_object-gopn').f;

	var gOPD$2 = require('./_object-gopd').f;

	var dP$6 = require('./_object-dp').f;

	var $trim = require('./_string-trim').trim;

	var NUMBER = 'Number';
	var $Number = global$8[NUMBER];
	var Base$1 = $Number;
	var proto$3 = $Number.prototype; // Opera ~12 has broken Object#toString

	var BROKEN_COF = cof$3(require('./_object-create')(proto$3)) == NUMBER;
	var TRIM = 'trim' in String.prototype; // 7.1.3 ToNumber(argument)

	var toNumber = function toNumber(argument) {
	  var it = toPrimitive$3(argument, false);

	  if (typeof it == 'string' && it.length > 2) {
	    it = TRIM ? it.trim() : $trim(it, 3);
	    var first = it.charCodeAt(0);
	    var third, radix, maxCode;

	    if (first === 43 || first === 45) {
	      third = it.charCodeAt(2);
	      if (third === 88 || third === 120) return NaN; // Number('+0x1') should be NaN, old V8 fix
	    } else if (first === 48) {
	      switch (it.charCodeAt(1)) {
	        case 66:
	        case 98:
	          radix = 2;
	          maxCode = 49;
	          break;
	        // fast equal /^0b[01]+$/i

	        case 79:
	        case 111:
	          radix = 8;
	          maxCode = 55;
	          break;
	        // fast equal /^0o[0-7]+$/i

	        default:
	          return +it;
	      }

	      for (var digits = it.slice(2), i = 0, l = digits.length, code; i < l; i++) {
	        code = digits.charCodeAt(i); // parseInt parses a string to a first unavailable symbol
	        // but ToNumber should return NaN if a string contains unavailable symbols

	        if (code < 48 || code > maxCode) return NaN;
	      }

	      return parseInt(digits, radix);
	    }
	  }

	  return +it;
	};

	if (!$Number(' 0o1') || !$Number('0b1') || $Number('+0x1')) {
	  $Number = function Number(value) {
	    var it = arguments.length < 1 ? 0 : value;
	    var that = this;
	    return that instanceof $Number // check on 1..constructor(foo) case
	    && (BROKEN_COF ? fails$5(function () {
	      proto$3.valueOf.call(that);
	    }) : cof$3(that) != NUMBER) ? inheritIfRequired(new Base$1(toNumber(it)), that, $Number) : toNumber(it);
	  };

	  for (var keys$2 = require('./_descriptors') ? gOPN$4(Base$1) : ( // ES3:
	  'MAX_VALUE,MIN_VALUE,NaN,NEGATIVE_INFINITY,POSITIVE_INFINITY,' + // ES6 (in case, if modules with ES6 Number statics required before):
	  'EPSILON,isFinite,isInteger,isNaN,isSafeInteger,MAX_SAFE_INTEGER,' + 'MIN_SAFE_INTEGER,parseFloat,parseInt,isInteger').split(','), j$2 = 0, key$2; keys$2.length > j$2; j$2++) {
	    if (has$3(Base$1, key$2 = keys$2[j$2]) && !has$3($Number, key$2)) {
	      dP$6($Number, key$2, gOPD$2(Base$1, key$2));
	    }
	  }

	  $Number.prototype = proto$3;
	  proto$3.constructor = $Number;

	  require('./_redefine')(global$8, NUMBER, $Number);
	}

	var $export$e = require('./_export');

	var $parseFloat = require('./_parse-float'); // 20.1.2.12 Number.parseFloat(string)


	$export$e($export$e.S + $export$e.F * (Number.parseFloat != $parseFloat), 'Number', {
	  parseFloat: $parseFloat
	});

	var $export$f = require('./_export');

	var $find = require('./_array-methods')(5);

	var KEY = 'find';
	var forced = true; // Shouldn't skip holes

	if (KEY in []) Array(1)[KEY](function () {
	  forced = false;
	});
	$export$f($export$f.P + $export$f.F * forced, 'Array', {
	  find: function find(callbackfn
	  /* , that = undefined */
	  ) {
	    return $find(this, callbackfn, arguments.length > 1 ? arguments[1] : undefined);
	  }
	});

	require('./_add-to-unscopables')(KEY);

	var $export$g = require('./_export');

	var $filter = require('./_array-methods')(2);

	$export$g($export$g.P + $export$g.F * !require('./_strict-method')([].filter, true), 'Array', {
	  // 22.1.3.7 / 15.4.4.20 Array.prototype.filter(callbackfn [, thisArg])
	  filter: function filter(callbackfn
	  /* , thisArg */
	  ) {
	    return $filter(this, callbackfn, arguments[1]);
	  }
	});

	// true  -> String#at
	// false -> String#codePointAt


	var _stringAt = function (TO_STRING) {
	  return function (that, pos) {
	    var s = String(_defined(that));
	    var i = _toInteger(pos);
	    var l = s.length;
	    var a, b;
	    if (i < 0 || i >= l) return TO_STRING ? '' : undefined;
	    a = s.charCodeAt(i);
	    return a < 0xd800 || a > 0xdbff || i + 1 === l || (b = s.charCodeAt(i + 1)) < 0xdc00 || b > 0xdfff ? TO_STRING ? s.charAt(i) : a : TO_STRING ? s.slice(i, i + 2) : (a - 0xd800 << 10) + (b - 0xdc00) + 0x10000;
	  };
	};

	var $at = _stringAt(true); // 21.1.3.27 String.prototype[@@iterator]()


	$iterDefine(String, 'String', function (iterated) {
	  this._t = String(iterated); // target

	  this._i = 0; // next index
	  // 21.1.5.2.1 %StringIteratorPrototype%.next()
	}, function () {
	  var O = this._t;
	  var index = this._i;
	  var point;
	  if (index >= O.length) return {
	    value: undefined,
	    done: true
	  };
	  point = $at(O, index);
	  this._i += point.length;
	  return {
	    value: point,
	    done: false
	  };
	});

	var ctx$3 = require('./_ctx');

	var $export$h = require('./_export');

	var toObject$3 = require('./_to-object');

	var call = require('./_iter-call');

	var isArrayIter$1 = require('./_is-array-iter');

	var toLength$3 = require('./_to-length');

	var createProperty = require('./_create-property');

	var getIterFn$1 = require('./core.get-iterator-method');

	$export$h($export$h.S + $export$h.F * !require('./_iter-detect')(function (iter) {
	}), 'Array', {
	  // 22.1.2.1 Array.from(arrayLike, mapfn = undefined, thisArg = undefined)
	  from: function from(arrayLike
	  /* , mapfn = undefined, thisArg = undefined */
	  ) {
	    var O = toObject$3(arrayLike);
	    var C = typeof this == 'function' ? this : Array;
	    var aLen = arguments.length;
	    var mapfn = aLen > 1 ? arguments[1] : undefined;
	    var mapping = mapfn !== undefined;
	    var index = 0;
	    var iterFn = getIterFn$1(O);
	    var length, result, step, iterator;
	    if (mapping) mapfn = ctx$3(mapfn, aLen > 2 ? arguments[2] : undefined, 2); // if object isn't iterable or it's array with default iterator - use simple case

	    if (iterFn != undefined && !(C == Array && isArrayIter$1(iterFn))) {
	      for (iterator = iterFn.call(O), result = new C(); !(step = iterator.next()).done; index++) {
	        createProperty(result, index, mapping ? call(iterator, mapfn, [step.value, index], true) : step.value);
	      }
	    } else {
	      length = toLength$3(O.length);

	      for (result = new C(length); length > index; index++) {
	        createProperty(result, index, mapping ? mapfn(O[index], index) : O[index]);
	      }
	    }

	    result.length = index;
	    return result;
	  }
	});

	function _isPlaceholder(a) {
	  return a != null && _typeof(a) === 'object' && a['@@functional/placeholder'] === true;
	}

	/**
	 * Optimized internal one-arity curry function.
	 *
	 * @private
	 * @category Function
	 * @param {Function} fn The function to curry.
	 * @return {Function} The curried function.
	 */

	function _curry1(fn) {
	  return function f1(a) {
	    if (arguments.length === 0 || _isPlaceholder(a)) {
	      return f1;
	    } else {
	      return fn.apply(this, arguments);
	    }
	  };
	}

	/**
	 * Returns a function that always returns the given value. Note that for
	 * non-primitives the value returned is a reference to the original value.
	 *
	 * This function is known as `const`, `constant`, or `K` (for K combinator) in
	 * other languages and libraries.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category Function
	 * @sig a -> (* -> a)
	 * @param {*} val The value to wrap in a function
	 * @return {Function} A Function :: * -> val.
	 * @example
	 *
	 *      var t = R.always('Tee');
	 *      t(); //=> 'Tee'
	 */

	var always =
	/*#__PURE__*/
	_curry1(function always(val) {
	  return function () {
	    return val;
	  };
	});

	/**
	 * A function that always returns `false`. Any passed in parameters are ignored.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.9.0
	 * @category Function
	 * @sig * -> Boolean
	 * @param {*}
	 * @return {Boolean}
	 * @see R.always, R.T
	 * @example
	 *
	 *      R.F(); //=> false
	 */

	var F =
	/*#__PURE__*/
	always(false);

	/**
	 * A function that always returns `true`. Any passed in parameters are ignored.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.9.0
	 * @category Function
	 * @sig * -> Boolean
	 * @param {*}
	 * @return {Boolean}
	 * @see R.always, R.F
	 * @example
	 *
	 *      R.T(); //=> true
	 */

	var T =
	/*#__PURE__*/
	always(true);

	/**
	 * A special placeholder value used to specify "gaps" within curried functions,
	 * allowing partial application of any combination of arguments, regardless of
	 * their positions.
	 *
	 * If `g` is a curried ternary function and `_` is `R.__`, the following are
	 * equivalent:
	 *
	 *   - `g(1, 2, 3)`
	 *   - `g(_, 2, 3)(1)`
	 *   - `g(_, _, 3)(1)(2)`
	 *   - `g(_, _, 3)(1, 2)`
	 *   - `g(_, 2, _)(1, 3)`
	 *   - `g(_, 2)(1)(3)`
	 *   - `g(_, 2)(1, 3)`
	 *   - `g(_, 2)(_, 3)(1)`
	 *
	 * @constant
	 * @memberOf R
	 * @since v0.6.0
	 * @category Function
	 * @example
	 *
	 *      var greet = R.replace('{name}', R.__, 'Hello, {name}!');
	 *      greet('Alice'); //=> 'Hello, Alice!'
	 */

	/**
	 * Optimized internal two-arity curry function.
	 *
	 * @private
	 * @category Function
	 * @param {Function} fn The function to curry.
	 * @return {Function} The curried function.
	 */

	function _curry2(fn) {
	  return function f2(a, b) {
	    switch (arguments.length) {
	      case 0:
	        return f2;

	      case 1:
	        return _isPlaceholder(a) ? f2 : _curry1(function (_b) {
	          return fn(a, _b);
	        });

	      default:
	        return _isPlaceholder(a) && _isPlaceholder(b) ? f2 : _isPlaceholder(a) ? _curry1(function (_a) {
	          return fn(_a, b);
	        }) : _isPlaceholder(b) ? _curry1(function (_b) {
	          return fn(a, _b);
	        }) : fn(a, b);
	    }
	  };
	}

	/**
	 * Adds two values.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category Math
	 * @sig Number -> Number -> Number
	 * @param {Number} a
	 * @param {Number} b
	 * @return {Number}
	 * @see R.subtract
	 * @example
	 *
	 *      R.add(2, 3);       //=>  5
	 *      R.add(7)(10);      //=> 17
	 */

	var add =
	/*#__PURE__*/
	_curry2(function add(a, b) {
	  return Number(a) + Number(b);
	});

	/**
	 * Private `concat` function to merge two array-like objects.
	 *
	 * @private
	 * @param {Array|Arguments} [set1=[]] An array-like object.
	 * @param {Array|Arguments} [set2=[]] An array-like object.
	 * @return {Array} A new, merged array.
	 * @example
	 *
	 *      _concat([4, 5, 6], [1, 2, 3]); //=> [4, 5, 6, 1, 2, 3]
	 */
	function _concat(set1, set2) {
	  set1 = set1 || [];
	  set2 = set2 || [];
	  var idx;
	  var len1 = set1.length;
	  var len2 = set2.length;
	  var result = [];
	  idx = 0;

	  while (idx < len1) {
	    result[result.length] = set1[idx];
	    idx += 1;
	  }

	  idx = 0;

	  while (idx < len2) {
	    result[result.length] = set2[idx];
	    idx += 1;
	  }

	  return result;
	}

	function _arity(n, fn) {
	  /* eslint-disable no-unused-vars */
	  switch (n) {
	    case 0:
	      return function () {
	        return fn.apply(this, arguments);
	      };

	    case 1:
	      return function (a0) {
	        return fn.apply(this, arguments);
	      };

	    case 2:
	      return function (a0, a1) {
	        return fn.apply(this, arguments);
	      };

	    case 3:
	      return function (a0, a1, a2) {
	        return fn.apply(this, arguments);
	      };

	    case 4:
	      return function (a0, a1, a2, a3) {
	        return fn.apply(this, arguments);
	      };

	    case 5:
	      return function (a0, a1, a2, a3, a4) {
	        return fn.apply(this, arguments);
	      };

	    case 6:
	      return function (a0, a1, a2, a3, a4, a5) {
	        return fn.apply(this, arguments);
	      };

	    case 7:
	      return function (a0, a1, a2, a3, a4, a5, a6) {
	        return fn.apply(this, arguments);
	      };

	    case 8:
	      return function (a0, a1, a2, a3, a4, a5, a6, a7) {
	        return fn.apply(this, arguments);
	      };

	    case 9:
	      return function (a0, a1, a2, a3, a4, a5, a6, a7, a8) {
	        return fn.apply(this, arguments);
	      };

	    case 10:
	      return function (a0, a1, a2, a3, a4, a5, a6, a7, a8, a9) {
	        return fn.apply(this, arguments);
	      };

	    default:
	      throw new Error('First argument to _arity must be a non-negative integer no greater than ten');
	  }
	}

	/**
	 * Internal curryN function.
	 *
	 * @private
	 * @category Function
	 * @param {Number} length The arity of the curried function.
	 * @param {Array} received An array of arguments received thus far.
	 * @param {Function} fn The function to curry.
	 * @return {Function} The curried function.
	 */

	function _curryN(length, received, fn) {
	  return function () {
	    var combined = [];
	    var argsIdx = 0;
	    var left = length;
	    var combinedIdx = 0;

	    while (combinedIdx < received.length || argsIdx < arguments.length) {
	      var result;

	      if (combinedIdx < received.length && (!_isPlaceholder(received[combinedIdx]) || argsIdx >= arguments.length)) {
	        result = received[combinedIdx];
	      } else {
	        result = arguments[argsIdx];
	        argsIdx += 1;
	      }

	      combined[combinedIdx] = result;

	      if (!_isPlaceholder(result)) {
	        left -= 1;
	      }

	      combinedIdx += 1;
	    }

	    return left <= 0 ? fn.apply(this, combined) : _arity(left, _curryN(length, combined, fn));
	  };
	}

	/**
	 * Returns a curried equivalent of the provided function, with the specified
	 * arity. The curried function has two unusual capabilities. First, its
	 * arguments needn't be provided one at a time. If `g` is `R.curryN(3, f)`, the
	 * following are equivalent:
	 *
	 *   - `g(1)(2)(3)`
	 *   - `g(1)(2, 3)`
	 *   - `g(1, 2)(3)`
	 *   - `g(1, 2, 3)`
	 *
	 * Secondly, the special placeholder value [`R.__`](#__) may be used to specify
	 * "gaps", allowing partial application of any combination of arguments,
	 * regardless of their positions. If `g` is as above and `_` is [`R.__`](#__),
	 * the following are equivalent:
	 *
	 *   - `g(1, 2, 3)`
	 *   - `g(_, 2, 3)(1)`
	 *   - `g(_, _, 3)(1)(2)`
	 *   - `g(_, _, 3)(1, 2)`
	 *   - `g(_, 2)(1)(3)`
	 *   - `g(_, 2)(1, 3)`
	 *   - `g(_, 2)(_, 3)(1)`
	 *
	 * @func
	 * @memberOf R
	 * @since v0.5.0
	 * @category Function
	 * @sig Number -> (* -> a) -> (* -> a)
	 * @param {Number} length The arity for the returned function.
	 * @param {Function} fn The function to curry.
	 * @return {Function} A new, curried function.
	 * @see R.curry
	 * @example
	 *
	 *      var sumArgs = (...args) => R.sum(args);
	 *
	 *      var curriedAddFourNumbers = R.curryN(4, sumArgs);
	 *      var f = curriedAddFourNumbers(1, 2);
	 *      var g = f(3);
	 *      g(4); //=> 10
	 */

	var curryN =
	/*#__PURE__*/
	_curry2(function curryN(length, fn) {
	  if (length === 1) {
	    return _curry1(fn);
	  }

	  return _arity(length, _curryN(length, [], fn));
	});

	/**
	 * Optimized internal three-arity curry function.
	 *
	 * @private
	 * @category Function
	 * @param {Function} fn The function to curry.
	 * @return {Function} The curried function.
	 */

	function _curry3(fn) {
	  return function f3(a, b, c) {
	    switch (arguments.length) {
	      case 0:
	        return f3;

	      case 1:
	        return _isPlaceholder(a) ? f3 : _curry2(function (_b, _c) {
	          return fn(a, _b, _c);
	        });

	      case 2:
	        return _isPlaceholder(a) && _isPlaceholder(b) ? f3 : _isPlaceholder(a) ? _curry2(function (_a, _c) {
	          return fn(_a, b, _c);
	        }) : _isPlaceholder(b) ? _curry2(function (_b, _c) {
	          return fn(a, _b, _c);
	        }) : _curry1(function (_c) {
	          return fn(a, b, _c);
	        });

	      default:
	        return _isPlaceholder(a) && _isPlaceholder(b) && _isPlaceholder(c) ? f3 : _isPlaceholder(a) && _isPlaceholder(b) ? _curry2(function (_a, _b) {
	          return fn(_a, _b, c);
	        }) : _isPlaceholder(a) && _isPlaceholder(c) ? _curry2(function (_a, _c) {
	          return fn(_a, b, _c);
	        }) : _isPlaceholder(b) && _isPlaceholder(c) ? _curry2(function (_b, _c) {
	          return fn(a, _b, _c);
	        }) : _isPlaceholder(a) ? _curry1(function (_a) {
	          return fn(_a, b, c);
	        }) : _isPlaceholder(b) ? _curry1(function (_b) {
	          return fn(a, _b, c);
	        }) : _isPlaceholder(c) ? _curry1(function (_c) {
	          return fn(a, b, _c);
	        }) : fn(a, b, c);
	    }
	  };
	}

	/**
	 * Tests whether or not an object is an array.
	 *
	 * @private
	 * @param {*} val The object to test.
	 * @return {Boolean} `true` if `val` is an array, `false` otherwise.
	 * @example
	 *
	 *      _isArray([]); //=> true
	 *      _isArray(null); //=> false
	 *      _isArray({}); //=> false
	 */
	var _isArray$1 = Array.isArray || function _isArray(val) {
	  return val != null && val.length >= 0 && Object.prototype.toString.call(val) === '[object Array]';
	};

	function _isTransformer(obj) {
	  return typeof obj['@@transducer/step'] === 'function';
	}

	/**
	 * Returns a function that dispatches with different strategies based on the
	 * object in list position (last argument). If it is an array, executes [fn].
	 * Otherwise, if it has a function with one of the given method names, it will
	 * execute that function (functor case). Otherwise, if it is a transformer,
	 * uses transducer [xf] to return a new transformer (transducer case).
	 * Otherwise, it will default to executing [fn].
	 *
	 * @private
	 * @param {Array} methodNames properties to check for a custom implementation
	 * @param {Function} xf transducer to initialize if object is transformer
	 * @param {Function} fn default ramda implementation
	 * @return {Function} A function that dispatches on object in list position
	 */

	function _dispatchable(methodNames, xf, fn) {
	  return function () {
	    if (arguments.length === 0) {
	      return fn();
	    }

	    var args = Array.prototype.slice.call(arguments, 0);
	    var obj = args.pop();

	    if (!_isArray$1(obj)) {
	      var idx = 0;

	      while (idx < methodNames.length) {
	        if (typeof obj[methodNames[idx]] === 'function') {
	          return obj[methodNames[idx]].apply(obj, args);
	        }

	        idx += 1;
	      }

	      if (_isTransformer(obj)) {
	        var transducer = xf.apply(null, args);
	        return transducer(obj);
	      }
	    }

	    return fn.apply(this, arguments);
	  };
	}

	function _reduced(x) {
	  return x && x['@@transducer/reduced'] ? x : {
	    '@@transducer/value': x,
	    '@@transducer/reduced': true
	  };
	}

	var _xfBase = {
	  init: function init() {
	    return this.xf['@@transducer/init']();
	  },
	  result: function result(_result) {
	    return this.xf['@@transducer/result'](_result);
	  }
	};

	/**
	 * Returns the larger of its two arguments.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category Relation
	 * @sig Ord a => a -> a -> a
	 * @param {*} a
	 * @param {*} b
	 * @return {*}
	 * @see R.maxBy, R.min
	 * @example
	 *
	 *      R.max(789, 123); //=> 789
	 *      R.max('a', 'b'); //=> 'b'
	 */

	var max$1 =
	/*#__PURE__*/
	_curry2(function max(a, b) {
	  return b > a ? b : a;
	});

	function _map(fn, functor) {
	  var idx = 0;
	  var len = functor.length;
	  var result = Array(len);

	  while (idx < len) {
	    result[idx] = fn(functor[idx]);
	    idx += 1;
	  }

	  return result;
	}

	function _isString(x) {
	  return Object.prototype.toString.call(x) === '[object String]';
	}

	/**
	 * Tests whether or not an object is similar to an array.
	 *
	 * @private
	 * @category Type
	 * @category List
	 * @sig * -> Boolean
	 * @param {*} x The object to test.
	 * @return {Boolean} `true` if `x` has a numeric length property and extreme indices defined; `false` otherwise.
	 * @example
	 *
	 *      _isArrayLike([]); //=> true
	 *      _isArrayLike(true); //=> false
	 *      _isArrayLike({}); //=> false
	 *      _isArrayLike({length: 10}); //=> false
	 *      _isArrayLike({0: 'zero', 9: 'nine', length: 10}); //=> true
	 */

	var _isArrayLike =
	/*#__PURE__*/
	_curry1(function isArrayLike(x) {
	  if (_isArray$1(x)) {
	    return true;
	  }

	  if (!x) {
	    return false;
	  }

	  if (_typeof(x) !== 'object') {
	    return false;
	  }

	  if (_isString(x)) {
	    return false;
	  }

	  if (x.nodeType === 1) {
	    return !!x.length;
	  }

	  if (x.length === 0) {
	    return true;
	  }

	  if (x.length > 0) {
	    return x.hasOwnProperty(0) && x.hasOwnProperty(x.length - 1);
	  }

	  return false;
	});

	var XWrap =
	/*#__PURE__*/
	function () {
	  function XWrap(fn) {
	    this.f = fn;
	  }

	  XWrap.prototype['@@transducer/init'] = function () {
	    throw new Error('init not implemented on XWrap');
	  };

	  XWrap.prototype['@@transducer/result'] = function (acc) {
	    return acc;
	  };

	  XWrap.prototype['@@transducer/step'] = function (acc, x) {
	    return this.f(acc, x);
	  };

	  return XWrap;
	}();

	function _xwrap(fn) {
	  return new XWrap(fn);
	}

	/**
	 * Creates a function that is bound to a context.
	 * Note: `R.bind` does not provide the additional argument-binding capabilities of
	 * [Function.prototype.bind](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Function/bind).
	 *
	 * @func
	 * @memberOf R
	 * @since v0.6.0
	 * @category Function
	 * @category Object
	 * @sig (* -> *) -> {*} -> (* -> *)
	 * @param {Function} fn The function to bind to context
	 * @param {Object} thisObj The context to bind `fn` to
	 * @return {Function} A function that will execute in the context of `thisObj`.
	 * @see R.partial
	 * @example
	 *
	 *      var log = R.bind(console.log, console);
	 *      R.pipe(R.assoc('a', 2), R.tap(log), R.assoc('a', 3))({a: 1}); //=> {a: 3}
	 *      // logs {a: 2}
	 * @symb R.bind(f, o)(a, b) = f.call(o, a, b)
	 */

	var bind =
	/*#__PURE__*/
	_curry2(function bind(fn, thisObj) {
	  return _arity(fn.length, function () {
	    return fn.apply(thisObj, arguments);
	  });
	});

	function _arrayReduce(xf, acc, list) {
	  var idx = 0;
	  var len = list.length;

	  while (idx < len) {
	    acc = xf['@@transducer/step'](acc, list[idx]);

	    if (acc && acc['@@transducer/reduced']) {
	      acc = acc['@@transducer/value'];
	      break;
	    }

	    idx += 1;
	  }

	  return xf['@@transducer/result'](acc);
	}

	function _iterableReduce(xf, acc, iter) {
	  var step = iter.next();

	  while (!step.done) {
	    acc = xf['@@transducer/step'](acc, step.value);

	    if (acc && acc['@@transducer/reduced']) {
	      acc = acc['@@transducer/value'];
	      break;
	    }

	    step = iter.next();
	  }

	  return xf['@@transducer/result'](acc);
	}

	function _methodReduce(xf, acc, obj, methodName) {
	  return xf['@@transducer/result'](obj[methodName](bind(xf['@@transducer/step'], xf), acc));
	}

	var symIterator = typeof Symbol !== 'undefined' ? Symbol.iterator : '@@iterator';
	function _reduce(fn, acc, list) {
	  if (typeof fn === 'function') {
	    fn = _xwrap(fn);
	  }

	  if (_isArrayLike(list)) {
	    return _arrayReduce(fn, acc, list);
	  }

	  if (typeof list['fantasy-land/reduce'] === 'function') {
	    return _methodReduce(fn, acc, list, 'fantasy-land/reduce');
	  }

	  if (list[symIterator] != null) {
	    return _iterableReduce(fn, acc, list[symIterator]());
	  }

	  if (typeof list.next === 'function') {
	    return _iterableReduce(fn, acc, list);
	  }

	  if (typeof list.reduce === 'function') {
	    return _methodReduce(fn, acc, list, 'reduce');
	  }

	  throw new TypeError('reduce: list must be array or iterable');
	}

	var XMap =
	/*#__PURE__*/
	function () {
	  function XMap(f, xf) {
	    this.xf = xf;
	    this.f = f;
	  }

	  XMap.prototype['@@transducer/init'] = _xfBase.init;
	  XMap.prototype['@@transducer/result'] = _xfBase.result;

	  XMap.prototype['@@transducer/step'] = function (result, input) {
	    return this.xf['@@transducer/step'](result, this.f(input));
	  };

	  return XMap;
	}();

	var _xmap =
	/*#__PURE__*/
	_curry2(function _xmap(f, xf) {
	  return new XMap(f, xf);
	});

	function _has$1(prop, obj) {
	  return Object.prototype.hasOwnProperty.call(obj, prop);
	}

	var toString$1 = Object.prototype.toString;

	var _isArguments = function _isArguments() {
	  return toString$1.call(arguments) === '[object Arguments]' ? function _isArguments(x) {
	    return toString$1.call(x) === '[object Arguments]';
	  } : function _isArguments(x) {
	    return _has$1('callee', x);
	  };
	};

	var hasEnumBug = !
	/*#__PURE__*/
	{
	  toString: null
	}.propertyIsEnumerable('toString');
	var nonEnumerableProps = ['constructor', 'valueOf', 'isPrototypeOf', 'toString', 'propertyIsEnumerable', 'hasOwnProperty', 'toLocaleString']; // Safari bug

	var hasArgsEnumBug =
	/*#__PURE__*/
	function () {

	  return arguments.propertyIsEnumerable('length');
	}();

	var contains = function contains(list, item) {
	  var idx = 0;

	  while (idx < list.length) {
	    if (list[idx] === item) {
	      return true;
	    }

	    idx += 1;
	  }

	  return false;
	};
	/**
	 * Returns a list containing the names of all the enumerable own properties of
	 * the supplied object.
	 * Note that the order of the output array is not guaranteed to be consistent
	 * across different JS platforms.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category Object
	 * @sig {k: v} -> [k]
	 * @param {Object} obj The object to extract properties from
	 * @return {Array} An array of the object's own properties.
	 * @see R.keysIn, R.values
	 * @example
	 *
	 *      R.keys({a: 1, b: 2, c: 3}); //=> ['a', 'b', 'c']
	 */


	var _keys = typeof Object.keys === 'function' && !hasArgsEnumBug ? function keys(obj) {
	  return Object(obj) !== obj ? [] : Object.keys(obj);
	} : function keys(obj) {
	  if (Object(obj) !== obj) {
	    return [];
	  }

	  var prop, nIdx;
	  var ks = [];

	  var checkArgsLength = hasArgsEnumBug && _isArguments(obj);

	  for (prop in obj) {
	    if (_has$1(prop, obj) && (!checkArgsLength || prop !== 'length')) {
	      ks[ks.length] = prop;
	    }
	  }

	  if (hasEnumBug) {
	    nIdx = nonEnumerableProps.length - 1;

	    while (nIdx >= 0) {
	      prop = nonEnumerableProps[nIdx];

	      if (_has$1(prop, obj) && !contains(ks, prop)) {
	        ks[ks.length] = prop;
	      }

	      nIdx -= 1;
	    }
	  }

	  return ks;
	};

	var keys$3 =
	/*#__PURE__*/
	_curry1(_keys);

	/**
	 * Takes a function and
	 * a [functor](https://github.com/fantasyland/fantasy-land#functor),
	 * applies the function to each of the functor's values, and returns
	 * a functor of the same shape.
	 *
	 * Ramda provides suitable `map` implementations for `Array` and `Object`,
	 * so this function may be applied to `[1, 2, 3]` or `{x: 1, y: 2, z: 3}`.
	 *
	 * Dispatches to the `map` method of the second argument, if present.
	 *
	 * Acts as a transducer if a transformer is given in list position.
	 *
	 * Also treats functions as functors and will compose them together.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category List
	 * @sig Functor f => (a -> b) -> f a -> f b
	 * @param {Function} fn The function to be called on every element of the input `list`.
	 * @param {Array} list The list to be iterated over.
	 * @return {Array} The new list.
	 * @see R.transduce, R.addIndex
	 * @example
	 *
	 *      var double = x => x * 2;
	 *
	 *      R.map(double, [1, 2, 3]); //=> [2, 4, 6]
	 *
	 *      R.map(double, {x: 1, y: 2, z: 3}); //=> {x: 2, y: 4, z: 6}
	 * @symb R.map(f, [a, b]) = [f(a), f(b)]
	 * @symb R.map(f, { x: a, y: b }) = { x: f(a), y: f(b) }
	 * @symb R.map(f, functor_o) = functor_o.map(f)
	 */

	var map =
	/*#__PURE__*/
	_curry2(
	/*#__PURE__*/
	_dispatchable(['fantasy-land/map', 'map'], _xmap, function map(fn, functor) {
	  switch (Object.prototype.toString.call(functor)) {
	    case '[object Function]':
	      return curryN(functor.length, function () {
	        return fn.call(this, functor.apply(this, arguments));
	      });

	    case '[object Object]':
	      return _reduce(function (acc, key) {
	        acc[key] = fn(functor[key]);
	        return acc;
	      }, {}, keys$3(functor));

	    default:
	      return _map(fn, functor);
	  }
	}));

	/**
	 * Retrieve the value at a given path.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.2.0
	 * @category Object
	 * @typedefn Idx = String | Int
	 * @sig [Idx] -> {a} -> a | Undefined
	 * @param {Array} path The path to use.
	 * @param {Object} obj The object to retrieve the nested property from.
	 * @return {*} The data at `path`.
	 * @see R.prop
	 * @example
	 *
	 *      R.path(['a', 'b'], {a: {b: 2}}); //=> 2
	 *      R.path(['a', 'b'], {c: {b: 2}}); //=> undefined
	 */

	var path =
	/*#__PURE__*/
	_curry2(function path(paths, obj) {
	  var val = obj;
	  var idx = 0;

	  while (idx < paths.length) {
	    if (val == null) {
	      return;
	    }

	    val = val[paths[idx]];
	    idx += 1;
	  }

	  return val;
	});

	/**
	 * Returns a function that when supplied an object returns the indicated
	 * property of that object, if it exists.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category Object
	 * @sig s -> {s: a} -> a | Undefined
	 * @param {String} p The property name
	 * @param {Object} obj The object to query
	 * @return {*} The value at `obj.p`.
	 * @see R.path
	 * @example
	 *
	 *      R.prop('x', {x: 100}); //=> 100
	 *      R.prop('x', {}); //=> undefined
	 */

	var prop =
	/*#__PURE__*/
	_curry2(function prop(p, obj) {
	  return path([p], obj);
	});

	/**
	 * Returns a new list by plucking the same named property off all objects in
	 * the list supplied.
	 *
	 * `pluck` will work on
	 * any [functor](https://github.com/fantasyland/fantasy-land#functor) in
	 * addition to arrays, as it is equivalent to `R.map(R.prop(k), f)`.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category List
	 * @sig Functor f => k -> f {k: v} -> f v
	 * @param {Number|String} key The key name to pluck off of each object.
	 * @param {Array} f The array or functor to consider.
	 * @return {Array} The list of values for the given key.
	 * @see R.props
	 * @example
	 *
	 *      R.pluck('a')([{a: 1}, {a: 2}]); //=> [1, 2]
	 *      R.pluck(0)([[1, 2], [3, 4]]);   //=> [1, 3]
	 *      R.pluck('val', {a: {val: 3}, b: {val: 5}}); //=> {a: 3, b: 5}
	 * @symb R.pluck('x', [{x: 1, y: 2}, {x: 3, y: 4}, {x: 5, y: 6}]) = [1, 3, 5]
	 * @symb R.pluck(0, [[1, 2], [3, 4], [5, 6]]) = [1, 3, 5]
	 */

	var pluck =
	/*#__PURE__*/
	_curry2(function pluck(p, list) {
	  return map(prop(p), list);
	});

	/**
	 * Returns a single item by iterating through the list, successively calling
	 * the iterator function and passing it an accumulator value and the current
	 * value from the array, and then passing the result to the next call.
	 *
	 * The iterator function receives two values: *(acc, value)*. It may use
	 * [`R.reduced`](#reduced) to shortcut the iteration.
	 *
	 * The arguments' order of [`reduceRight`](#reduceRight)'s iterator function
	 * is *(value, acc)*.
	 *
	 * Note: `R.reduce` does not skip deleted or unassigned indices (sparse
	 * arrays), unlike the native `Array.prototype.reduce` method. For more details
	 * on this behavior, see:
	 * https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array/reduce#Description
	 *
	 * Dispatches to the `reduce` method of the third argument, if present. When
	 * doing so, it is up to the user to handle the [`R.reduced`](#reduced)
	 * shortcuting, as this is not implemented by `reduce`.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category List
	 * @sig ((a, b) -> a) -> a -> [b] -> a
	 * @param {Function} fn The iterator function. Receives two values, the accumulator and the
	 *        current element from the array.
	 * @param {*} acc The accumulator value.
	 * @param {Array} list The list to iterate over.
	 * @return {*} The final, accumulated value.
	 * @see R.reduced, R.addIndex, R.reduceRight
	 * @example
	 *
	 *      R.reduce(R.subtract, 0, [1, 2, 3, 4]) // => ((((0 - 1) - 2) - 3) - 4) = -10
	 *      //          -               -10
	 *      //         / \              / \
	 *      //        -   4           -6   4
	 *      //       / \              / \
	 *      //      -   3   ==>     -3   3
	 *      //     / \              / \
	 *      //    -   2           -1   2
	 *      //   / \              / \
	 *      //  0   1            0   1
	 *
	 * @symb R.reduce(f, a, [b, c, d]) = f(f(f(a, b), c), d)
	 */

	var reduce =
	/*#__PURE__*/
	_curry3(_reduce);

	/**
	 * ap applies a list of functions to a list of values.
	 *
	 * Dispatches to the `ap` method of the second argument, if present. Also
	 * treats curried functions as applicatives.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.3.0
	 * @category Function
	 * @sig [a -> b] -> [a] -> [b]
	 * @sig Apply f => f (a -> b) -> f a -> f b
	 * @sig (a -> b -> c) -> (a -> b) -> (a -> c)
	 * @param {*} applyF
	 * @param {*} applyX
	 * @return {*}
	 * @example
	 *
	 *      R.ap([R.multiply(2), R.add(3)], [1,2,3]); //=> [2, 4, 6, 4, 5, 6]
	 *      R.ap([R.concat('tasty '), R.toUpper], ['pizza', 'salad']); //=> ["tasty pizza", "tasty salad", "PIZZA", "SALAD"]
	 *
	 *      // R.ap can also be used as S combinator
	 *      // when only two functions are passed
	 *      R.ap(R.concat, R.toUpper)('Ramda') //=> 'RamdaRAMDA'
	 * @symb R.ap([f, g], [a, b]) = [f(a), f(b), g(a), g(b)]
	 */

	var ap =
	/*#__PURE__*/
	_curry2(function ap(applyF, applyX) {
	  return typeof applyX['fantasy-land/ap'] === 'function' ? applyX['fantasy-land/ap'](applyF) : typeof applyF.ap === 'function' ? applyF.ap(applyX) : typeof applyF === 'function' ? function (x) {
	    return applyF(x)(applyX(x));
	  } : // else
	  _reduce(function (acc, f) {
	    return _concat(acc, map(f, applyX));
	  }, [], applyF);
	});

	// 20.1.2.3 Number.isInteger(number)


	var floor$2 = Math.floor;

	var _isInteger = function isInteger(it) {
	  return !isObject(it) && isFinite(it) && floor$2(it) === it;
	};

	// 20.1.2.3 Number.isInteger(number)


	$export$1($export$1.S, 'Number', {
	  isInteger: _isInteger
	});

	function _isFunction(x) {
	  return Object.prototype.toString.call(x) === '[object Function]';
	}

	/**
	 * "lifts" a function to be the specified arity, so that it may "map over" that
	 * many lists, Functions or other objects that satisfy the [FantasyLand Apply spec](https://github.com/fantasyland/fantasy-land#apply).
	 *
	 * @func
	 * @memberOf R
	 * @since v0.7.0
	 * @category Function
	 * @sig Number -> (*... -> *) -> ([*]... -> [*])
	 * @param {Function} fn The function to lift into higher context
	 * @return {Function} The lifted function.
	 * @see R.lift, R.ap
	 * @example
	 *
	 *      var madd3 = R.liftN(3, (...args) => R.sum(args));
	 *      madd3([1,2,3], [1,2,3], [1]); //=> [3, 4, 5, 4, 5, 6, 5, 6, 7]
	 */

	var liftN =
	/*#__PURE__*/
	_curry2(function liftN(arity, fn) {
	  var lifted = curryN(arity, fn);
	  return curryN(arity, function () {
	    return _reduce(ap, map(lifted, arguments[0]), Array.prototype.slice.call(arguments, 1));
	  });
	});

	/**
	 * "lifts" a function of arity > 1 so that it may "map over" a list, Function or other
	 * object that satisfies the [FantasyLand Apply spec](https://github.com/fantasyland/fantasy-land#apply).
	 *
	 * @func
	 * @memberOf R
	 * @since v0.7.0
	 * @category Function
	 * @sig (*... -> *) -> ([*]... -> [*])
	 * @param {Function} fn The function to lift into higher context
	 * @return {Function} The lifted function.
	 * @see R.liftN
	 * @example
	 *
	 *      var madd3 = R.lift((a, b, c) => a + b + c);
	 *
	 *      madd3([1,2,3], [1,2,3], [1]); //=> [3, 4, 5, 4, 5, 6, 5, 6, 7]
	 *
	 *      var madd5 = R.lift((a, b, c, d, e) => a + b + c + d + e);
	 *
	 *      madd5([1,2], [3], [4, 5], [6], [7, 8]); //=> [21, 22, 22, 23, 22, 23, 23, 24]
	 */

	var lift =
	/*#__PURE__*/
	_curry1(function lift(fn) {
	  return liftN(fn.length, fn);
	});

	/**
	 * Returns a curried equivalent of the provided function. The curried function
	 * has two unusual capabilities. First, its arguments needn't be provided one
	 * at a time. If `f` is a ternary function and `g` is `R.curry(f)`, the
	 * following are equivalent:
	 *
	 *   - `g(1)(2)(3)`
	 *   - `g(1)(2, 3)`
	 *   - `g(1, 2)(3)`
	 *   - `g(1, 2, 3)`
	 *
	 * Secondly, the special placeholder value [`R.__`](#__) may be used to specify
	 * "gaps", allowing partial application of any combination of arguments,
	 * regardless of their positions. If `g` is as above and `_` is [`R.__`](#__),
	 * the following are equivalent:
	 *
	 *   - `g(1, 2, 3)`
	 *   - `g(_, 2, 3)(1)`
	 *   - `g(_, _, 3)(1)(2)`
	 *   - `g(_, _, 3)(1, 2)`
	 *   - `g(_, 2)(1)(3)`
	 *   - `g(_, 2)(1, 3)`
	 *   - `g(_, 2)(_, 3)(1)`
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category Function
	 * @sig (* -> a) -> (* -> a)
	 * @param {Function} fn The function to curry.
	 * @return {Function} A new, curried function.
	 * @see R.curryN
	 * @example
	 *
	 *      var addFourNumbers = (a, b, c, d) => a + b + c + d;
	 *
	 *      var curriedAddFourNumbers = R.curry(addFourNumbers);
	 *      var f = curriedAddFourNumbers(1, 2);
	 *      var g = f(3);
	 *      g(4); //=> 10
	 */

	var curry =
	/*#__PURE__*/
	_curry1(function curry(fn) {
	  return curryN(fn.length, fn);
	});

	/**
	 * Returns the result of calling its first argument with the remaining
	 * arguments. This is occasionally useful as a converging function for
	 * [`R.converge`](#converge): the first branch can produce a function while the
	 * remaining branches produce values to be passed to that function as its
	 * arguments.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.9.0
	 * @category Function
	 * @sig (*... -> a),*... -> a
	 * @param {Function} fn The function to apply to the remaining arguments.
	 * @param {...*} args Any number of positional arguments.
	 * @return {*}
	 * @see R.apply
	 * @example
	 *
	 *      R.call(R.add, 1, 2); //=> 3
	 *
	 *      var indentN = R.pipe(R.repeat(' '),
	 *                           R.join(''),
	 *                           R.replace(/^(?!$)/gm));
	 *
	 *      var format = R.converge(R.call, [
	 *                                  R.pipe(R.prop('indent'), indentN),
	 *                                  R.prop('value')
	 *                              ]);
	 *
	 *      format({indent: 2, value: 'foo\nbar\nbaz\n'}); //=> '  foo\n  bar\n  baz\n'
	 * @symb R.call(f, a, b) = f(a, b)
	 */

	var call$1 =
	/*#__PURE__*/
	curry(function call(fn) {
	  return fn.apply(this, Array.prototype.slice.call(arguments, 1));
	});

	/**
	 * `_makeFlat` is a helper function that returns a one-level or fully recursive
	 * function based on the flag passed in.
	 *
	 * @private
	 */

	function _makeFlat(recursive) {
	  return function flatt(list) {
	    var value, jlen, j;
	    var result = [];
	    var idx = 0;
	    var ilen = list.length;

	    while (idx < ilen) {
	      if (_isArrayLike(list[idx])) {
	        value = recursive ? flatt(list[idx]) : list[idx];
	        j = 0;
	        jlen = value.length;

	        while (j < jlen) {
	          result[result.length] = value[j];
	          j += 1;
	        }
	      } else {
	        result[result.length] = list[idx];
	      }

	      idx += 1;
	    }

	    return result;
	  };
	}

	function _forceReduced(x) {
	  return {
	    '@@transducer/value': x,
	    '@@transducer/reduced': true
	  };
	}

	var preservingReduced = function preservingReduced(xf) {
	  return {
	    '@@transducer/init': _xfBase.init,
	    '@@transducer/result': function transducerResult(result) {
	      return xf['@@transducer/result'](result);
	    },
	    '@@transducer/step': function transducerStep(result, input) {
	      var ret = xf['@@transducer/step'](result, input);
	      return ret['@@transducer/reduced'] ? _forceReduced(ret) : ret;
	    }
	  };
	};

	var _flatCat = function _xcat(xf) {
	  var rxf = preservingReduced(xf);
	  return {
	    '@@transducer/init': _xfBase.init,
	    '@@transducer/result': function transducerResult(result) {
	      return rxf['@@transducer/result'](result);
	    },
	    '@@transducer/step': function transducerStep(result, input) {
	      return !_isArrayLike(input) ? _reduce(rxf, result, [input]) : _reduce(rxf, result, input);
	    }
	  };
	};

	var _xchain =
	/*#__PURE__*/
	_curry2(function _xchain(f, xf) {
	  return map(f, _flatCat(xf));
	});

	/**
	 * `chain` maps a function over a list and concatenates the results. `chain`
	 * is also known as `flatMap` in some libraries
	 *
	 * Dispatches to the `chain` method of the second argument, if present,
	 * according to the [FantasyLand Chain spec](https://github.com/fantasyland/fantasy-land#chain).
	 *
	 * @func
	 * @memberOf R
	 * @since v0.3.0
	 * @category List
	 * @sig Chain m => (a -> m b) -> m a -> m b
	 * @param {Function} fn The function to map with
	 * @param {Array} list The list to map over
	 * @return {Array} The result of flat-mapping `list` with `fn`
	 * @example
	 *
	 *      var duplicate = n => [n, n];
	 *      R.chain(duplicate, [1, 2, 3]); //=> [1, 1, 2, 2, 3, 3]
	 *
	 *      R.chain(R.append, R.head)([1, 2, 3]); //=> [1, 2, 3, 1]
	 */

	var chain =
	/*#__PURE__*/
	_curry2(
	/*#__PURE__*/
	_dispatchable(['fantasy-land/chain', 'chain'], _xchain, function chain(fn, monad) {
	  if (typeof monad === 'function') {
	    return function (x) {
	      return fn(monad(x))(x);
	    };
	  }

	  return _makeFlat(false)(map(fn, monad));
	}));

	/**
	 * Gives a single-word string description of the (native) type of a value,
	 * returning such answers as 'Object', 'Number', 'Array', or 'Null'. Does not
	 * attempt to distinguish user Object types any further, reporting them all as
	 * 'Object'.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.8.0
	 * @category Type
	 * @sig (* -> {*}) -> String
	 * @param {*} val The value to test
	 * @return {String}
	 * @example
	 *
	 *      R.type({}); //=> "Object"
	 *      R.type(1); //=> "Number"
	 *      R.type(false); //=> "Boolean"
	 *      R.type('s'); //=> "String"
	 *      R.type(null); //=> "Null"
	 *      R.type([]); //=> "Array"
	 *      R.type(/[A-z]/); //=> "RegExp"
	 *      R.type(() => {}); //=> "Function"
	 *      R.type(undefined); //=> "Undefined"
	 */

	var type =
	/*#__PURE__*/
	_curry1(function type(val) {
	  return val === null ? 'Null' : val === undefined ? 'Undefined' : Object.prototype.toString.call(val).slice(8, -1);
	});

	/**
	 * A function that returns the `!` of its argument. It will return `true` when
	 * passed false-y value, and `false` when passed a truth-y one.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category Logic
	 * @sig * -> Boolean
	 * @param {*} a any value
	 * @return {Boolean} the logical inverse of passed argument.
	 * @see R.complement
	 * @example
	 *
	 *      R.not(true); //=> false
	 *      R.not(false); //=> true
	 *      R.not(0); //=> true
	 *      R.not(1); //=> false
	 */

	var not =
	/*#__PURE__*/
	_curry1(function not(a) {
	  return !a;
	});

	/**
	 * Takes a function `f` and returns a function `g` such that if called with the same arguments
	 * when `f` returns a "truthy" value, `g` returns `false` and when `f` returns a "falsy" value `g` returns `true`.
	 *
	 * `R.complement` may be applied to any functor
	 *
	 * @func
	 * @memberOf R
	 * @since v0.12.0
	 * @category Logic
	 * @sig (*... -> *) -> (*... -> Boolean)
	 * @param {Function} f
	 * @return {Function}
	 * @see R.not
	 * @example
	 *
	 *      var isNotNil = R.complement(R.isNil);
	 *      isNil(null); //=> true
	 *      isNotNil(null); //=> false
	 *      isNil(7); //=> false
	 *      isNotNil(7); //=> true
	 */

	var complement =
	/*#__PURE__*/
	lift(not);

	function _pipe(f, g) {
	  return function () {
	    return g.call(this, f.apply(this, arguments));
	  };
	}

	/**
	 * This checks whether a function has a [methodname] function. If it isn't an
	 * array it will execute that function otherwise it will default to the ramda
	 * implementation.
	 *
	 * @private
	 * @param {Function} fn ramda implemtation
	 * @param {String} methodname property to check for a custom implementation
	 * @return {Object} Whatever the return value of the method is.
	 */

	function _checkForMethod(methodname, fn) {
	  return function () {
	    var length = arguments.length;

	    if (length === 0) {
	      return fn();
	    }

	    var obj = arguments[length - 1];
	    return _isArray$1(obj) || typeof obj[methodname] !== 'function' ? fn.apply(this, arguments) : obj[methodname].apply(obj, Array.prototype.slice.call(arguments, 0, length - 1));
	  };
	}

	/**
	 * Returns the elements of the given list or string (or object with a `slice`
	 * method) from `fromIndex` (inclusive) to `toIndex` (exclusive).
	 *
	 * Dispatches to the `slice` method of the third argument, if present.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.4
	 * @category List
	 * @sig Number -> Number -> [a] -> [a]
	 * @sig Number -> Number -> String -> String
	 * @param {Number} fromIndex The start index (inclusive).
	 * @param {Number} toIndex The end index (exclusive).
	 * @param {*} list
	 * @return {*}
	 * @example
	 *
	 *      R.slice(1, 3, ['a', 'b', 'c', 'd']);        //=> ['b', 'c']
	 *      R.slice(1, Infinity, ['a', 'b', 'c', 'd']); //=> ['b', 'c', 'd']
	 *      R.slice(0, -1, ['a', 'b', 'c', 'd']);       //=> ['a', 'b', 'c']
	 *      R.slice(-3, -1, ['a', 'b', 'c', 'd']);      //=> ['b', 'c']
	 *      R.slice(0, 3, 'ramda');                     //=> 'ram'
	 */

	var slice =
	/*#__PURE__*/
	_curry3(
	/*#__PURE__*/
	_checkForMethod('slice', function slice(fromIndex, toIndex, list) {
	  return Array.prototype.slice.call(list, fromIndex, toIndex);
	}));

	/**
	 * Returns all but the first element of the given list or string (or object
	 * with a `tail` method).
	 *
	 * Dispatches to the `slice` method of the first argument, if present.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category List
	 * @sig [a] -> [a]
	 * @sig String -> String
	 * @param {*} list
	 * @return {*}
	 * @see R.head, R.init, R.last
	 * @example
	 *
	 *      R.tail([1, 2, 3]);  //=> [2, 3]
	 *      R.tail([1, 2]);     //=> [2]
	 *      R.tail([1]);        //=> []
	 *      R.tail([]);         //=> []
	 *
	 *      R.tail('abc');  //=> 'bc'
	 *      R.tail('ab');   //=> 'b'
	 *      R.tail('a');    //=> ''
	 *      R.tail('');     //=> ''
	 */

	var tail =
	/*#__PURE__*/
	_curry1(
	/*#__PURE__*/
	_checkForMethod('tail',
	/*#__PURE__*/
	slice(1, Infinity)));

	/**
	 * Performs left-to-right function composition. The leftmost function may have
	 * any arity; the remaining functions must be unary.
	 *
	 * In some libraries this function is named `sequence`.
	 *
	 * **Note:** The result of pipe is not automatically curried.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category Function
	 * @sig (((a, b, ..., n) -> o), (o -> p), ..., (x -> y), (y -> z)) -> ((a, b, ..., n) -> z)
	 * @param {...Function} functions
	 * @return {Function}
	 * @see R.compose
	 * @example
	 *
	 *      var f = R.pipe(Math.pow, R.negate, R.inc);
	 *
	 *      f(3, 4); // -(3^4) + 1
	 * @symb R.pipe(f, g, h)(a, b) = h(g(f(a, b)))
	 */

	function pipe() {
	  if (arguments.length === 0) {
	    throw new Error('pipe requires at least one argument');
	  }

	  return _arity(arguments[0].length, reduce(_pipe, arguments[0], tail(arguments)));
	}

	/**
	 * Returns a new list or string with the elements or characters in reverse
	 * order.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category List
	 * @sig [a] -> [a]
	 * @sig String -> String
	 * @param {Array|String} list
	 * @return {Array|String}
	 * @example
	 *
	 *      R.reverse([1, 2, 3]);  //=> [3, 2, 1]
	 *      R.reverse([1, 2]);     //=> [2, 1]
	 *      R.reverse([1]);        //=> [1]
	 *      R.reverse([]);         //=> []
	 *
	 *      R.reverse('abc');      //=> 'cba'
	 *      R.reverse('ab');       //=> 'ba'
	 *      R.reverse('a');        //=> 'a'
	 *      R.reverse('');         //=> ''
	 */

	var reverse =
	/*#__PURE__*/
	_curry1(function reverse(list) {
	  return _isString(list) ? list.split('').reverse().join('') : Array.prototype.slice.call(list, 0).reverse();
	});

	/**
	 * Performs right-to-left function composition. The rightmost function may have
	 * any arity; the remaining functions must be unary.
	 *
	 * **Note:** The result of compose is not automatically curried.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category Function
	 * @sig ((y -> z), (x -> y), ..., (o -> p), ((a, b, ..., n) -> o)) -> ((a, b, ..., n) -> z)
	 * @param {...Function} ...functions The functions to compose
	 * @return {Function}
	 * @see R.pipe
	 * @example
	 *
	 *      var classyGreeting = (firstName, lastName) => "The name's " + lastName + ", " + firstName + " " + lastName
	 *      var yellGreeting = R.compose(R.toUpper, classyGreeting);
	 *      yellGreeting('James', 'Bond'); //=> "THE NAME'S BOND, JAMES BOND"
	 *
	 *      R.compose(Math.abs, R.add(1), R.multiply(2))(-4) //=> 7
	 *
	 * @symb R.compose(f, g, h)(a, b) = f(g(h(a, b)))
	 */

	function compose() {
	  if (arguments.length === 0) {
	    throw new Error('compose requires at least one argument');
	  }

	  return pipe.apply(this, reverse(arguments));
	}

	function _arrayFromIterator(iter) {
	  var list = [];
	  var next;

	  while (!(next = iter.next()).done) {
	    list.push(next.value);
	  }

	  return list;
	}

	function _containsWith(pred, x, list) {
	  var idx = 0;
	  var len = list.length;

	  while (idx < len) {
	    if (pred(x, list[idx])) {
	      return true;
	    }

	    idx += 1;
	  }

	  return false;
	}

	function _functionName(f) {
	  // String(x => x) evaluates to "x => x", so the pattern may not match.
	  var match = String(f).match(/^function (\w*)/);
	  return match == null ? '' : match[1];
	}

	/**
	 * Returns true if its arguments are identical, false otherwise. Values are
	 * identical if they reference the same memory. `NaN` is identical to `NaN`;
	 * `0` and `-0` are not identical.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.15.0
	 * @category Relation
	 * @sig a -> a -> Boolean
	 * @param {*} a
	 * @param {*} b
	 * @return {Boolean}
	 * @example
	 *
	 *      var o = {};
	 *      R.identical(o, o); //=> true
	 *      R.identical(1, 1); //=> true
	 *      R.identical(1, '1'); //=> false
	 *      R.identical([], []); //=> false
	 *      R.identical(0, -0); //=> false
	 *      R.identical(NaN, NaN); //=> true
	 */

	var identical =
	/*#__PURE__*/
	_curry2(function identical(a, b) {
	  // SameValue algorithm
	  if (a === b) {
	    // Steps 1-5, 7-10
	    // Steps 6.b-6.e: +0 != -0
	    return a !== 0 || 1 / a === 1 / b;
	  } else {
	    // Step 6.a: NaN == NaN
	    return a !== a && b !== b;
	  }
	});

	/**
	 * private _uniqContentEquals function.
	 * That function is checking equality of 2 iterator contents with 2 assumptions
	 * - iterators lengths are the same
	 * - iterators values are unique
	 *
	 * false-positive result will be returned for comparision of, e.g.
	 * - [1,2,3] and [1,2,3,4]
	 * - [1,1,1] and [1,2,3]
	 * */

	function _uniqContentEquals(aIterator, bIterator, stackA, stackB) {
	  var a = _arrayFromIterator(aIterator);

	  var b = _arrayFromIterator(bIterator);

	  function eq(_a, _b) {
	    return _equals(_a, _b, stackA.slice(), stackB.slice());
	  } // if *a* array contains any element that is not included in *b*


	  return !_containsWith(function (b, aItem) {
	    return !_containsWith(eq, aItem, b);
	  }, b, a);
	}

	function _equals(a, b, stackA, stackB) {
	  if (identical(a, b)) {
	    return true;
	  }

	  var typeA = type(a);

	  if (typeA !== type(b)) {
	    return false;
	  }

	  if (a == null || b == null) {
	    return false;
	  }

	  if (typeof a['fantasy-land/equals'] === 'function' || typeof b['fantasy-land/equals'] === 'function') {
	    return typeof a['fantasy-land/equals'] === 'function' && a['fantasy-land/equals'](b) && typeof b['fantasy-land/equals'] === 'function' && b['fantasy-land/equals'](a);
	  }

	  if (typeof a.equals === 'function' || typeof b.equals === 'function') {
	    return typeof a.equals === 'function' && a.equals(b) && typeof b.equals === 'function' && b.equals(a);
	  }

	  switch (typeA) {
	    case 'Arguments':
	    case 'Array':
	    case 'Object':
	      if (typeof a.constructor === 'function' && _functionName(a.constructor) === 'Promise') {
	        return a === b;
	      }

	      break;

	    case 'Boolean':
	    case 'Number':
	    case 'String':
	      if (!(_typeof(a) === _typeof(b) && identical(a.valueOf(), b.valueOf()))) {
	        return false;
	      }

	      break;

	    case 'Date':
	      if (!identical(a.valueOf(), b.valueOf())) {
	        return false;
	      }

	      break;

	    case 'Error':
	      return a.name === b.name && a.message === b.message;

	    case 'RegExp':
	      if (!(a.source === b.source && a.global === b.global && a.ignoreCase === b.ignoreCase && a.multiline === b.multiline && a.sticky === b.sticky && a.unicode === b.unicode)) {
	        return false;
	      }

	      break;
	  }

	  var idx = stackA.length - 1;

	  while (idx >= 0) {
	    if (stackA[idx] === a) {
	      return stackB[idx] === b;
	    }

	    idx -= 1;
	  }

	  switch (typeA) {
	    case 'Map':
	      if (a.size !== b.size) {
	        return false;
	      }

	      return _uniqContentEquals(a.entries(), b.entries(), stackA.concat([a]), stackB.concat([b]));

	    case 'Set':
	      if (a.size !== b.size) {
	        return false;
	      }

	      return _uniqContentEquals(a.values(), b.values(), stackA.concat([a]), stackB.concat([b]));

	    case 'Arguments':
	    case 'Array':
	    case 'Object':
	    case 'Boolean':
	    case 'Number':
	    case 'String':
	    case 'Date':
	    case 'Error':
	    case 'RegExp':
	    case 'Int8Array':
	    case 'Uint8Array':
	    case 'Uint8ClampedArray':
	    case 'Int16Array':
	    case 'Uint16Array':
	    case 'Int32Array':
	    case 'Uint32Array':
	    case 'Float32Array':
	    case 'Float64Array':
	    case 'ArrayBuffer':
	      break;

	    default:
	      // Values of other types are only equal if identical.
	      return false;
	  }

	  var keysA = keys$3(a);

	  if (keysA.length !== keys$3(b).length) {
	    return false;
	  }

	  var extendedStackA = stackA.concat([a]);
	  var extendedStackB = stackB.concat([b]);
	  idx = keysA.length - 1;

	  while (idx >= 0) {
	    var key = keysA[idx];

	    if (!(_has$1(key, b) && _equals(b[key], a[key], extendedStackA, extendedStackB))) {
	      return false;
	    }

	    idx -= 1;
	  }

	  return true;
	}

	/**
	 * Returns `true` if its arguments are equivalent, `false` otherwise. Handles
	 * cyclical data structures.
	 *
	 * Dispatches symmetrically to the `equals` methods of both arguments, if
	 * present.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.15.0
	 * @category Relation
	 * @sig a -> b -> Boolean
	 * @param {*} a
	 * @param {*} b
	 * @return {Boolean}
	 * @example
	 *
	 *      R.equals(1, 1); //=> true
	 *      R.equals(1, '1'); //=> false
	 *      R.equals([1, 2, 3], [1, 2, 3]); //=> true
	 *
	 *      var a = {}; a.v = a;
	 *      var b = {}; b.v = b;
	 *      R.equals(a, b); //=> true
	 */

	var equals =
	/*#__PURE__*/
	_curry2(function equals(a, b) {
	  return _equals(a, b, [], []);
	});

	function _indexOf(list, a, idx) {
	  var inf, item; // Array.prototype.indexOf doesn't exist below IE9

	  if (typeof list.indexOf === 'function') {
	    switch (_typeof(a)) {
	      case 'number':
	        if (a === 0) {
	          // manually crawl the list to distinguish between +0 and -0
	          inf = 1 / a;

	          while (idx < list.length) {
	            item = list[idx];

	            if (item === 0 && 1 / item === inf) {
	              return idx;
	            }

	            idx += 1;
	          }

	          return -1;
	        } else if (a !== a) {
	          // NaN
	          while (idx < list.length) {
	            item = list[idx];

	            if (typeof item === 'number' && item !== item) {
	              return idx;
	            }

	            idx += 1;
	          }

	          return -1;
	        } // non-zero numbers can utilise Set


	        return list.indexOf(a, idx);
	      // all these types can utilise Set

	      case 'string':
	      case 'boolean':
	      case 'function':
	      case 'undefined':
	        return list.indexOf(a, idx);

	      case 'object':
	        if (a === null) {
	          // null can utilise Set
	          return list.indexOf(a, idx);
	        }

	    }
	  } // anything else not covered above, defer to R.equals


	  while (idx < list.length) {
	    if (equals(list[idx], a)) {
	      return idx;
	    }

	    idx += 1;
	  }

	  return -1;
	}

	function _contains(a, list) {
	  return _indexOf(list, a, 0) >= 0;
	}

	function _quote(s) {
	  var escaped = s.replace(/\\/g, '\\\\').replace(/[\b]/g, '\\b') // \b matches word boundary; [\b] matches backspace
	  .replace(/\f/g, '\\f').replace(/\n/g, '\\n').replace(/\r/g, '\\r').replace(/\t/g, '\\t').replace(/\v/g, '\\v').replace(/\0/g, '\\0');
	  return '"' + escaped.replace(/"/g, '\\"') + '"';
	}

	// 20.3.4.36 / 15.9.5.43 Date.prototype.toISOString()
	var $export$i = require('./_export');

	var toISOString = require('./_date-to-iso-string'); // PhantomJS / old WebKit has a broken implementations


	$export$i($export$i.P + $export$i.F * (Date.prototype.toISOString !== toISOString), 'Date', {
	  toISOString: toISOString
	});

	/**
	 * Polyfill from <https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Date/toISOString>.
	 */
	var pad = function pad(n) {
	  return (n < 10 ? '0' : '') + n;
	};

	var _toISOString = typeof Date.prototype.toISOString === 'function' ? function _toISOString(d) {
	  return d.toISOString();
	} : function _toISOString(d) {
	  return d.getUTCFullYear() + '-' + pad(d.getUTCMonth() + 1) + '-' + pad(d.getUTCDate()) + 'T' + pad(d.getUTCHours()) + ':' + pad(d.getUTCMinutes()) + ':' + pad(d.getUTCSeconds()) + '.' + (d.getUTCMilliseconds() / 1000).toFixed(3).slice(2, 5) + 'Z';
	};

	function _complement(f) {
	  return function () {
	    return !f.apply(this, arguments);
	  };
	}

	function _filter(fn, list) {
	  var idx = 0;
	  var len = list.length;
	  var result = [];

	  while (idx < len) {
	    if (fn(list[idx])) {
	      result[result.length] = list[idx];
	    }

	    idx += 1;
	  }

	  return result;
	}

	function _isObject(x) {
	  return Object.prototype.toString.call(x) === '[object Object]';
	}

	var XFilter =
	/*#__PURE__*/
	function () {
	  function XFilter(f, xf) {
	    this.xf = xf;
	    this.f = f;
	  }

	  XFilter.prototype['@@transducer/init'] = _xfBase.init;
	  XFilter.prototype['@@transducer/result'] = _xfBase.result;

	  XFilter.prototype['@@transducer/step'] = function (result, input) {
	    return this.f(input) ? this.xf['@@transducer/step'](result, input) : result;
	  };

	  return XFilter;
	}();

	var _xfilter =
	/*#__PURE__*/
	_curry2(function _xfilter(f, xf) {
	  return new XFilter(f, xf);
	});

	/**
	 * Takes a predicate and a `Filterable`, and returns a new filterable of the
	 * same type containing the members of the given filterable which satisfy the
	 * given predicate. Filterable objects include plain objects or any object
	 * that has a filter method such as `Array`.
	 *
	 * Dispatches to the `filter` method of the second argument, if present.
	 *
	 * Acts as a transducer if a transformer is given in list position.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category List
	 * @sig Filterable f => (a -> Boolean) -> f a -> f a
	 * @param {Function} pred
	 * @param {Array} filterable
	 * @return {Array} Filterable
	 * @see R.reject, R.transduce, R.addIndex
	 * @example
	 *
	 *      var isEven = n => n % 2 === 0;
	 *
	 *      R.filter(isEven, [1, 2, 3, 4]); //=> [2, 4]
	 *
	 *      R.filter(isEven, {a: 1, b: 2, c: 3, d: 4}); //=> {b: 2, d: 4}
	 */

	var filter =
	/*#__PURE__*/
	_curry2(
	/*#__PURE__*/
	_dispatchable(['filter'], _xfilter, function (pred, filterable) {
	  return _isObject(filterable) ? _reduce(function (acc, key) {
	    if (pred(filterable[key])) {
	      acc[key] = filterable[key];
	    }

	    return acc;
	  }, {}, keys$3(filterable)) : // else
	  _filter(pred, filterable);
	}));

	/**
	 * The complement of [`filter`](#filter).
	 *
	 * Acts as a transducer if a transformer is given in list position. Filterable
	 * objects include plain objects or any object that has a filter method such
	 * as `Array`.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category List
	 * @sig Filterable f => (a -> Boolean) -> f a -> f a
	 * @param {Function} pred
	 * @param {Array} filterable
	 * @return {Array}
	 * @see R.filter, R.transduce, R.addIndex
	 * @example
	 *
	 *      var isOdd = (n) => n % 2 === 1;
	 *
	 *      R.reject(isOdd, [1, 2, 3, 4]); //=> [2, 4]
	 *
	 *      R.reject(isOdd, {a: 1, b: 2, c: 3, d: 4}); //=> {b: 2, d: 4}
	 */

	var reject =
	/*#__PURE__*/
	_curry2(function reject(pred, filterable) {
	  return filter(_complement(pred), filterable);
	});

	function _toString(x, seen) {
	  var recur = function recur(y) {
	    var xs = seen.concat([x]);
	    return _contains(y, xs) ? '<Circular>' : _toString(y, xs);
	  }; //  mapPairs :: (Object, [String]) -> [String]


	  var mapPairs = function mapPairs(obj, keys) {
	    return _map(function (k) {
	      return _quote(k) + ': ' + recur(obj[k]);
	    }, keys.slice().sort());
	  };

	  switch (Object.prototype.toString.call(x)) {
	    case '[object Arguments]':
	      return '(function() { return arguments; }(' + _map(recur, x).join(', ') + '))';

	    case '[object Array]':
	      return '[' + _map(recur, x).concat(mapPairs(x, reject(function (k) {
	        return /^\d+$/.test(k);
	      }, keys$3(x)))).join(', ') + ']';

	    case '[object Boolean]':
	      return _typeof(x) === 'object' ? 'new Boolean(' + recur(x.valueOf()) + ')' : x.toString();

	    case '[object Date]':
	      return 'new Date(' + (isNaN(x.valueOf()) ? recur(NaN) : _quote(_toISOString(x))) + ')';

	    case '[object Null]':
	      return 'null';

	    case '[object Number]':
	      return _typeof(x) === 'object' ? 'new Number(' + recur(x.valueOf()) + ')' : 1 / x === -Infinity ? '-0' : x.toString(10);

	    case '[object String]':
	      return _typeof(x) === 'object' ? 'new String(' + recur(x.valueOf()) + ')' : _quote(x);

	    case '[object Undefined]':
	      return 'undefined';

	    default:
	      if (typeof x.toString === 'function') {
	        var repr = x.toString();

	        if (repr !== '[object Object]') {
	          return repr;
	        }
	      }

	      return '{' + mapPairs(x, keys$3(x)).join(', ') + '}';
	  }
	}

	/**
	 * Returns the string representation of the given value. `eval`'ing the output
	 * should result in a value equivalent to the input value. Many of the built-in
	 * `toString` methods do not satisfy this requirement.
	 *
	 * If the given value is an `[object Object]` with a `toString` method other
	 * than `Object.prototype.toString`, this method is invoked with no arguments
	 * to produce the return value. This means user-defined constructor functions
	 * can provide a suitable `toString` method. For example:
	 *
	 *     function Point(x, y) {
	 *       this.x = x;
	 *       this.y = y;
	 *     }
	 *
	 *     Point.prototype.toString = function() {
	 *       return 'new Point(' + this.x + ', ' + this.y + ')';
	 *     };
	 *
	 *     R.toString(new Point(1, 2)); //=> 'new Point(1, 2)'
	 *
	 * @func
	 * @memberOf R
	 * @since v0.14.0
	 * @category String
	 * @sig * -> String
	 * @param {*} val
	 * @return {String}
	 * @example
	 *
	 *      R.toString(42); //=> '42'
	 *      R.toString('abc'); //=> '"abc"'
	 *      R.toString([1, 2, 3]); //=> '[1, 2, 3]'
	 *      R.toString({foo: 1, bar: 2, baz: 3}); //=> '{"bar": 2, "baz": 3, "foo": 1}'
	 *      R.toString(new Date('2001-02-03T04:05:06Z')); //=> 'new Date("2001-02-03T04:05:06.000Z")'
	 */

	var toString$2 =
	/*#__PURE__*/
	_curry1(function toString(val) {
	  return _toString(val, []);
	});

	/**
	 * Accepts a converging function and a list of branching functions and returns
	 * a new function. When invoked, this new function is applied to some
	 * arguments, each branching function is applied to those same arguments. The
	 * results of each branching function are passed as arguments to the converging
	 * function to produce the return value.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.4.2
	 * @category Function
	 * @sig ((x1, x2, ...) -> z) -> [((a, b, ...) -> x1), ((a, b, ...) -> x2), ...] -> (a -> b -> ... -> z)
	 * @param {Function} after A function. `after` will be invoked with the return values of
	 *        `fn1` and `fn2` as its arguments.
	 * @param {Array} functions A list of functions.
	 * @return {Function} A new function.
	 * @see R.useWith
	 * @example
	 *
	 *      var average = R.converge(R.divide, [R.sum, R.length])
	 *      average([1, 2, 3, 4, 5, 6, 7]) //=> 4
	 *
	 *      var strangeConcat = R.converge(R.concat, [R.toUpper, R.toLower])
	 *      strangeConcat("Yodel") //=> "YODELyodel"
	 *
	 * @symb R.converge(f, [g, h])(a, b) = f(g(a, b), h(a, b))
	 */

	var converge =
	/*#__PURE__*/
	_curry2(function converge(after, fns) {
	  return curryN(reduce(max$1, 0, pluck('length', fns)), function () {
	    var args = arguments;
	    var context = this;
	    return after.apply(context, _map(function (fn) {
	      return fn.apply(context, args);
	    }, fns));
	  });
	});

	var XReduceBy =
	/*#__PURE__*/
	function () {
	  function XReduceBy(valueFn, valueAcc, keyFn, xf) {
	    this.valueFn = valueFn;
	    this.valueAcc = valueAcc;
	    this.keyFn = keyFn;
	    this.xf = xf;
	    this.inputs = {};
	  }

	  XReduceBy.prototype['@@transducer/init'] = _xfBase.init;

	  XReduceBy.prototype['@@transducer/result'] = function (result) {
	    var key;

	    for (key in this.inputs) {
	      if (_has$1(key, this.inputs)) {
	        result = this.xf['@@transducer/step'](result, this.inputs[key]);

	        if (result['@@transducer/reduced']) {
	          result = result['@@transducer/value'];
	          break;
	        }
	      }
	    }

	    this.inputs = null;
	    return this.xf['@@transducer/result'](result);
	  };

	  XReduceBy.prototype['@@transducer/step'] = function (result, input) {
	    var key = this.keyFn(input);
	    this.inputs[key] = this.inputs[key] || [key, this.valueAcc];
	    this.inputs[key][1] = this.valueFn(this.inputs[key][1], input);
	    return result;
	  };

	  return XReduceBy;
	}();

	var _xreduceBy =
	/*#__PURE__*/
	_curryN(4, [], function _xreduceBy(valueFn, valueAcc, keyFn, xf) {
	  return new XReduceBy(valueFn, valueAcc, keyFn, xf);
	});

	/**
	 * Groups the elements of the list according to the result of calling
	 * the String-returning function `keyFn` on each element and reduces the elements
	 * of each group to a single value via the reducer function `valueFn`.
	 *
	 * This function is basically a more general [`groupBy`](#groupBy) function.
	 *
	 * Acts as a transducer if a transformer is given in list position.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.20.0
	 * @category List
	 * @sig ((a, b) -> a) -> a -> (b -> String) -> [b] -> {String: a}
	 * @param {Function} valueFn The function that reduces the elements of each group to a single
	 *        value. Receives two values, accumulator for a particular group and the current element.
	 * @param {*} acc The (initial) accumulator value for each group.
	 * @param {Function} keyFn The function that maps the list's element into a key.
	 * @param {Array} list The array to group.
	 * @return {Object} An object with the output of `keyFn` for keys, mapped to the output of
	 *         `valueFn` for elements which produced that key when passed to `keyFn`.
	 * @see R.groupBy, R.reduce
	 * @example
	 *
	 *      var reduceToNamesBy = R.reduceBy((acc, student) => acc.concat(student.name), []);
	 *      var namesByGrade = reduceToNamesBy(function(student) {
	 *        var score = student.score;
	 *        return score < 65 ? 'F' :
	 *               score < 70 ? 'D' :
	 *               score < 80 ? 'C' :
	 *               score < 90 ? 'B' : 'A';
	 *      });
	 *      var students = [{name: 'Lucy', score: 92},
	 *                      {name: 'Drew', score: 85},
	 *                      // ...
	 *                      {name: 'Bart', score: 62}];
	 *      namesByGrade(students);
	 *      // {
	 *      //   'A': ['Lucy'],
	 *      //   'B': ['Drew']
	 *      //   // ...,
	 *      //   'F': ['Bart']
	 *      // }
	 */

	var reduceBy =
	/*#__PURE__*/
	_curryN(4, [],
	/*#__PURE__*/
	_dispatchable([], _xreduceBy, function reduceBy(valueFn, valueAcc, keyFn, list) {
	  return _reduce(function (acc, elt) {
	    var key = keyFn(elt);
	    acc[key] = valueFn(_has$1(key, acc) ? acc[key] : valueAcc, elt);
	    return acc;
	  }, {}, list);
	}));

	/**
	 * Counts the elements of a list according to how many match each value of a
	 * key generated by the supplied function. Returns an object mapping the keys
	 * produced by `fn` to the number of occurrences in the list. Note that all
	 * keys are coerced to strings because of how JavaScript objects work.
	 *
	 * Acts as a transducer if a transformer is given in list position.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category Relation
	 * @sig (a -> String) -> [a] -> {*}
	 * @param {Function} fn The function used to map values to keys.
	 * @param {Array} list The list to count elements from.
	 * @return {Object} An object mapping keys to number of occurrences in the list.
	 * @example
	 *
	 *      var numbers = [1.0, 1.1, 1.2, 2.0, 3.0, 2.2];
	 *      R.countBy(Math.floor)(numbers);    //=> {'1': 3, '2': 2, '3': 1}
	 *
	 *      var letters = ['a', 'b', 'A', 'a', 'B', 'c'];
	 *      R.countBy(R.toLower)(letters);   //=> {'a': 3, 'b': 2, 'c': 1}
	 */

	var countBy =
	/*#__PURE__*/
	reduceBy(function (acc, elem) {
	  return acc + 1;
	}, 0);

	/**
	 * Decrements its argument.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.9.0
	 * @category Math
	 * @sig Number -> Number
	 * @param {Number} n
	 * @return {Number} n - 1
	 * @see R.inc
	 * @example
	 *
	 *      R.dec(42); //=> 41
	 */

	var dec =
	/*#__PURE__*/
	add(-1);

	var XTake =
	/*#__PURE__*/
	function () {
	  function XTake(n, xf) {
	    this.xf = xf;
	    this.n = n;
	    this.i = 0;
	  }

	  XTake.prototype['@@transducer/init'] = _xfBase.init;
	  XTake.prototype['@@transducer/result'] = _xfBase.result;

	  XTake.prototype['@@transducer/step'] = function (result, input) {
	    this.i += 1;
	    var ret = this.n === 0 ? result : this.xf['@@transducer/step'](result, input);
	    return this.n >= 0 && this.i >= this.n ? _reduced(ret) : ret;
	  };

	  return XTake;
	}();

	var _xtake =
	/*#__PURE__*/
	_curry2(function _xtake(n, xf) {
	  return new XTake(n, xf);
	});

	/**
	 * Returns the first `n` elements of the given list, string, or
	 * transducer/transformer (or object with a `take` method).
	 *
	 * Dispatches to the `take` method of the second argument, if present.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category List
	 * @sig Number -> [a] -> [a]
	 * @sig Number -> String -> String
	 * @param {Number} n
	 * @param {*} list
	 * @return {*}
	 * @see R.drop
	 * @example
	 *
	 *      R.take(1, ['foo', 'bar', 'baz']); //=> ['foo']
	 *      R.take(2, ['foo', 'bar', 'baz']); //=> ['foo', 'bar']
	 *      R.take(3, ['foo', 'bar', 'baz']); //=> ['foo', 'bar', 'baz']
	 *      R.take(4, ['foo', 'bar', 'baz']); //=> ['foo', 'bar', 'baz']
	 *      R.take(3, 'ramda');               //=> 'ram'
	 *
	 *      var personnel = [
	 *        'Dave Brubeck',
	 *        'Paul Desmond',
	 *        'Eugene Wright',
	 *        'Joe Morello',
	 *        'Gerry Mulligan',
	 *        'Bob Bates',
	 *        'Joe Dodge',
	 *        'Ron Crotty'
	 *      ];
	 *
	 *      var takeFive = R.take(5);
	 *      takeFive(personnel);
	 *      //=> ['Dave Brubeck', 'Paul Desmond', 'Eugene Wright', 'Joe Morello', 'Gerry Mulligan']
	 * @symb R.take(-1, [a, b]) = [a, b]
	 * @symb R.take(0, [a, b]) = []
	 * @symb R.take(1, [a, b]) = [a]
	 * @symb R.take(2, [a, b]) = [a, b]
	 */

	var take =
	/*#__PURE__*/
	_curry2(
	/*#__PURE__*/
	_dispatchable(['take'], _xtake, function take(n, xs) {
	  return slice(0, n < 0 ? Infinity : n, xs);
	}));

	function dropLast(n, xs) {
	  return take(n < xs.length ? xs.length - n : 0, xs);
	}

	var XDropLast =
	/*#__PURE__*/
	function () {
	  function XDropLast(n, xf) {
	    this.xf = xf;
	    this.pos = 0;
	    this.full = false;
	    this.acc = new Array(n);
	  }

	  XDropLast.prototype['@@transducer/init'] = _xfBase.init;

	  XDropLast.prototype['@@transducer/result'] = function (result) {
	    this.acc = null;
	    return this.xf['@@transducer/result'](result);
	  };

	  XDropLast.prototype['@@transducer/step'] = function (result, input) {
	    if (this.full) {
	      result = this.xf['@@transducer/step'](result, this.acc[this.pos]);
	    }

	    this.store(input);
	    return result;
	  };

	  XDropLast.prototype.store = function (input) {
	    this.acc[this.pos] = input;
	    this.pos += 1;

	    if (this.pos === this.acc.length) {
	      this.pos = 0;
	      this.full = true;
	    }
	  };

	  return XDropLast;
	}();

	var _xdropLast =
	/*#__PURE__*/
	_curry2(function _xdropLast(n, xf) {
	  return new XDropLast(n, xf);
	});

	/**
	 * Returns a list containing all but the last `n` elements of the given `list`.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.16.0
	 * @category List
	 * @sig Number -> [a] -> [a]
	 * @sig Number -> String -> String
	 * @param {Number} n The number of elements of `list` to skip.
	 * @param {Array} list The list of elements to consider.
	 * @return {Array} A copy of the list with only the first `list.length - n` elements
	 * @see R.takeLast, R.drop, R.dropWhile, R.dropLastWhile
	 * @example
	 *
	 *      R.dropLast(1, ['foo', 'bar', 'baz']); //=> ['foo', 'bar']
	 *      R.dropLast(2, ['foo', 'bar', 'baz']); //=> ['foo']
	 *      R.dropLast(3, ['foo', 'bar', 'baz']); //=> []
	 *      R.dropLast(4, ['foo', 'bar', 'baz']); //=> []
	 *      R.dropLast(3, 'ramda');               //=> 'ra'
	 */

	var dropLast$1 =
	/*#__PURE__*/
	_curry2(
	/*#__PURE__*/
	_dispatchable([], _xdropLast, dropLast));

	var XDropRepeatsWith =
	/*#__PURE__*/
	function () {
	  function XDropRepeatsWith(pred, xf) {
	    this.xf = xf;
	    this.pred = pred;
	    this.lastValue = undefined;
	    this.seenFirstValue = false;
	  }

	  XDropRepeatsWith.prototype['@@transducer/init'] = _xfBase.init;
	  XDropRepeatsWith.prototype['@@transducer/result'] = _xfBase.result;

	  XDropRepeatsWith.prototype['@@transducer/step'] = function (result, input) {
	    var sameAsLast = false;

	    if (!this.seenFirstValue) {
	      this.seenFirstValue = true;
	    } else if (this.pred(this.lastValue, input)) {
	      sameAsLast = true;
	    }

	    this.lastValue = input;
	    return sameAsLast ? result : this.xf['@@transducer/step'](result, input);
	  };

	  return XDropRepeatsWith;
	}();

	var _xdropRepeatsWith =
	/*#__PURE__*/
	_curry2(function _xdropRepeatsWith(pred, xf) {
	  return new XDropRepeatsWith(pred, xf);
	});

	/**
	 * Returns the nth element of the given list or string. If n is negative the
	 * element at index length + n is returned.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category List
	 * @sig Number -> [a] -> a | Undefined
	 * @sig Number -> String -> String
	 * @param {Number} offset
	 * @param {*} list
	 * @return {*}
	 * @example
	 *
	 *      var list = ['foo', 'bar', 'baz', 'quux'];
	 *      R.nth(1, list); //=> 'bar'
	 *      R.nth(-1, list); //=> 'quux'
	 *      R.nth(-99, list); //=> undefined
	 *
	 *      R.nth(2, 'abc'); //=> 'c'
	 *      R.nth(3, 'abc'); //=> ''
	 * @symb R.nth(-1, [a, b, c]) = c
	 * @symb R.nth(0, [a, b, c]) = a
	 * @symb R.nth(1, [a, b, c]) = b
	 */

	var nth =
	/*#__PURE__*/
	_curry2(function nth(offset, list) {
	  var idx = offset < 0 ? list.length + offset : offset;
	  return _isString(list) ? list.charAt(idx) : list[idx];
	});

	/**
	 * Returns the last element of the given list or string.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.4
	 * @category List
	 * @sig [a] -> a | Undefined
	 * @sig String -> String
	 * @param {*} list
	 * @return {*}
	 * @see R.init, R.head, R.tail
	 * @example
	 *
	 *      R.last(['fi', 'fo', 'fum']); //=> 'fum'
	 *      R.last([]); //=> undefined
	 *
	 *      R.last('abc'); //=> 'c'
	 *      R.last(''); //=> ''
	 */

	var last =
	/*#__PURE__*/
	nth(-1);

	/**
	 * Returns a new list without any consecutively repeating elements. Equality is
	 * determined by applying the supplied predicate to each pair of consecutive elements. The
	 * first element in a series of equal elements will be preserved.
	 *
	 * Acts as a transducer if a transformer is given in list position.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.14.0
	 * @category List
	 * @sig ((a, a) -> Boolean) -> [a] -> [a]
	 * @param {Function} pred A predicate used to test whether two items are equal.
	 * @param {Array} list The array to consider.
	 * @return {Array} `list` without repeating elements.
	 * @see R.transduce
	 * @example
	 *
	 *      var l = [1, -1, 1, 3, 4, -4, -4, -5, 5, 3, 3];
	 *      R.dropRepeatsWith(R.eqBy(Math.abs), l); //=> [1, 3, 4, -5, 3]
	 */

	var dropRepeatsWith =
	/*#__PURE__*/
	_curry2(
	/*#__PURE__*/
	_dispatchable([], _xdropRepeatsWith, function dropRepeatsWith(pred, list) {
	  var result = [];
	  var idx = 1;
	  var len = list.length;

	  if (len !== 0) {
	    result[0] = list[0];

	    while (idx < len) {
	      if (!pred(last(result), list[idx])) {
	        result[result.length] = list[idx];
	      }

	      idx += 1;
	    }
	  }

	  return result;
	}));

	/**
	 * Returns a new list without any consecutively repeating elements.
	 * [`R.equals`](#equals) is used to determine equality.
	 *
	 * Acts as a transducer if a transformer is given in list position.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.14.0
	 * @category List
	 * @sig [a] -> [a]
	 * @param {Array} list The array to consider.
	 * @return {Array} `list` without repeating elements.
	 * @see R.transduce
	 * @example
	 *
	 *     R.dropRepeats([1, 1, 1, 2, 3, 4, 4, 2, 2]); //=> [1, 2, 3, 4, 2]
	 */

	var dropRepeats =
	/*#__PURE__*/
	_curry1(
	/*#__PURE__*/
	_dispatchable([],
	/*#__PURE__*/
	_xdropRepeatsWith(equals),
	/*#__PURE__*/
	dropRepeatsWith(equals)));

	/**
	 * Returns a new function much like the supplied one, except that the first two
	 * arguments' order is reversed.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category Function
	 * @sig ((a, b, c, ...) -> z) -> (b -> a -> c -> ... -> z)
	 * @param {Function} fn The function to invoke with its first two parameters reversed.
	 * @return {*} The result of invoking `fn` with its first two parameters' order reversed.
	 * @example
	 *
	 *      var mergeThree = (a, b, c) => [].concat(a, b, c);
	 *
	 *      mergeThree(1, 2, 3); //=> [1, 2, 3]
	 *
	 *      R.flip(mergeThree)(1, 2, 3); //=> [2, 1, 3]
	 * @symb R.flip(f)(a, b, c) = f(b, a, c)
	 */

	var flip =
	/*#__PURE__*/
	_curry1(function flip(fn) {
	  return curryN(fn.length, function (a, b) {
	    var args = Array.prototype.slice.call(arguments, 0);
	    args[0] = b;
	    args[1] = a;
	    return fn.apply(this, args);
	  });
	});

	/**
	 * Splits a list into sub-lists stored in an object, based on the result of
	 * calling a String-returning function on each element, and grouping the
	 * results according to values returned.
	 *
	 * Dispatches to the `groupBy` method of the second argument, if present.
	 *
	 * Acts as a transducer if a transformer is given in list position.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category List
	 * @sig (a -> String) -> [a] -> {String: [a]}
	 * @param {Function} fn Function :: a -> String
	 * @param {Array} list The array to group
	 * @return {Object} An object with the output of `fn` for keys, mapped to arrays of elements
	 *         that produced that key when passed to `fn`.
	 * @see R.transduce
	 * @example
	 *
	 *      var byGrade = R.groupBy(function(student) {
	 *        var score = student.score;
	 *        return score < 65 ? 'F' :
	 *               score < 70 ? 'D' :
	 *               score < 80 ? 'C' :
	 *               score < 90 ? 'B' : 'A';
	 *      });
	 *      var students = [{name: 'Abby', score: 84},
	 *                      {name: 'Eddy', score: 58},
	 *                      // ...
	 *                      {name: 'Jack', score: 69}];
	 *      byGrade(students);
	 *      // {
	 *      //   'A': [{name: 'Dianne', score: 99}],
	 *      //   'B': [{name: 'Abby', score: 84}]
	 *      //   // ...,
	 *      //   'F': [{name: 'Eddy', score: 58}]
	 *      // }
	 */

	var groupBy =
	/*#__PURE__*/
	_curry2(
	/*#__PURE__*/
	_checkForMethod('groupBy',
	/*#__PURE__*/
	reduceBy(function (acc, item) {
	  if (acc == null) {
	    acc = [];
	  }

	  acc.push(item);
	  return acc;
	}, null)));

	/**
	 * Returns the first element of the given list or string. In some libraries
	 * this function is named `first`.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category List
	 * @sig [a] -> a | Undefined
	 * @sig String -> String
	 * @param {Array|String} list
	 * @return {*}
	 * @see R.tail, R.init, R.last
	 * @example
	 *
	 *      R.head(['fi', 'fo', 'fum']); //=> 'fi'
	 *      R.head([]); //=> undefined
	 *
	 *      R.head('abc'); //=> 'a'
	 *      R.head(''); //=> ''
	 */

	var head =
	/*#__PURE__*/
	nth(0);

	function _identity(x) {
	  return x;
	}

	/**
	 * A function that does nothing but return the parameter supplied to it. Good
	 * as a default or placeholder function.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category Function
	 * @sig a -> a
	 * @param {*} x The value to return.
	 * @return {*} The input value, `x`.
	 * @example
	 *
	 *      R.identity(1); //=> 1
	 *
	 *      var obj = {};
	 *      R.identity(obj) === obj; //=> true
	 * @symb R.identity(a) = a
	 */

	var identity =
	/*#__PURE__*/
	_curry1(_identity);

	/**
	 * Increments its argument.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.9.0
	 * @category Math
	 * @sig Number -> Number
	 * @param {Number} n
	 * @return {Number} n + 1
	 * @see R.dec
	 * @example
	 *
	 *      R.inc(42); //=> 43
	 */

	var inc =
	/*#__PURE__*/
	add(1);

	/**
	 * Given a function that generates a key, turns a list of objects into an
	 * object indexing the objects by the given key. Note that if multiple
	 * objects generate the same value for the indexing key only the last value
	 * will be included in the generated object.
	 *
	 * Acts as a transducer if a transformer is given in list position.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.19.0
	 * @category List
	 * @sig (a -> String) -> [{k: v}] -> {k: {k: v}}
	 * @param {Function} fn Function :: a -> String
	 * @param {Array} array The array of objects to index
	 * @return {Object} An object indexing each array element by the given property.
	 * @example
	 *
	 *      var list = [{id: 'xyz', title: 'A'}, {id: 'abc', title: 'B'}];
	 *      R.indexBy(R.prop('id'), list);
	 *      //=> {abc: {id: 'abc', title: 'B'}, xyz: {id: 'xyz', title: 'A'}}
	 */

	var indexBy =
	/*#__PURE__*/
	reduceBy(function (acc, elem) {
	  return elem;
	}, null);

	/**
	 * Returns all but the last element of the given list or string.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.9.0
	 * @category List
	 * @sig [a] -> [a]
	 * @sig String -> String
	 * @param {*} list
	 * @return {*}
	 * @see R.last, R.head, R.tail
	 * @example
	 *
	 *      R.init([1, 2, 3]);  //=> [1, 2]
	 *      R.init([1, 2]);     //=> [1]
	 *      R.init([1]);        //=> []
	 *      R.init([]);         //=> []
	 *
	 *      R.init('abc');  //=> 'ab'
	 *      R.init('ab');   //=> 'a'
	 *      R.init('a');    //=> ''
	 *      R.init('');     //=> ''
	 */

	var init =
	/*#__PURE__*/
	slice(0, -1);

	var _redefineAll = function (target, src, safe) {
	  for (var key in src) {
	    redefine$2(target, key, src[key], safe);
	  }

	  return target;
	};

	var _aFunction = function (it) {
	  if (typeof it != 'function') throw TypeError(it + ' is not a function!');
	  return it;
	};

	// optional / simple context binding


	var _ctx = function (fn, that, length) {
	  _aFunction(fn);
	  if (that === undefined) return fn;

	  switch (length) {
	    case 1:
	      return function (a) {
	        return fn.call(that, a);
	      };

	    case 2:
	      return function (a, b) {
	        return fn.call(that, a, b);
	      };

	    case 3:
	      return function (a, b, c) {
	        return fn.call(that, a, b, c);
	      };
	  }

	  return function ()
	  /* ...args */
	  {
	    return fn.apply(that, arguments);
	  };
	};

	var _anInstance = function (it, Constructor, name, forbiddenField) {
	  if (!(it instanceof Constructor) || forbiddenField !== undefined && forbiddenField in it) {
	    throw TypeError(name + ': incorrect invocation!');
	  }

	  return it;
	};

	// call something on iterator step with safe closing on error


	var _iterCall = function (iterator, fn, value, entries) {
	  try {
	    return entries ? fn(_anObject(value)[0], value[1]) : fn(value); // 7.4.6 IteratorClose(iterator, completion)
	  } catch (e) {
	    var ret = iterator['return'];
	    if (ret !== undefined) _anObject(ret.call(iterator));
	    throw e;
	  }
	};

	// check on default Array iterator


	var ITERATOR$3 = require$$0$2('iterator');

	var ArrayProto$2 = Array.prototype;

	var _isArrayIter = function (it) {
	  return it !== undefined && (_iterators.Array === it || ArrayProto$2[ITERATOR$3] === it);
	};

	// getting tag from 19.1.3.6 Object.prototype.toString()


	var TAG$1 = require$$0$2('toStringTag'); // ES3 wrong here


	var ARG = cof$1(function () {
	  return arguments;
	}()) == 'Arguments'; // fallback for IE11 Script Access Denied error

	var tryGet = function tryGet(it, key) {
	  try {
	    return it[key];
	  } catch (e) {
	    /* empty */
	  }
	};

	var _classof = function (it) {
	  var O, T, B;
	  return it === undefined ? 'Undefined' : it === null ? 'Null' // @@toStringTag case
	  : typeof (T = tryGet(O = Object(it), TAG$1)) == 'string' ? T // builtinTag case
	  : ARG ? cof$1(O) // ES3 arguments fallback
	  : (B = cof$1(O)) == 'Object' && typeof O.callee == 'function' ? 'Arguments' : B;
	};

	var ITERATOR$4 = require$$0$2('iterator');



	var core_getIteratorMethod = _core.getIteratorMethod = function (it) {
	  if (it != undefined) return it[ITERATOR$4] || it['@@iterator'] || _iterators[_classof(it)];
	};

	var _forOf = createCommonjsModule(function (module) {
	var BREAK = {};
	var RETURN = {};

	var exports = module.exports = function (iterable, entries, fn, that, ITERATOR) {
	  var iterFn = ITERATOR ? function () {
	    return iterable;
	  } : core_getIteratorMethod(iterable);
	  var f = _ctx(fn, that, entries ? 2 : 1);
	  var index = 0;
	  var length, step, iterator, result;
	  if (typeof iterFn != 'function') throw TypeError(iterable + ' is not iterable!'); // fast case for arrays with default iterator

	  if (_isArrayIter(iterFn)) for (length = _toLength(iterable.length); length > index; index++) {
	    result = entries ? f(_anObject(step = iterable[index])[0], step[1]) : f(iterable[index]);
	    if (result === BREAK || result === RETURN) return result;
	  } else for (iterator = iterFn.call(iterable); !(step = iterator.next()).done;) {
	    result = _iterCall(iterator, f, step.value, entries);
	    if (result === BREAK || result === RETURN) return result;
	  }
	};

	exports.BREAK = BREAK;
	exports.RETURN = RETURN;
	});

	// 19.1.2.15 Object.preventExtensions(O)


	var meta = require$$0$4.onFreeze;

	_objectSap('preventExtensions', function ($preventExtensions) {
	  return function preventExtensions(it) {
	    return $preventExtensions && isObject(it) ? $preventExtensions(meta(it)) : it;
	  };
	});

	// 19.1.2.11 Object.isExtensible(O)


	_objectSap('isExtensible', function ($isExtensible) {
	  return function isExtensible(it) {
	    return isObject(it) ? $isExtensible ? $isExtensible(it) : true : false;
	  };
	});

	var META$1 = require('./_uid')('meta');

	var isObject$5 = require('./_is-object');

	var has$5 = require('./_has');

	var setDesc = require('./_object-dp').f;

	var id$2 = 0;

	var isExtensible = Object.isExtensible || function () {
	  return true;
	};

	var FREEZE = !require('./_fails')(function () {
	  return isExtensible(Object.preventExtensions({}));
	});

	var setMeta = function setMeta(it) {
	  setDesc(it, META$1, {
	    value: {
	      i: 'O' + ++id$2,
	      // object ID
	      w: {} // weak collections IDs

	    }
	  });
	};

	var fastKey = function fastKey(it, create) {
	  // return primitive with prefix
	  if (!isObject$5(it)) return _typeof(it) == 'symbol' ? it : (typeof it == 'string' ? 'S' : 'P') + it;

	  if (!has$5(it, META$1)) {
	    // can't set metadata to uncaught frozen object
	    if (!isExtensible(it)) return 'F'; // not necessary to add metadata

	    if (!create) return 'E'; // add missing metadata

	    setMeta(it); // return object ID
	  }

	  return it[META$1].i;
	};

	var getWeak = function getWeak(it, create) {
	  if (!has$5(it, META$1)) {
	    // can't set metadata to uncaught frozen object
	    if (!isExtensible(it)) return true; // not necessary to add metadata

	    if (!create) return false; // add missing metadata

	    setMeta(it); // return hash weak collections IDs
	  }

	  return it[META$1].w;
	}; // add metadata on freeze-family methods calling


	var onFreeze = function onFreeze(it) {
	  if (FREEZE && meta$1.NEED && isExtensible(it) && !has$5(it, META$1)) setMeta(it);
	  return it;
	};

	var meta$1 = module.exports = {
	  KEY: META$1,
	  NEED: false,
	  fastKey: fastKey,
	  getWeak: getWeak,
	  onFreeze: onFreeze
	};

	var _validateCollection = function (it, TYPE) {
	  if (!isObject(it) || it._t !== TYPE) throw TypeError('Incompatible receiver, ' + TYPE + ' required!');
	  return it;
	};

	var dP$7 = require$$0$1.f;



















	var fastKey$1 = require$$0$4.fastKey;



	var SIZE = DESCRIPTORS ? '_s' : 'size';

	var getEntry = function getEntry(that, key) {
	  // fast case
	  var index = fastKey$1(key);
	  var entry;
	  if (index !== 'F') return that._i[index]; // frozen object case

	  for (entry = that._f; entry; entry = entry.n) {
	    if (entry.k == key) return entry;
	  }
	};

	var _collectionStrong = {
	  getConstructor: function getConstructor(wrapper, NAME, IS_MAP, ADDER) {
	    var C = wrapper(function (that, iterable) {
	      _anInstance(that, C, NAME, '_i');
	      that._t = NAME; // collection type

	      that._i = create(null); // index

	      that._f = undefined; // first entry

	      that._l = undefined; // last entry

	      that[SIZE] = 0; // size

	      if (iterable != undefined) _forOf(iterable, IS_MAP, that[ADDER], that);
	    });
	    _redefineAll(C.prototype, {
	      // 23.1.3.1 Map.prototype.clear()
	      // 23.2.3.2 Set.prototype.clear()
	      clear: function clear() {
	        for (var that = _validateCollection(this, NAME), data = that._i, entry = that._f; entry; entry = entry.n) {
	          entry.r = true;
	          if (entry.p) entry.p = entry.p.n = undefined;
	          delete data[entry.i];
	        }

	        that._f = that._l = undefined;
	        that[SIZE] = 0;
	      },
	      // 23.1.3.3 Map.prototype.delete(key)
	      // 23.2.3.4 Set.prototype.delete(value)
	      'delete': function _delete(key) {
	        var that = _validateCollection(this, NAME);
	        var entry = getEntry(that, key);

	        if (entry) {
	          var next = entry.n;
	          var prev = entry.p;
	          delete that._i[entry.i];
	          entry.r = true;
	          if (prev) prev.n = next;
	          if (next) next.p = prev;
	          if (that._f == entry) that._f = next;
	          if (that._l == entry) that._l = prev;
	          that[SIZE]--;
	        }

	        return !!entry;
	      },
	      // 23.2.3.6 Set.prototype.forEach(callbackfn, thisArg = undefined)
	      // 23.1.3.5 Map.prototype.forEach(callbackfn, thisArg = undefined)
	      forEach: function forEach(callbackfn
	      /* , that = undefined */
	      ) {
	        _validateCollection(this, NAME);
	        var f = _ctx(callbackfn, arguments.length > 1 ? arguments[1] : undefined, 3);
	        var entry;

	        while (entry = entry ? entry.n : this._f) {
	          f(entry.v, entry.k, this); // revert to the last existing entry

	          while (entry && entry.r) {
	            entry = entry.p;
	          }
	        }
	      },
	      // 23.1.3.7 Map.prototype.has(key)
	      // 23.2.3.7 Set.prototype.has(value)
	      has: function has(key) {
	        return !!getEntry(_validateCollection(this, NAME), key);
	      }
	    });
	    if (DESCRIPTORS) dP$7(C.prototype, 'size', {
	      get: function get() {
	        return _validateCollection(this, NAME)[SIZE];
	      }
	    });
	    return C;
	  },
	  def: function def(that, key, value) {
	    var entry = getEntry(that, key);
	    var prev, index; // change existing entry

	    if (entry) {
	      entry.v = value; // create new entry
	    } else {
	      that._l = entry = {
	        i: index = fastKey$1(key, true),
	        // <- index
	        k: key,
	        // <- key
	        v: value,
	        // <- value
	        p: prev = that._l,
	        // <- previous entry
	        n: undefined,
	        // <- next entry
	        r: false // <- removed

	      };
	      if (!that._f) that._f = entry;
	      if (prev) prev.n = entry;
	      that[SIZE]++; // add to index

	      if (index !== 'F') that._i[index] = entry;
	    }

	    return that;
	  },
	  getEntry: getEntry,
	  setStrong: function setStrong(C, NAME, IS_MAP) {
	    // add .keys, .values, .entries, [@@iterator]
	    // 23.1.3.4, 23.1.3.8, 23.1.3.11, 23.1.3.12, 23.2.3.5, 23.2.3.8, 23.2.3.10, 23.2.3.11
	    $iterDefine(C, NAME, function (iterated, kind) {
	      this._t = _validateCollection(iterated, NAME); // target

	      this._k = kind; // kind

	      this._l = undefined; // previous
	    }, function () {
	      var that = this;
	      var kind = that._k;
	      var entry = that._l; // revert to the last existing entry

	      while (entry && entry.r) {
	        entry = entry.p;
	      } // get next entry


	      if (!that._t || !(that._l = entry = entry ? entry.n : that._t._f)) {
	        // or finish the iteration
	        that._t = undefined;
	        return _iterStep(1);
	      } // return step by kind


	      if (kind == 'keys') return _iterStep(0, entry.k);
	      if (kind == 'values') return _iterStep(0, entry.v);
	      return _iterStep(0, [entry.k, entry.v]);
	    }, IS_MAP ? 'entries' : 'values', !IS_MAP, true); // add [@@species], 23.1.2.2, 23.2.2.2

	    _setSpecies(NAME);
	  }
	};

	var global$9 = require('./_global');

	var $export$j = require('./_export');

	var redefine$5 = require('./_redefine');

	var redefineAll$2 = require('./_redefine-all');

	var meta$2 = require('./_meta');

	var forOf$1 = require('./_for-of');

	var anInstance$3 = require('./_an-instance');

	var isObject$6 = require('./_is-object');

	var fails$6 = require('./_fails');

	var $iterDetect$1 = require('./_iter-detect');

	var setToStringTag$3 = require('./_set-to-string-tag');

	var inheritIfRequired$1 = require('./_inherit-if-required');

	module.exports = function (NAME, wrapper, methods, common, IS_MAP, IS_WEAK) {
	  var Base = global$9[NAME];
	  var C = Base;
	  var ADDER = IS_MAP ? 'set' : 'add';
	  var proto = C && C.prototype;
	  var O = {};

	  var fixMethod = function fixMethod(KEY) {
	    var fn = proto[KEY];
	    redefine$5(proto, KEY, KEY == 'delete' ? function (a) {
	      return IS_WEAK && !isObject$6(a) ? false : fn.call(this, a === 0 ? 0 : a);
	    } : KEY == 'has' ? function has(a) {
	      return IS_WEAK && !isObject$6(a) ? false : fn.call(this, a === 0 ? 0 : a);
	    } : KEY == 'get' ? function get(a) {
	      return IS_WEAK && !isObject$6(a) ? undefined : fn.call(this, a === 0 ? 0 : a);
	    } : KEY == 'add' ? function add(a) {
	      fn.call(this, a === 0 ? 0 : a);
	      return this;
	    } : function set(a, b) {
	      fn.call(this, a === 0 ? 0 : a, b);
	      return this;
	    });
	  };

	  if (typeof C != 'function' || !(IS_WEAK || proto.forEach && !fails$6(function () {
	    new C().entries().next();
	  }))) {
	    // create collection constructor
	    C = common.getConstructor(wrapper, NAME, IS_MAP, ADDER);
	    redefineAll$2(C.prototype, methods);
	    meta$2.NEED = true;
	  } else {
	    var instance = new C(); // early implementations not supports chaining

	    var HASNT_CHAINING = instance[ADDER](IS_WEAK ? {} : -0, 1) != instance; // V8 ~  Chromium 40- weak-collections throws on primitives, but should return false

	    var THROWS_ON_PRIMITIVES = fails$6(function () {
	      instance.has(1);
	    }); // most early implementations doesn't supports iterables, most modern - not close it correctly

	    var ACCEPT_ITERABLES = $iterDetect$1(function (iter) {
	      new C(iter);
	    }); // eslint-disable-line no-new
	    // for early implementations -0 and +0 not the same

	    var BUGGY_ZERO = !IS_WEAK && fails$6(function () {
	      // V8 ~ Chromium 42- fails only with 5+ elements
	      var $instance = new C();
	      var index = 5;

	      while (index--) {
	        $instance[ADDER](index, index);
	      }

	      return !$instance.has(-0);
	    });

	    if (!ACCEPT_ITERABLES) {
	      C = wrapper(function (target, iterable) {
	        anInstance$3(target, C, NAME);
	        var that = inheritIfRequired$1(new Base(), target, C);
	        if (iterable != undefined) forOf$1(iterable, IS_MAP, that[ADDER], that);
	        return that;
	      });
	      C.prototype = proto;
	      proto.constructor = C;
	    }

	    if (THROWS_ON_PRIMITIVES || BUGGY_ZERO) {
	      fixMethod('delete');
	      fixMethod('has');
	      IS_MAP && fixMethod('get');
	    }

	    if (BUGGY_ZERO || HASNT_CHAINING) fixMethod(ADDER); // weak collections should not contains .clear method

	    if (IS_WEAK && proto.clear) delete proto.clear;
	  }

	  setToStringTag$3(C, NAME);
	  O[NAME] = C;
	  $export$j($export$j.G + $export$j.W + $export$j.F * (C != Base), O);
	  if (!IS_WEAK) common.setStrong(C, NAME, IS_MAP);
	  return C;
	};

	var _collection = /*#__PURE__*/Object.freeze({

	});

	var SET = 'Set'; // 23.2 Set Objects

	var es6_set = _collection(SET, function (get) {
	  return function Set() {
	    return get(this, arguments.length > 0 ? arguments[0] : undefined);
	  };
	}, {
	  // 23.2.3.1 Set.prototype.add(value)
	  add: function add(value) {
	    return _collectionStrong.def(_validateCollection(this, SET), value = value === 0 ? 0 : value, value);
	  }
	}, _collectionStrong);

	var _Set =
	/*#__PURE__*/
	function () {
	  function _Set() {
	    /* globals Set */
	    this._nativeSet = typeof Set === 'function' ? new Set() : null;
	    this._items = {};
	  } // until we figure out why jsdoc chokes on this
	  // @param item The item to add to the Set
	  // @returns {boolean} true if the item did not exist prior, otherwise false
	  //


	  _Set.prototype.add = function (item) {
	    return !hasOrAdd(item, true, this);
	  }; //
	  // @param item The item to check for existence in the Set
	  // @returns {boolean} true if the item exists in the Set, otherwise false
	  //


	  _Set.prototype.has = function (item) {
	    return hasOrAdd(item, false, this);
	  }; //
	  // Combines the logic for checking whether an item is a member of the set and
	  // for adding a new item to the set.
	  //
	  // @param item       The item to check or add to the Set instance.
	  // @param shouldAdd  If true, the item will be added to the set if it doesn't
	  //                   already exist.
	  // @param set        The set instance to check or add to.
	  // @return {boolean} true if the item already existed, otherwise false.
	  //


	  return _Set;
	}();

	function hasOrAdd(item, shouldAdd, set) {
	  var type = _typeof(item);

	  var prevSize, newSize;

	  switch (type) {
	    case 'string':
	    case 'number':
	      // distinguish between +0 and -0
	      if (item === 0 && 1 / item === -Infinity) {
	        if (set._items['-0']) {
	          return true;
	        } else {
	          if (shouldAdd) {
	            set._items['-0'] = true;
	          }

	          return false;
	        }
	      } // these types can all utilise the native Set


	      if (set._nativeSet !== null) {
	        if (shouldAdd) {
	          prevSize = set._nativeSet.size;

	          set._nativeSet.add(item);

	          newSize = set._nativeSet.size;
	          return newSize === prevSize;
	        } else {
	          return set._nativeSet.has(item);
	        }
	      } else {
	        if (!(type in set._items)) {
	          if (shouldAdd) {
	            set._items[type] = {};
	            set._items[type][item] = true;
	          }

	          return false;
	        } else if (item in set._items[type]) {
	          return true;
	        } else {
	          if (shouldAdd) {
	            set._items[type][item] = true;
	          }

	          return false;
	        }
	      }

	    case 'boolean':
	      // set._items['boolean'] holds a two element array
	      // representing [ falseExists, trueExists ]
	      if (type in set._items) {
	        var bIdx = item ? 1 : 0;

	        if (set._items[type][bIdx]) {
	          return true;
	        } else {
	          if (shouldAdd) {
	            set._items[type][bIdx] = true;
	          }

	          return false;
	        }
	      } else {
	        if (shouldAdd) {
	          set._items[type] = item ? [false, true] : [true, false];
	        }

	        return false;
	      }

	    case 'function':
	      // compare functions for reference equality
	      if (set._nativeSet !== null) {
	        if (shouldAdd) {
	          prevSize = set._nativeSet.size;

	          set._nativeSet.add(item);

	          newSize = set._nativeSet.size;
	          return newSize === prevSize;
	        } else {
	          return set._nativeSet.has(item);
	        }
	      } else {
	        if (!(type in set._items)) {
	          if (shouldAdd) {
	            set._items[type] = [item];
	          }

	          return false;
	        }

	        if (!_contains(item, set._items[type])) {
	          if (shouldAdd) {
	            set._items[type].push(item);
	          }

	          return false;
	        }

	        return true;
	      }

	    case 'undefined':
	      if (set._items[type]) {
	        return true;
	      } else {
	        if (shouldAdd) {
	          set._items[type] = true;
	        }

	        return false;
	      }

	    case 'object':
	      if (item === null) {
	        if (!set._items['null']) {
	          if (shouldAdd) {
	            set._items['null'] = true;
	          }

	          return false;
	        }

	        return true;
	      }

	    /* falls through */

	    default:
	      // reduce the search size of heterogeneous sets by creating buckets
	      // for each type.
	      type = Object.prototype.toString.call(item);

	      if (!(type in set._items)) {
	        if (shouldAdd) {
	          set._items[type] = [item];
	        }

	        return false;
	      } // scan through all previously applied items


	      if (!_contains(item, set._items[type])) {
	        if (shouldAdd) {
	          set._items[type].push(item);
	        }

	        return false;
	      }

	      return true;
	  }
	} // A simple Set type that honours R.equals semantics

	/**
	 * Returns a new list containing only one copy of each element in the original
	 * list, based upon the value returned by applying the supplied function to
	 * each list element. Prefers the first item if the supplied function produces
	 * the same value on two items. [`R.equals`](#equals) is used for comparison.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.16.0
	 * @category List
	 * @sig (a -> b) -> [a] -> [a]
	 * @param {Function} fn A function used to produce a value to use during comparisons.
	 * @param {Array} list The array to consider.
	 * @return {Array} The list of unique items.
	 * @example
	 *
	 *      R.uniqBy(Math.abs, [-1, -5, 2, 10, 1, 2]); //=> [-1, -5, 2, 10]
	 */

	var uniqBy =
	/*#__PURE__*/
	_curry2(function uniqBy(fn, list) {
	  var set = new _Set();
	  var result = [];
	  var idx = 0;
	  var appliedItem, item;

	  while (idx < list.length) {
	    item = list[idx];
	    appliedItem = fn(item);

	    if (set.add(appliedItem)) {
	      result.push(item);
	    }

	    idx += 1;
	  }

	  return result;
	});

	/**
	 * Returns a new list containing only one copy of each element in the original
	 * list. [`R.equals`](#equals) is used to determine equality.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category List
	 * @sig [a] -> [a]
	 * @param {Array} list The array to consider.
	 * @return {Array} The list of unique items.
	 * @example
	 *
	 *      R.uniq([1, 1, 2, 1]); //=> [1, 2]
	 *      R.uniq([1, '1']);     //=> [1, '1']
	 *      R.uniq([[42], [42]]); //=> [[42]]
	 */

	var uniq =
	/*#__PURE__*/
	uniqBy(identity);

	/**
	 * Turns a named method with a specified arity into a function that can be
	 * called directly supplied with arguments and a target object.
	 *
	 * The returned function is curried and accepts `arity + 1` parameters where
	 * the final parameter is the target object.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category Function
	 * @sig Number -> String -> (a -> b -> ... -> n -> Object -> *)
	 * @param {Number} arity Number of arguments the returned function should take
	 *        before the target object.
	 * @param {String} method Name of the method to call.
	 * @return {Function} A new curried function.
	 * @see R.construct
	 * @example
	 *
	 *      var sliceFrom = R.invoker(1, 'slice');
	 *      sliceFrom(6, 'abcdefghijklm'); //=> 'ghijklm'
	 *      var sliceFrom6 = R.invoker(2, 'slice')(6);
	 *      sliceFrom6(8, 'abcdefghijklm'); //=> 'gh'
	 * @symb R.invoker(0, 'method')(o) = o['method']()
	 * @symb R.invoker(1, 'method')(a, o) = o['method'](a)
	 * @symb R.invoker(2, 'method')(a, b, o) = o['method'](a, b)
	 */

	var invoker =
	/*#__PURE__*/
	_curry2(function invoker(arity, method) {
	  return curryN(arity + 1, function () {
	    var target = arguments[arity];

	    if (target != null && _isFunction(target[method])) {
	      return target[method].apply(target, Array.prototype.slice.call(arguments, 0, arity));
	    }

	    throw new TypeError(toString$2(target) + ' does not have a method named "' + method + '"');
	  });
	});

	/**
	 * Returns a string made by inserting the `separator` between each element and
	 * concatenating all the elements into a single string.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category List
	 * @sig String -> [a] -> String
	 * @param {Number|String} separator The string used to separate the elements.
	 * @param {Array} xs The elements to join into a string.
	 * @return {String} str The string made by concatenating `xs` with `separator`.
	 * @see R.split
	 * @example
	 *
	 *      var spacer = R.join(' ');
	 *      spacer(['a', 2, 3.4]);   //=> 'a 2 3.4'
	 *      R.join('|', [1, 2, 3]);    //=> '1|2|3'
	 */

	var join =
	/*#__PURE__*/
	invoker(1, 'join');

	/**
	 * juxt applies a list of functions to a list of values.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.19.0
	 * @category Function
	 * @sig [(a, b, ..., m) -> n] -> ((a, b, ..., m) -> [n])
	 * @param {Array} fns An array of functions
	 * @return {Function} A function that returns a list of values after applying each of the original `fns` to its parameters.
	 * @see R.applySpec
	 * @example
	 *
	 *      var getRange = R.juxt([Math.min, Math.max]);
	 *      getRange(3, 4, 9, -3); //=> [-3, 9]
	 * @symb R.juxt([f, g, h])(a, b) = [f(a, b), g(a, b), h(a, b)]
	 */

	var juxt =
	/*#__PURE__*/
	_curry1(function juxt(fns) {
	  return converge(function () {
	    return Array.prototype.slice.call(arguments, 0);
	  }, fns);
	});

	/**
	 * Takes a function and two values, and returns whichever value produces the
	 * larger result when passed to the provided function.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.8.0
	 * @category Relation
	 * @sig Ord b => (a -> b) -> a -> a -> a
	 * @param {Function} f
	 * @param {*} a
	 * @param {*} b
	 * @return {*}
	 * @see R.max, R.minBy
	 * @example
	 *
	 *      //  square :: Number -> Number
	 *      var square = n => n * n;
	 *
	 *      R.maxBy(square, -3, 2); //=> -3
	 *
	 *      R.reduce(R.maxBy(square), 0, [3, -5, 4, 1, -2]); //=> -5
	 *      R.reduce(R.maxBy(square), 0, []); //=> 0
	 */

	var maxBy =
	/*#__PURE__*/
	_curry3(function maxBy(f, a, b) {
	  return f(b) > f(a) ? b : a;
	});

	/**
	 * Adds together all the elements of a list.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category Math
	 * @sig [Number] -> Number
	 * @param {Array} list An array of numbers
	 * @return {Number} The sum of all the numbers in the list.
	 * @see R.reduce
	 * @example
	 *
	 *      R.sum([2,4,6,8,100,1]); //=> 121
	 */

	var sum =
	/*#__PURE__*/
	reduce(add, 0);

	/**
	 * A customisable version of [`R.memoize`](#memoize). `memoizeWith` takes an
	 * additional function that will be applied to a given argument set and used to
	 * create the cache key under which the results of the function to be memoized
	 * will be stored. Care must be taken when implementing key generation to avoid
	 * clashes that may overwrite previous entries erroneously.
	 *
	 *
	 * @func
	 * @memberOf R
	 * @since v0.24.0
	 * @category Function
	 * @sig (*... -> String) -> (*... -> a) -> (*... -> a)
	 * @param {Function} fn The function to generate the cache key.
	 * @param {Function} fn The function to memoize.
	 * @return {Function} Memoized version of `fn`.
	 * @see R.memoize
	 * @example
	 *
	 *      let count = 0;
	 *      const factorial = R.memoizeWith(R.identity, n => {
	 *        count += 1;
	 *        return R.product(R.range(1, n + 1));
	 *      });
	 *      factorial(5); //=> 120
	 *      factorial(5); //=> 120
	 *      factorial(5); //=> 120
	 *      count; //=> 1
	 */

	var memoizeWith =
	/*#__PURE__*/
	_curry2(function memoizeWith(mFn, fn) {
	  var cache = {};
	  return _arity(fn.length, function () {
	    var key = mFn.apply(this, arguments);

	    if (!_has$1(key, cache)) {
	      cache[key] = fn.apply(this, arguments);
	    }

	    return cache[key];
	  });
	});

	/**
	 * Creates a new function that, when invoked, caches the result of calling `fn`
	 * for a given argument set and returns the result. Subsequent calls to the
	 * memoized `fn` with the same argument set will not result in an additional
	 * call to `fn`; instead, the cached result for that set of arguments will be
	 * returned.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category Function
	 * @sig (*... -> a) -> (*... -> a)
	 * @param {Function} fn The function to memoize.
	 * @return {Function} Memoized version of `fn`.
	 * @see R.memoizeWith
	 * @deprecated since v0.25.0
	 * @example
	 *
	 *      let count = 0;
	 *      const factorial = R.memoize(n => {
	 *        count += 1;
	 *        return R.product(R.range(1, n + 1));
	 *      });
	 *      factorial(5); //=> 120
	 *      factorial(5); //=> 120
	 *      factorial(5); //=> 120
	 *      count; //=> 1
	 */

	var memoize =
	/*#__PURE__*/
	memoizeWith(function () {
	  return toString$2(arguments);
	});

	/**
	 * Takes a function and two values, and returns whichever value produces the
	 * smaller result when passed to the provided function.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.8.0
	 * @category Relation
	 * @sig Ord b => (a -> b) -> a -> a -> a
	 * @param {Function} f
	 * @param {*} a
	 * @param {*} b
	 * @return {*}
	 * @see R.min, R.maxBy
	 * @example
	 *
	 *      //  square :: Number -> Number
	 *      var square = n => n * n;
	 *
	 *      R.minBy(square, -3, 2); //=> 2
	 *
	 *      R.reduce(R.minBy(square), Infinity, [3, -5, 4, 1, -2]); //=> 1
	 *      R.reduce(R.minBy(square), Infinity, []); //=> Infinity
	 */

	var minBy =
	/*#__PURE__*/
	_curry3(function minBy(f, a, b) {
	  return f(b) < f(a) ? b : a;
	});

	/**
	 * Multiplies two numbers. Equivalent to `a * b` but curried.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category Math
	 * @sig Number -> Number -> Number
	 * @param {Number} a The first value.
	 * @param {Number} b The second value.
	 * @return {Number} The result of `a * b`.
	 * @see R.divide
	 * @example
	 *
	 *      var double = R.multiply(2);
	 *      var triple = R.multiply(3);
	 *      double(3);       //=>  6
	 *      triple(4);       //=> 12
	 *      R.multiply(2, 5);  //=> 10
	 */

	var multiply =
	/*#__PURE__*/
	_curry2(function multiply(a, b) {
	  return a * b;
	});

	function _createPartialApplicator(concat) {
	  return _curry2(function (fn, args) {
	    return _arity(Math.max(0, fn.length - args.length), function () {
	      return fn.apply(this, concat(args, arguments));
	    });
	  });
	}

	/**
	 * Takes a function `f` and a list of arguments, and returns a function `g`.
	 * When applied, `g` returns the result of applying `f` to the arguments
	 * provided to `g` followed by the arguments provided initially.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.10.0
	 * @category Function
	 * @sig ((a, b, c, ..., n) -> x) -> [d, e, f, ..., n] -> ((a, b, c, ...) -> x)
	 * @param {Function} f
	 * @param {Array} args
	 * @return {Function}
	 * @see R.partial
	 * @example
	 *
	 *      var greet = (salutation, title, firstName, lastName) =>
	 *        salutation + ', ' + title + ' ' + firstName + ' ' + lastName + '!';
	 *
	 *      var greetMsJaneJones = R.partialRight(greet, ['Ms.', 'Jane', 'Jones']);
	 *
	 *      greetMsJaneJones('Hello'); //=> 'Hello, Ms. Jane Jones!'
	 * @symb R.partialRight(f, [a, b])(c, d) = f(c, d, a, b)
	 */

	var partialRight =
	/*#__PURE__*/
	_createPartialApplicator(
	/*#__PURE__*/
	flip(_concat));

	/**
	 * Takes a predicate and a list or other `Filterable` object and returns the
	 * pair of filterable objects of the same type of elements which do and do not
	 * satisfy, the predicate, respectively. Filterable objects include plain objects or any object
	 * that has a filter method such as `Array`.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.4
	 * @category List
	 * @sig Filterable f => (a -> Boolean) -> f a -> [f a, f a]
	 * @param {Function} pred A predicate to determine which side the element belongs to.
	 * @param {Array} filterable the list (or other filterable) to partition.
	 * @return {Array} An array, containing first the subset of elements that satisfy the
	 *         predicate, and second the subset of elements that do not satisfy.
	 * @see R.filter, R.reject
	 * @example
	 *
	 *      R.partition(R.contains('s'), ['sss', 'ttt', 'foo', 'bars']);
	 *      // => [ [ 'sss', 'bars' ],  [ 'ttt', 'foo' ] ]
	 *
	 *      R.partition(R.contains('s'), { a: 'sss', b: 'ttt', foo: 'bars' });
	 *      // => [ { a: 'sss', foo: 'bars' }, { b: 'ttt' }  ]
	 */

	var partition =
	/*#__PURE__*/
	juxt([filter, reject]);

	/**
	 * Similar to `pick` except that this one includes a `key: undefined` pair for
	 * properties that don't exist.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category Object
	 * @sig [k] -> {k: v} -> {k: v}
	 * @param {Array} names an array of String property names to copy onto a new object
	 * @param {Object} obj The object to copy from
	 * @return {Object} A new object with only properties from `names` on it.
	 * @see R.pick
	 * @example
	 *
	 *      R.pickAll(['a', 'd'], {a: 1, b: 2, c: 3, d: 4}); //=> {a: 1, d: 4}
	 *      R.pickAll(['a', 'e', 'f'], {a: 1, b: 2, c: 3, d: 4}); //=> {a: 1, e: undefined, f: undefined}
	 */

	var pickAll =
	/*#__PURE__*/
	_curry2(function pickAll(names, obj) {
	  var result = {};
	  var idx = 0;
	  var len = names.length;

	  while (idx < len) {
	    var name = names[idx];
	    result[name] = obj[name];
	    idx += 1;
	  }

	  return result;
	});

	/**
	 * Multiplies together all the elements of a list.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category Math
	 * @sig [Number] -> Number
	 * @param {Array} list An array of numbers
	 * @return {Number} The product of all the numbers in the list.
	 * @see R.reduce
	 * @example
	 *
	 *      R.product([2,4,6,8,100,1]); //=> 38400
	 */

	var product =
	/*#__PURE__*/
	reduce(multiply, 1);

	/**
	 * Accepts a function `fn` and a list of transformer functions and returns a
	 * new curried function. When the new function is invoked, it calls the
	 * function `fn` with parameters consisting of the result of calling each
	 * supplied handler on successive arguments to the new function.
	 *
	 * If more arguments are passed to the returned function than transformer
	 * functions, those arguments are passed directly to `fn` as additional
	 * parameters. If you expect additional arguments that don't need to be
	 * transformed, although you can ignore them, it's best to pass an identity
	 * function so that the new function reports the correct arity.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category Function
	 * @sig ((x1, x2, ...) -> z) -> [(a -> x1), (b -> x2), ...] -> (a -> b -> ... -> z)
	 * @param {Function} fn The function to wrap.
	 * @param {Array} transformers A list of transformer functions
	 * @return {Function} The wrapped function.
	 * @see R.converge
	 * @example
	 *
	 *      R.useWith(Math.pow, [R.identity, R.identity])(3, 4); //=> 81
	 *      R.useWith(Math.pow, [R.identity, R.identity])(3)(4); //=> 81
	 *      R.useWith(Math.pow, [R.dec, R.inc])(3, 4); //=> 32
	 *      R.useWith(Math.pow, [R.dec, R.inc])(3)(4); //=> 32
	 * @symb R.useWith(f, [g, h])(a, b) = f(g(a), h(b))
	 */

	var useWith =
	/*#__PURE__*/
	_curry2(function useWith(fn, transformers) {
	  return curryN(transformers.length, function () {
	    var args = [];
	    var idx = 0;

	    while (idx < transformers.length) {
	      args.push(transformers[idx].call(this, arguments[idx]));
	      idx += 1;
	    }

	    return fn.apply(this, args.concat(Array.prototype.slice.call(arguments, transformers.length)));
	  });
	});

	/**
	 * Reasonable analog to SQL `select` statement.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category Object
	 * @category Relation
	 * @sig [k] -> [{k: v}] -> [{k: v}]
	 * @param {Array} props The property names to project
	 * @param {Array} objs The objects to query
	 * @return {Array} An array of objects with just the `props` properties.
	 * @example
	 *
	 *      var abby = {name: 'Abby', age: 7, hair: 'blond', grade: 2};
	 *      var fred = {name: 'Fred', age: 12, hair: 'brown', grade: 7};
	 *      var kids = [abby, fred];
	 *      R.project(['name', 'grade'], kids); //=> [{name: 'Abby', grade: 2}, {name: 'Fred', grade: 7}]
	 */

	var project =
	/*#__PURE__*/
	useWith(_map, [pickAll, identity]); // passing `identity` gives correct arity

	/**
	 * Splits a string into an array of strings based on the given
	 * separator.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category String
	 * @sig (String | RegExp) -> String -> [String]
	 * @param {String|RegExp} sep The pattern.
	 * @param {String} str The string to separate into an array.
	 * @return {Array} The array of strings from `str` separated by `str`.
	 * @see R.join
	 * @example
	 *
	 *      var pathComponents = R.split('/');
	 *      R.tail(pathComponents('/usr/local/bin/node')); //=> ['usr', 'local', 'bin', 'node']
	 *
	 *      R.split('.', 'a.b.c.xyz.d'); //=> ['a', 'b', 'c', 'xyz', 'd']
	 */

	var split =
	/*#__PURE__*/
	invoker(1, 'split');

	/**
	 * The lower case version of a string.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.9.0
	 * @category String
	 * @sig String -> String
	 * @param {String} str The string to lower case.
	 * @return {String} The lower case version of `str`.
	 * @see R.toUpper
	 * @example
	 *
	 *      R.toLower('XYZ'); //=> 'xyz'
	 */

	var toLower =
	/*#__PURE__*/
	invoker(0, 'toLowerCase');

	/**
	 * Converts an object into an array of key, value arrays. Only the object's
	 * own properties are used.
	 * Note that the order of the output array is not guaranteed to be consistent
	 * across different JS platforms.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.4.0
	 * @category Object
	 * @sig {String: *} -> [[String,*]]
	 * @param {Object} obj The object to extract from
	 * @return {Array} An array of key, value arrays from the object's own properties.
	 * @see R.fromPairs
	 * @example
	 *
	 *      R.toPairs({a: 1, b: 2, c: 3}); //=> [['a', 1], ['b', 2], ['c', 3]]
	 */

	var toPairs =
	/*#__PURE__*/
	_curry1(function toPairs(obj) {
	  var pairs = [];

	  for (var prop in obj) {
	    if (_has$1(prop, obj)) {
	      pairs[pairs.length] = [prop, obj[prop]];
	    }
	  }

	  return pairs;
	});

	/**
	 * The upper case version of a string.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.9.0
	 * @category String
	 * @sig String -> String
	 * @param {String} str The string to upper case.
	 * @return {String} The upper case version of `str`.
	 * @see R.toLower
	 * @example
	 *
	 *      R.toUpper('abc'); //=> 'ABC'
	 */

	var toUpper =
	/*#__PURE__*/
	invoker(0, 'toUpperCase');

	/**
	 * Initializes a transducer using supplied iterator function. Returns a single
	 * item by iterating through the list, successively calling the transformed
	 * iterator function and passing it an accumulator value and the current value
	 * from the array, and then passing the result to the next call.
	 *
	 * The iterator function receives two values: *(acc, value)*. It will be
	 * wrapped as a transformer to initialize the transducer. A transformer can be
	 * passed directly in place of an iterator function. In both cases, iteration
	 * may be stopped early with the [`R.reduced`](#reduced) function.
	 *
	 * A transducer is a function that accepts a transformer and returns a
	 * transformer and can be composed directly.
	 *
	 * A transformer is an an object that provides a 2-arity reducing iterator
	 * function, step, 0-arity initial value function, init, and 1-arity result
	 * extraction function, result. The step function is used as the iterator
	 * function in reduce. The result function is used to convert the final
	 * accumulator into the return type and in most cases is
	 * [`R.identity`](#identity). The init function can be used to provide an
	 * initial accumulator, but is ignored by transduce.
	 *
	 * The iteration is performed with [`R.reduce`](#reduce) after initializing the transducer.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.12.0
	 * @category List
	 * @sig (c -> c) -> ((a, b) -> a) -> a -> [b] -> a
	 * @param {Function} xf The transducer function. Receives a transformer and returns a transformer.
	 * @param {Function} fn The iterator function. Receives two values, the accumulator and the
	 *        current element from the array. Wrapped as transformer, if necessary, and used to
	 *        initialize the transducer
	 * @param {*} acc The initial accumulator value.
	 * @param {Array} list The list to iterate over.
	 * @return {*} The final, accumulated value.
	 * @see R.reduce, R.reduced, R.into
	 * @example
	 *
	 *      var numbers = [1, 2, 3, 4];
	 *      var transducer = R.compose(R.map(R.add(1)), R.take(2));
	 *      R.transduce(transducer, R.flip(R.append), [], numbers); //=> [2, 3]
	 *
	 *      var isOdd = (x) => x % 2 === 1;
	 *      var firstOddTransducer = R.compose(R.filter(isOdd), R.take(1));
	 *      R.transduce(firstOddTransducer, R.flip(R.append), [], R.range(0, 100)); //=> [1]
	 */

	var transduce =
	/*#__PURE__*/
	curryN(4, function transduce(xf, fn, acc, list) {
	  return _reduce(xf(typeof fn === 'function' ? _xwrap(fn) : fn), acc, list);
	});

	var ws = "\t\n\x0B\f\r \xA0\u1680\u180E\u2000\u2001\u2002\u2003" + "\u2004\u2005\u2006\u2007\u2008\u2009\u200A\u202F\u205F\u3000\u2028" + "\u2029\uFEFF";
	var zeroWidth = "\u200B";
	var hasProtoTrim = typeof String.prototype.trim === 'function';
	/**
	 * Removes (strips) whitespace from both ends of the string.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.6.0
	 * @category String
	 * @sig String -> String
	 * @param {String} str The string to trim.
	 * @return {String} Trimmed version of `str`.
	 * @example
	 *
	 *      R.trim('   xyz  '); //=> 'xyz'
	 *      R.map(R.trim, R.split(',', 'x, y, z')); //=> ['x', 'y', 'z']
	 */

	var _trim = !hasProtoTrim ||
	/*#__PURE__*/
	ws.trim() || !
	/*#__PURE__*/
	zeroWidth.trim() ? function trim(str) {
	  var beginRx = new RegExp('^[' + ws + '][' + ws + ']*');
	  var endRx = new RegExp('[' + ws + '][' + ws + ']*$');
	  return str.replace(beginRx, '').replace(endRx, '');
	} : function trim(str) {
	  return str.trim();
	};

	/**
	 * Combines two lists into a set (i.e. no duplicates) composed of the elements
	 * of each list.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category Relation
	 * @sig [*] -> [*] -> [*]
	 * @param {Array} as The first list.
	 * @param {Array} bs The second list.
	 * @return {Array} The first and second lists concatenated, with
	 *         duplicates removed.
	 * @example
	 *
	 *      R.union([1, 2, 3], [2, 3, 4]); //=> [1, 2, 3, 4]
	 */

	var union =
	/*#__PURE__*/
	_curry2(
	/*#__PURE__*/
	compose(uniq, _concat));

	/**
	 * Shorthand for `R.chain(R.identity)`, which removes one level of nesting from
	 * any [Chain](https://github.com/fantasyland/fantasy-land#chain).
	 *
	 * @func
	 * @memberOf R
	 * @since v0.3.0
	 * @category List
	 * @sig Chain c => c (c a) -> c a
	 * @param {*} list
	 * @return {*}
	 * @see R.flatten, R.chain
	 * @example
	 *
	 *      R.unnest([1, [2], [[3]]]); //=> [1, 2, [3]]
	 *      R.unnest([[1, 2], [3, 4], [5, 6]]); //=> [1, 2, 3, 4, 5, 6]
	 */

	var unnest =
	/*#__PURE__*/
	chain(_identity);

	// 20.3.3.1 / 15.9.4.4 Date.now()


	$export$1($export$1.S, 'Date', {
	  now: function now() {
	    return new Date().getTime();
	  }
	});

	// 19.1.2.12 Object.isFrozen(O)


	_objectSap('isFrozen', function ($isFrozen) {
	  return function isFrozen(it) {
	    return isObject(it) ? $isFrozen ? $isFrozen(it) : false : true;
	  };
	});

	var $export$k = require('./_export');

	var $some = require('./_array-methods')(3);

	$export$k($export$k.P + $export$k.F * !require('./_strict-method')([].some, true), 'Array', {
	  // 22.1.3.23 / 15.4.4.17 Array.prototype.some(callbackfn [, thisArg])
	  some: function some(callbackfn
	  /* , thisArg */
	  ) {
	    return $some(this, callbackfn, arguments[1]);
	  }
	});

	(function (global, factory) {
	  (typeof exports === "undefined" ? "undefined" : _typeof(exports)) === 'object' && typeof module !== 'undefined' ? module.exports = factory() : typeof define === 'function' && define.amd ? define(factory) : global.moment = factory();
	})(undefined, function () {

	  var hookCallback;

	  function hooks() {
	    return hookCallback.apply(null, arguments);
	  } // This is done to register the method called with moment()
	  // without creating circular dependencies.


	  function setHookCallback(callback) {
	    hookCallback = callback;
	  }

	  function isArray(input) {
	    return input instanceof Array || Object.prototype.toString.call(input) === '[object Array]';
	  }

	  function isObject(input) {
	    // IE8 will treat undefined and null as object if it wasn't for
	    // input != null
	    return input != null && Object.prototype.toString.call(input) === '[object Object]';
	  }

	  function isObjectEmpty(obj) {
	    if (Object.getOwnPropertyNames) {
	      return Object.getOwnPropertyNames(obj).length === 0;
	    } else {
	      var k;

	      for (k in obj) {
	        if (obj.hasOwnProperty(k)) {
	          return false;
	        }
	      }

	      return true;
	    }
	  }

	  function isUndefined(input) {
	    return input === void 0;
	  }

	  function isNumber(input) {
	    return typeof input === 'number' || Object.prototype.toString.call(input) === '[object Number]';
	  }

	  function isDate(input) {
	    return input instanceof Date || Object.prototype.toString.call(input) === '[object Date]';
	  }

	  function map(arr, fn) {
	    var res = [],
	        i;

	    for (i = 0; i < arr.length; ++i) {
	      res.push(fn(arr[i], i));
	    }

	    return res;
	  }

	  function hasOwnProp(a, b) {
	    return Object.prototype.hasOwnProperty.call(a, b);
	  }

	  function extend(a, b) {
	    for (var i in b) {
	      if (hasOwnProp(b, i)) {
	        a[i] = b[i];
	      }
	    }

	    if (hasOwnProp(b, 'toString')) {
	      a.toString = b.toString;
	    }

	    if (hasOwnProp(b, 'valueOf')) {
	      a.valueOf = b.valueOf;
	    }

	    return a;
	  }

	  function createUTC(input, format, locale, strict) {
	    return createLocalOrUTC(input, format, locale, strict, true).utc();
	  }

	  function defaultParsingFlags() {
	    // We need to deep clone this object.
	    return {
	      empty: false,
	      unusedTokens: [],
	      unusedInput: [],
	      overflow: -2,
	      charsLeftOver: 0,
	      nullInput: false,
	      invalidMonth: null,
	      invalidFormat: false,
	      userInvalidated: false,
	      iso: false,
	      parsedDateParts: [],
	      meridiem: null,
	      rfc2822: false,
	      weekdayMismatch: false
	    };
	  }

	  function getParsingFlags(m) {
	    if (m._pf == null) {
	      m._pf = defaultParsingFlags();
	    }

	    return m._pf;
	  }

	  var some;

	  if (Array.prototype.some) {
	    some = Array.prototype.some;
	  } else {
	    some = function some(fun) {
	      var t = Object(this);
	      var len = t.length >>> 0;

	      for (var i = 0; i < len; i++) {
	        if (i in t && fun.call(this, t[i], i, t)) {
	          return true;
	        }
	      }

	      return false;
	    };
	  }

	  function isValid(m) {
	    if (m._isValid == null) {
	      var flags = getParsingFlags(m);
	      var parsedParts = some.call(flags.parsedDateParts, function (i) {
	        return i != null;
	      });
	      var isNowValid = !isNaN(m._d.getTime()) && flags.overflow < 0 && !flags.empty && !flags.invalidMonth && !flags.invalidWeekday && !flags.weekdayMismatch && !flags.nullInput && !flags.invalidFormat && !flags.userInvalidated && (!flags.meridiem || flags.meridiem && parsedParts);

	      if (m._strict) {
	        isNowValid = isNowValid && flags.charsLeftOver === 0 && flags.unusedTokens.length === 0 && flags.bigHour === undefined;
	      }

	      if (Object.isFrozen == null || !Object.isFrozen(m)) {
	        m._isValid = isNowValid;
	      } else {
	        return isNowValid;
	      }
	    }

	    return m._isValid;
	  }

	  function createInvalid(flags) {
	    var m = createUTC(NaN);

	    if (flags != null) {
	      extend(getParsingFlags(m), flags);
	    } else {
	      getParsingFlags(m).userInvalidated = true;
	    }

	    return m;
	  } // Plugins that add properties should also add the key here (null value),
	  // so we can properly clone ourselves.


	  var momentProperties = hooks.momentProperties = [];

	  function copyConfig(to, from) {
	    var i, prop, val;

	    if (!isUndefined(from._isAMomentObject)) {
	      to._isAMomentObject = from._isAMomentObject;
	    }

	    if (!isUndefined(from._i)) {
	      to._i = from._i;
	    }

	    if (!isUndefined(from._f)) {
	      to._f = from._f;
	    }

	    if (!isUndefined(from._l)) {
	      to._l = from._l;
	    }

	    if (!isUndefined(from._strict)) {
	      to._strict = from._strict;
	    }

	    if (!isUndefined(from._tzm)) {
	      to._tzm = from._tzm;
	    }

	    if (!isUndefined(from._isUTC)) {
	      to._isUTC = from._isUTC;
	    }

	    if (!isUndefined(from._offset)) {
	      to._offset = from._offset;
	    }

	    if (!isUndefined(from._pf)) {
	      to._pf = getParsingFlags(from);
	    }

	    if (!isUndefined(from._locale)) {
	      to._locale = from._locale;
	    }

	    if (momentProperties.length > 0) {
	      for (i = 0; i < momentProperties.length; i++) {
	        prop = momentProperties[i];
	        val = from[prop];

	        if (!isUndefined(val)) {
	          to[prop] = val;
	        }
	      }
	    }

	    return to;
	  }

	  var updateInProgress = false; // Moment prototype object

	  function Moment(config) {
	    copyConfig(this, config);
	    this._d = new Date(config._d != null ? config._d.getTime() : NaN);

	    if (!this.isValid()) {
	      this._d = new Date(NaN);
	    } // Prevent infinite loop in case updateOffset creates new moment
	    // objects.


	    if (updateInProgress === false) {
	      updateInProgress = true;
	      hooks.updateOffset(this);
	      updateInProgress = false;
	    }
	  }

	  function isMoment(obj) {
	    return obj instanceof Moment || obj != null && obj._isAMomentObject != null;
	  }

	  function absFloor(number) {
	    if (number < 0) {
	      // -0 -> 0
	      return Math.ceil(number) || 0;
	    } else {
	      return Math.floor(number);
	    }
	  }

	  function toInt(argumentForCoercion) {
	    var coercedNumber = +argumentForCoercion,
	        value = 0;

	    if (coercedNumber !== 0 && isFinite(coercedNumber)) {
	      value = absFloor(coercedNumber);
	    }

	    return value;
	  } // compare two arrays, return the number of differences


	  function compareArrays(array1, array2, dontConvert) {
	    var len = Math.min(array1.length, array2.length),
	        lengthDiff = Math.abs(array1.length - array2.length),
	        diffs = 0,
	        i;

	    for (i = 0; i < len; i++) {
	      if (dontConvert && array1[i] !== array2[i] || !dontConvert && toInt(array1[i]) !== toInt(array2[i])) {
	        diffs++;
	      }
	    }

	    return diffs + lengthDiff;
	  }

	  function warn(msg) {
	    if (hooks.suppressDeprecationWarnings === false && typeof console !== 'undefined' && console.warn) {
	      console.warn('Deprecation warning: ' + msg);
	    }
	  }

	  function deprecate(msg, fn) {
	    var firstTime = true;
	    return extend(function () {
	      if (hooks.deprecationHandler != null) {
	        hooks.deprecationHandler(null, msg);
	      }

	      if (firstTime) {
	        var args = [];
	        var arg;

	        for (var i = 0; i < arguments.length; i++) {
	          arg = '';

	          if (_typeof(arguments[i]) === 'object') {
	            arg += '\n[' + i + '] ';

	            for (var key in arguments[0]) {
	              arg += key + ': ' + arguments[0][key] + ', ';
	            }

	            arg = arg.slice(0, -2); // Remove trailing comma and space
	          } else {
	            arg = arguments[i];
	          }

	          args.push(arg);
	        }

	        warn(msg + '\nArguments: ' + Array.prototype.slice.call(args).join('') + '\n' + new Error().stack);
	        firstTime = false;
	      }

	      return fn.apply(this, arguments);
	    }, fn);
	  }

	  var deprecations = {};

	  function deprecateSimple(name, msg) {
	    if (hooks.deprecationHandler != null) {
	      hooks.deprecationHandler(name, msg);
	    }

	    if (!deprecations[name]) {
	      warn(msg);
	      deprecations[name] = true;
	    }
	  }

	  hooks.suppressDeprecationWarnings = false;
	  hooks.deprecationHandler = null;

	  function isFunction(input) {
	    return input instanceof Function || Object.prototype.toString.call(input) === '[object Function]';
	  }

	  function set(config) {
	    var prop, i;

	    for (i in config) {
	      prop = config[i];

	      if (isFunction(prop)) {
	        this[i] = prop;
	      } else {
	        this['_' + i] = prop;
	      }
	    }

	    this._config = config; // Lenient ordinal parsing accepts just a number in addition to
	    // number + (possibly) stuff coming from _dayOfMonthOrdinalParse.
	    // TODO: Remove "ordinalParse" fallback in next major release.

	    this._dayOfMonthOrdinalParseLenient = new RegExp((this._dayOfMonthOrdinalParse.source || this._ordinalParse.source) + '|' + /\d{1,2}/.source);
	  }

	  function mergeConfigs(parentConfig, childConfig) {
	    var res = extend({}, parentConfig),
	        prop;

	    for (prop in childConfig) {
	      if (hasOwnProp(childConfig, prop)) {
	        if (isObject(parentConfig[prop]) && isObject(childConfig[prop])) {
	          res[prop] = {};
	          extend(res[prop], parentConfig[prop]);
	          extend(res[prop], childConfig[prop]);
	        } else if (childConfig[prop] != null) {
	          res[prop] = childConfig[prop];
	        } else {
	          delete res[prop];
	        }
	      }
	    }

	    for (prop in parentConfig) {
	      if (hasOwnProp(parentConfig, prop) && !hasOwnProp(childConfig, prop) && isObject(parentConfig[prop])) {
	        // make sure changes to properties don't modify parent config
	        res[prop] = extend({}, res[prop]);
	      }
	    }

	    return res;
	  }

	  function Locale(config) {
	    if (config != null) {
	      this.set(config);
	    }
	  }

	  var keys;

	  if (Object.keys) {
	    keys = Object.keys;
	  } else {
	    keys = function keys(obj) {
	      var i,
	          res = [];

	      for (i in obj) {
	        if (hasOwnProp(obj, i)) {
	          res.push(i);
	        }
	      }

	      return res;
	    };
	  }

	  var defaultCalendar = {
	    sameDay: '[Today at] LT',
	    nextDay: '[Tomorrow at] LT',
	    nextWeek: 'dddd [at] LT',
	    lastDay: '[Yesterday at] LT',
	    lastWeek: '[Last] dddd [at] LT',
	    sameElse: 'L'
	  };

	  function calendar(key, mom, now) {
	    var output = this._calendar[key] || this._calendar['sameElse'];
	    return isFunction(output) ? output.call(mom, now) : output;
	  }

	  var defaultLongDateFormat = {
	    LTS: 'h:mm:ss A',
	    LT: 'h:mm A',
	    L: 'MM/DD/YYYY',
	    LL: 'MMMM D, YYYY',
	    LLL: 'MMMM D, YYYY h:mm A',
	    LLLL: 'dddd, MMMM D, YYYY h:mm A'
	  };

	  function longDateFormat(key) {
	    var format = this._longDateFormat[key],
	        formatUpper = this._longDateFormat[key.toUpperCase()];

	    if (format || !formatUpper) {
	      return format;
	    }

	    this._longDateFormat[key] = formatUpper.replace(/MMMM|MM|DD|dddd/g, function (val) {
	      return val.slice(1);
	    });
	    return this._longDateFormat[key];
	  }

	  var defaultInvalidDate = 'Invalid date';

	  function invalidDate() {
	    return this._invalidDate;
	  }

	  var defaultOrdinal = '%d';
	  var defaultDayOfMonthOrdinalParse = /\d{1,2}/;

	  function ordinal(number) {
	    return this._ordinal.replace('%d', number);
	  }

	  var defaultRelativeTime = {
	    future: 'in %s',
	    past: '%s ago',
	    s: 'a few seconds',
	    ss: '%d seconds',
	    m: 'a minute',
	    mm: '%d minutes',
	    h: 'an hour',
	    hh: '%d hours',
	    d: 'a day',
	    dd: '%d days',
	    M: 'a month',
	    MM: '%d months',
	    y: 'a year',
	    yy: '%d years'
	  };

	  function relativeTime(number, withoutSuffix, string, isFuture) {
	    var output = this._relativeTime[string];
	    return isFunction(output) ? output(number, withoutSuffix, string, isFuture) : output.replace(/%d/i, number);
	  }

	  function pastFuture(diff, output) {
	    var format = this._relativeTime[diff > 0 ? 'future' : 'past'];
	    return isFunction(format) ? format(output) : format.replace(/%s/i, output);
	  }

	  var aliases = {};

	  function addUnitAlias(unit, shorthand) {
	    var lowerCase = unit.toLowerCase();
	    aliases[lowerCase] = aliases[lowerCase + 's'] = aliases[shorthand] = unit;
	  }

	  function normalizeUnits(units) {
	    return typeof units === 'string' ? aliases[units] || aliases[units.toLowerCase()] : undefined;
	  }

	  function normalizeObjectUnits(inputObject) {
	    var normalizedInput = {},
	        normalizedProp,
	        prop;

	    for (prop in inputObject) {
	      if (hasOwnProp(inputObject, prop)) {
	        normalizedProp = normalizeUnits(prop);

	        if (normalizedProp) {
	          normalizedInput[normalizedProp] = inputObject[prop];
	        }
	      }
	    }

	    return normalizedInput;
	  }

	  var priorities = {};

	  function addUnitPriority(unit, priority) {
	    priorities[unit] = priority;
	  }

	  function getPrioritizedUnits(unitsObj) {
	    var units = [];

	    for (var u in unitsObj) {
	      units.push({
	        unit: u,
	        priority: priorities[u]
	      });
	    }

	    units.sort(function (a, b) {
	      return a.priority - b.priority;
	    });
	    return units;
	  }

	  function zeroFill(number, targetLength, forceSign) {
	    var absNumber = '' + Math.abs(number),
	        zerosToFill = targetLength - absNumber.length,
	        sign = number >= 0;
	    return (sign ? forceSign ? '+' : '' : '-') + Math.pow(10, Math.max(0, zerosToFill)).toString().substr(1) + absNumber;
	  }

	  var formattingTokens = /(\[[^\[]*\])|(\\)?([Hh]mm(ss)?|Mo|MM?M?M?|Do|DDDo|DD?D?D?|ddd?d?|do?|w[o|w]?|W[o|W]?|Qo?|YYYYYY|YYYYY|YYYY|YY|gg(ggg?)?|GG(GGG?)?|e|E|a|A|hh?|HH?|kk?|mm?|ss?|S{1,9}|x|X|zz?|ZZ?|.)/g;
	  var localFormattingTokens = /(\[[^\[]*\])|(\\)?(LTS|LT|LL?L?L?|l{1,4})/g;
	  var formatFunctions = {};
	  var formatTokenFunctions = {}; // token:    'M'
	  // padded:   ['MM', 2]
	  // ordinal:  'Mo'
	  // callback: function () { this.month() + 1 }

	  function addFormatToken(token, padded, ordinal, callback) {
	    var func = callback;

	    if (typeof callback === 'string') {
	      func = function func() {
	        return this[callback]();
	      };
	    }

	    if (token) {
	      formatTokenFunctions[token] = func;
	    }

	    if (padded) {
	      formatTokenFunctions[padded[0]] = function () {
	        return zeroFill(func.apply(this, arguments), padded[1], padded[2]);
	      };
	    }

	    if (ordinal) {
	      formatTokenFunctions[ordinal] = function () {
	        return this.localeData().ordinal(func.apply(this, arguments), token);
	      };
	    }
	  }

	  function removeFormattingTokens(input) {
	    if (input.match(/\[[\s\S]/)) {
	      return input.replace(/^\[|\]$/g, '');
	    }

	    return input.replace(/\\/g, '');
	  }

	  function makeFormatFunction(format) {
	    var array = format.match(formattingTokens),
	        i,
	        length;

	    for (i = 0, length = array.length; i < length; i++) {
	      if (formatTokenFunctions[array[i]]) {
	        array[i] = formatTokenFunctions[array[i]];
	      } else {
	        array[i] = removeFormattingTokens(array[i]);
	      }
	    }

	    return function (mom) {
	      var output = '',
	          i;

	      for (i = 0; i < length; i++) {
	        output += isFunction(array[i]) ? array[i].call(mom, format) : array[i];
	      }

	      return output;
	    };
	  } // format date using native date object


	  function formatMoment(m, format) {
	    if (!m.isValid()) {
	      return m.localeData().invalidDate();
	    }

	    format = expandFormat(format, m.localeData());
	    formatFunctions[format] = formatFunctions[format] || makeFormatFunction(format);
	    return formatFunctions[format](m);
	  }

	  function expandFormat(format, locale) {
	    var i = 5;

	    function replaceLongDateFormatTokens(input) {
	      return locale.longDateFormat(input) || input;
	    }

	    localFormattingTokens.lastIndex = 0;

	    while (i >= 0 && localFormattingTokens.test(format)) {
	      format = format.replace(localFormattingTokens, replaceLongDateFormatTokens);
	      localFormattingTokens.lastIndex = 0;
	      i -= 1;
	    }

	    return format;
	  }

	  var match1 = /\d/; //       0 - 9

	  var match2 = /\d\d/; //      00 - 99

	  var match3 = /\d{3}/; //     000 - 999

	  var match4 = /\d{4}/; //    0000 - 9999

	  var match6 = /[+-]?\d{6}/; // -999999 - 999999

	  var match1to2 = /\d\d?/; //       0 - 99

	  var match3to4 = /\d\d\d\d?/; //     999 - 9999

	  var match5to6 = /\d\d\d\d\d\d?/; //   99999 - 999999

	  var match1to3 = /\d{1,3}/; //       0 - 999

	  var match1to4 = /\d{1,4}/; //       0 - 9999

	  var match1to6 = /[+-]?\d{1,6}/; // -999999 - 999999

	  var matchUnsigned = /\d+/; //       0 - inf

	  var matchSigned = /[+-]?\d+/; //    -inf - inf

	  var matchOffset = /Z|[+-]\d\d:?\d\d/gi; // +00:00 -00:00 +0000 -0000 or Z

	  var matchShortOffset = /Z|[+-]\d\d(?::?\d\d)?/gi; // +00 -00 +00:00 -00:00 +0000 -0000 or Z

	  var matchTimestamp = /[+-]?\d+(\.\d{1,3})?/; // 123456789 123456789.123
	  // any word (or two) characters or numbers including two/three word month in arabic.
	  // includes scottish gaelic two word and hyphenated months

	  var matchWord = /[0-9]{0,256}['a-z\u00A0-\u05FF\u0700-\uD7FF\uF900-\uFDCF\uFDF0-\uFF07\uFF10-\uFFEF]{1,256}|[\u0600-\u06FF\/]{1,256}(\s*?[\u0600-\u06FF]{1,256}){1,2}/i;
	  var regexes = {};

	  function addRegexToken(token, regex, strictRegex) {
	    regexes[token] = isFunction(regex) ? regex : function (isStrict, localeData) {
	      return isStrict && strictRegex ? strictRegex : regex;
	    };
	  }

	  function getParseRegexForToken(token, config) {
	    if (!hasOwnProp(regexes, token)) {
	      return new RegExp(unescapeFormat(token));
	    }

	    return regexes[token](config._strict, config._locale);
	  } // Code from http://stackoverflow.com/questions/3561493/is-there-a-regexp-escape-function-in-javascript


	  function unescapeFormat(s) {
	    return regexEscape(s.replace('\\', '').replace(/\\(\[)|\\(\])|\[([^\]\[]*)\]|\\(.)/g, function (matched, p1, p2, p3, p4) {
	      return p1 || p2 || p3 || p4;
	    }));
	  }

	  function regexEscape(s) {
	    return s.replace(/[-\/\\^$*+?.()|[\]{}]/g, '\\$&');
	  }

	  var tokens = {};

	  function addParseToken(token, callback) {
	    var i,
	        func = callback;

	    if (typeof token === 'string') {
	      token = [token];
	    }

	    if (isNumber(callback)) {
	      func = function func(input, array) {
	        array[callback] = toInt(input);
	      };
	    }

	    for (i = 0; i < token.length; i++) {
	      tokens[token[i]] = func;
	    }
	  }

	  function addWeekParseToken(token, callback) {
	    addParseToken(token, function (input, array, config, token) {
	      config._w = config._w || {};
	      callback(input, config._w, config, token);
	    });
	  }

	  function addTimeToArrayFromToken(token, input, config) {
	    if (input != null && hasOwnProp(tokens, token)) {
	      tokens[token](input, config._a, config, token);
	    }
	  }

	  var YEAR = 0;
	  var MONTH = 1;
	  var DATE = 2;
	  var HOUR = 3;
	  var MINUTE = 4;
	  var SECOND = 5;
	  var MILLISECOND = 6;
	  var WEEK = 7;
	  var WEEKDAY = 8; // FORMATTING

	  addFormatToken('Y', 0, 0, function () {
	    var y = this.year();
	    return y <= 9999 ? '' + y : '+' + y;
	  });
	  addFormatToken(0, ['YY', 2], 0, function () {
	    return this.year() % 100;
	  });
	  addFormatToken(0, ['YYYY', 4], 0, 'year');
	  addFormatToken(0, ['YYYYY', 5], 0, 'year');
	  addFormatToken(0, ['YYYYYY', 6, true], 0, 'year'); // ALIASES

	  addUnitAlias('year', 'y'); // PRIORITIES

	  addUnitPriority('year', 1); // PARSING

	  addRegexToken('Y', matchSigned);
	  addRegexToken('YY', match1to2, match2);
	  addRegexToken('YYYY', match1to4, match4);
	  addRegexToken('YYYYY', match1to6, match6);
	  addRegexToken('YYYYYY', match1to6, match6);
	  addParseToken(['YYYYY', 'YYYYYY'], YEAR);
	  addParseToken('YYYY', function (input, array) {
	    array[YEAR] = input.length === 2 ? hooks.parseTwoDigitYear(input) : toInt(input);
	  });
	  addParseToken('YY', function (input, array) {
	    array[YEAR] = hooks.parseTwoDigitYear(input);
	  });
	  addParseToken('Y', function (input, array) {
	    array[YEAR] = parseInt(input, 10);
	  }); // HELPERS

	  function daysInYear(year) {
	    return isLeapYear(year) ? 366 : 365;
	  }

	  function isLeapYear(year) {
	    return year % 4 === 0 && year % 100 !== 0 || year % 400 === 0;
	  } // HOOKS


	  hooks.parseTwoDigitYear = function (input) {
	    return toInt(input) + (toInt(input) > 68 ? 1900 : 2000);
	  }; // MOMENTS


	  var getSetYear = makeGetSet('FullYear', true);

	  function getIsLeapYear() {
	    return isLeapYear(this.year());
	  }

	  function makeGetSet(unit, keepTime) {
	    return function (value) {
	      if (value != null) {
	        set$1(this, unit, value);
	        hooks.updateOffset(this, keepTime);
	        return this;
	      } else {
	        return get(this, unit);
	      }
	    };
	  }

	  function get(mom, unit) {
	    return mom.isValid() ? mom._d['get' + (mom._isUTC ? 'UTC' : '') + unit]() : NaN;
	  }

	  function set$1(mom, unit, value) {
	    if (mom.isValid() && !isNaN(value)) {
	      if (unit === 'FullYear' && isLeapYear(mom.year()) && mom.month() === 1 && mom.date() === 29) {
	        mom._d['set' + (mom._isUTC ? 'UTC' : '') + unit](value, mom.month(), daysInMonth(value, mom.month()));
	      } else {
	        mom._d['set' + (mom._isUTC ? 'UTC' : '') + unit](value);
	      }
	    }
	  } // MOMENTS


	  function stringGet(units) {
	    units = normalizeUnits(units);

	    if (isFunction(this[units])) {
	      return this[units]();
	    }

	    return this;
	  }

	  function stringSet(units, value) {
	    if (_typeof(units) === 'object') {
	      units = normalizeObjectUnits(units);
	      var prioritized = getPrioritizedUnits(units);

	      for (var i = 0; i < prioritized.length; i++) {
	        this[prioritized[i].unit](units[prioritized[i].unit]);
	      }
	    } else {
	      units = normalizeUnits(units);

	      if (isFunction(this[units])) {
	        return this[units](value);
	      }
	    }

	    return this;
	  }

	  function mod(n, x) {
	    return (n % x + x) % x;
	  }

	  var indexOf;

	  if (Array.prototype.indexOf) {
	    indexOf = Array.prototype.indexOf;
	  } else {
	    indexOf = function indexOf(o) {
	      // I know
	      var i;

	      for (i = 0; i < this.length; ++i) {
	        if (this[i] === o) {
	          return i;
	        }
	      }

	      return -1;
	    };
	  }

	  function daysInMonth(year, month) {
	    if (isNaN(year) || isNaN(month)) {
	      return NaN;
	    }

	    var modMonth = mod(month, 12);
	    year += (month - modMonth) / 12;
	    return modMonth === 1 ? isLeapYear(year) ? 29 : 28 : 31 - modMonth % 7 % 2;
	  } // FORMATTING


	  addFormatToken('M', ['MM', 2], 'Mo', function () {
	    return this.month() + 1;
	  });
	  addFormatToken('MMM', 0, 0, function (format) {
	    return this.localeData().monthsShort(this, format);
	  });
	  addFormatToken('MMMM', 0, 0, function (format) {
	    return this.localeData().months(this, format);
	  }); // ALIASES

	  addUnitAlias('month', 'M'); // PRIORITY

	  addUnitPriority('month', 8); // PARSING

	  addRegexToken('M', match1to2);
	  addRegexToken('MM', match1to2, match2);
	  addRegexToken('MMM', function (isStrict, locale) {
	    return locale.monthsShortRegex(isStrict);
	  });
	  addRegexToken('MMMM', function (isStrict, locale) {
	    return locale.monthsRegex(isStrict);
	  });
	  addParseToken(['M', 'MM'], function (input, array) {
	    array[MONTH] = toInt(input) - 1;
	  });
	  addParseToken(['MMM', 'MMMM'], function (input, array, config, token) {
	    var month = config._locale.monthsParse(input, token, config._strict); // if we didn't find a month name, mark the date as invalid.


	    if (month != null) {
	      array[MONTH] = month;
	    } else {
	      getParsingFlags(config).invalidMonth = input;
	    }
	  }); // LOCALES

	  var MONTHS_IN_FORMAT = /D[oD]?(\[[^\[\]]*\]|\s)+MMMM?/;
	  var defaultLocaleMonths = 'January_February_March_April_May_June_July_August_September_October_November_December'.split('_');

	  function localeMonths(m, format) {
	    if (!m) {
	      return isArray(this._months) ? this._months : this._months['standalone'];
	    }

	    return isArray(this._months) ? this._months[m.month()] : this._months[(this._months.isFormat || MONTHS_IN_FORMAT).test(format) ? 'format' : 'standalone'][m.month()];
	  }

	  var defaultLocaleMonthsShort = 'Jan_Feb_Mar_Apr_May_Jun_Jul_Aug_Sep_Oct_Nov_Dec'.split('_');

	  function localeMonthsShort(m, format) {
	    if (!m) {
	      return isArray(this._monthsShort) ? this._monthsShort : this._monthsShort['standalone'];
	    }

	    return isArray(this._monthsShort) ? this._monthsShort[m.month()] : this._monthsShort[MONTHS_IN_FORMAT.test(format) ? 'format' : 'standalone'][m.month()];
	  }

	  function handleStrictParse(monthName, format, strict) {
	    var i,
	        ii,
	        mom,
	        llc = monthName.toLocaleLowerCase();

	    if (!this._monthsParse) {
	      // this is not used
	      this._monthsParse = [];
	      this._longMonthsParse = [];
	      this._shortMonthsParse = [];

	      for (i = 0; i < 12; ++i) {
	        mom = createUTC([2000, i]);
	        this._shortMonthsParse[i] = this.monthsShort(mom, '').toLocaleLowerCase();
	        this._longMonthsParse[i] = this.months(mom, '').toLocaleLowerCase();
	      }
	    }

	    if (strict) {
	      if (format === 'MMM') {
	        ii = indexOf.call(this._shortMonthsParse, llc);
	        return ii !== -1 ? ii : null;
	      } else {
	        ii = indexOf.call(this._longMonthsParse, llc);
	        return ii !== -1 ? ii : null;
	      }
	    } else {
	      if (format === 'MMM') {
	        ii = indexOf.call(this._shortMonthsParse, llc);

	        if (ii !== -1) {
	          return ii;
	        }

	        ii = indexOf.call(this._longMonthsParse, llc);
	        return ii !== -1 ? ii : null;
	      } else {
	        ii = indexOf.call(this._longMonthsParse, llc);

	        if (ii !== -1) {
	          return ii;
	        }

	        ii = indexOf.call(this._shortMonthsParse, llc);
	        return ii !== -1 ? ii : null;
	      }
	    }
	  }

	  function localeMonthsParse(monthName, format, strict) {
	    var i, mom, regex;

	    if (this._monthsParseExact) {
	      return handleStrictParse.call(this, monthName, format, strict);
	    }

	    if (!this._monthsParse) {
	      this._monthsParse = [];
	      this._longMonthsParse = [];
	      this._shortMonthsParse = [];
	    } // TODO: add sorting
	    // Sorting makes sure if one month (or abbr) is a prefix of another
	    // see sorting in computeMonthsParse


	    for (i = 0; i < 12; i++) {
	      // make the regex if we don't have it already
	      mom = createUTC([2000, i]);

	      if (strict && !this._longMonthsParse[i]) {
	        this._longMonthsParse[i] = new RegExp('^' + this.months(mom, '').replace('.', '') + '$', 'i');
	        this._shortMonthsParse[i] = new RegExp('^' + this.monthsShort(mom, '').replace('.', '') + '$', 'i');
	      }

	      if (!strict && !this._monthsParse[i]) {
	        regex = '^' + this.months(mom, '') + '|^' + this.monthsShort(mom, '');
	        this._monthsParse[i] = new RegExp(regex.replace('.', ''), 'i');
	      } // test the regex


	      if (strict && format === 'MMMM' && this._longMonthsParse[i].test(monthName)) {
	        return i;
	      } else if (strict && format === 'MMM' && this._shortMonthsParse[i].test(monthName)) {
	        return i;
	      } else if (!strict && this._monthsParse[i].test(monthName)) {
	        return i;
	      }
	    }
	  } // MOMENTS


	  function setMonth(mom, value) {
	    var dayOfMonth;

	    if (!mom.isValid()) {
	      // No op
	      return mom;
	    }

	    if (typeof value === 'string') {
	      if (/^\d+$/.test(value)) {
	        value = toInt(value);
	      } else {
	        value = mom.localeData().monthsParse(value); // TODO: Another silent failure?

	        if (!isNumber(value)) {
	          return mom;
	        }
	      }
	    }

	    dayOfMonth = Math.min(mom.date(), daysInMonth(mom.year(), value));

	    mom._d['set' + (mom._isUTC ? 'UTC' : '') + 'Month'](value, dayOfMonth);

	    return mom;
	  }

	  function getSetMonth(value) {
	    if (value != null) {
	      setMonth(this, value);
	      hooks.updateOffset(this, true);
	      return this;
	    } else {
	      return get(this, 'Month');
	    }
	  }

	  function getDaysInMonth() {
	    return daysInMonth(this.year(), this.month());
	  }

	  var defaultMonthsShortRegex = matchWord;

	  function monthsShortRegex(isStrict) {
	    if (this._monthsParseExact) {
	      if (!hasOwnProp(this, '_monthsRegex')) {
	        computeMonthsParse.call(this);
	      }

	      if (isStrict) {
	        return this._monthsShortStrictRegex;
	      } else {
	        return this._monthsShortRegex;
	      }
	    } else {
	      if (!hasOwnProp(this, '_monthsShortRegex')) {
	        this._monthsShortRegex = defaultMonthsShortRegex;
	      }

	      return this._monthsShortStrictRegex && isStrict ? this._monthsShortStrictRegex : this._monthsShortRegex;
	    }
	  }

	  var defaultMonthsRegex = matchWord;

	  function monthsRegex(isStrict) {
	    if (this._monthsParseExact) {
	      if (!hasOwnProp(this, '_monthsRegex')) {
	        computeMonthsParse.call(this);
	      }

	      if (isStrict) {
	        return this._monthsStrictRegex;
	      } else {
	        return this._monthsRegex;
	      }
	    } else {
	      if (!hasOwnProp(this, '_monthsRegex')) {
	        this._monthsRegex = defaultMonthsRegex;
	      }

	      return this._monthsStrictRegex && isStrict ? this._monthsStrictRegex : this._monthsRegex;
	    }
	  }

	  function computeMonthsParse() {
	    function cmpLenRev(a, b) {
	      return b.length - a.length;
	    }

	    var shortPieces = [],
	        longPieces = [],
	        mixedPieces = [],
	        i,
	        mom;

	    for (i = 0; i < 12; i++) {
	      // make the regex if we don't have it already
	      mom = createUTC([2000, i]);
	      shortPieces.push(this.monthsShort(mom, ''));
	      longPieces.push(this.months(mom, ''));
	      mixedPieces.push(this.months(mom, ''));
	      mixedPieces.push(this.monthsShort(mom, ''));
	    } // Sorting makes sure if one month (or abbr) is a prefix of another it
	    // will match the longer piece.


	    shortPieces.sort(cmpLenRev);
	    longPieces.sort(cmpLenRev);
	    mixedPieces.sort(cmpLenRev);

	    for (i = 0; i < 12; i++) {
	      shortPieces[i] = regexEscape(shortPieces[i]);
	      longPieces[i] = regexEscape(longPieces[i]);
	    }

	    for (i = 0; i < 24; i++) {
	      mixedPieces[i] = regexEscape(mixedPieces[i]);
	    }

	    this._monthsRegex = new RegExp('^(' + mixedPieces.join('|') + ')', 'i');
	    this._monthsShortRegex = this._monthsRegex;
	    this._monthsStrictRegex = new RegExp('^(' + longPieces.join('|') + ')', 'i');
	    this._monthsShortStrictRegex = new RegExp('^(' + shortPieces.join('|') + ')', 'i');
	  }

	  function createDate(y, m, d, h, M, s, ms) {
	    // can't just apply() to create a date:
	    // https://stackoverflow.com/q/181348
	    var date = new Date(y, m, d, h, M, s, ms); // the date constructor remaps years 0-99 to 1900-1999

	    if (y < 100 && y >= 0 && isFinite(date.getFullYear())) {
	      date.setFullYear(y);
	    }

	    return date;
	  }

	  function createUTCDate(y) {
	    var date = new Date(Date.UTC.apply(null, arguments)); // the Date.UTC function remaps years 0-99 to 1900-1999

	    if (y < 100 && y >= 0 && isFinite(date.getUTCFullYear())) {
	      date.setUTCFullYear(y);
	    }

	    return date;
	  } // start-of-first-week - start-of-year


	  function firstWeekOffset(year, dow, doy) {
	    var // first-week day -- which january is always in the first week (4 for iso, 1 for other)
	    fwd = 7 + dow - doy,
	        // first-week day local weekday -- which local weekday is fwd
	    fwdlw = (7 + createUTCDate(year, 0, fwd).getUTCDay() - dow) % 7;
	    return -fwdlw + fwd - 1;
	  } // https://en.wikipedia.org/wiki/ISO_week_date#Calculating_a_date_given_the_year.2C_week_number_and_weekday


	  function dayOfYearFromWeeks(year, week, weekday, dow, doy) {
	    var localWeekday = (7 + weekday - dow) % 7,
	        weekOffset = firstWeekOffset(year, dow, doy),
	        dayOfYear = 1 + 7 * (week - 1) + localWeekday + weekOffset,
	        resYear,
	        resDayOfYear;

	    if (dayOfYear <= 0) {
	      resYear = year - 1;
	      resDayOfYear = daysInYear(resYear) + dayOfYear;
	    } else if (dayOfYear > daysInYear(year)) {
	      resYear = year + 1;
	      resDayOfYear = dayOfYear - daysInYear(year);
	    } else {
	      resYear = year;
	      resDayOfYear = dayOfYear;
	    }

	    return {
	      year: resYear,
	      dayOfYear: resDayOfYear
	    };
	  }

	  function weekOfYear(mom, dow, doy) {
	    var weekOffset = firstWeekOffset(mom.year(), dow, doy),
	        week = Math.floor((mom.dayOfYear() - weekOffset - 1) / 7) + 1,
	        resWeek,
	        resYear;

	    if (week < 1) {
	      resYear = mom.year() - 1;
	      resWeek = week + weeksInYear(resYear, dow, doy);
	    } else if (week > weeksInYear(mom.year(), dow, doy)) {
	      resWeek = week - weeksInYear(mom.year(), dow, doy);
	      resYear = mom.year() + 1;
	    } else {
	      resYear = mom.year();
	      resWeek = week;
	    }

	    return {
	      week: resWeek,
	      year: resYear
	    };
	  }

	  function weeksInYear(year, dow, doy) {
	    var weekOffset = firstWeekOffset(year, dow, doy),
	        weekOffsetNext = firstWeekOffset(year + 1, dow, doy);
	    return (daysInYear(year) - weekOffset + weekOffsetNext) / 7;
	  } // FORMATTING


	  addFormatToken('w', ['ww', 2], 'wo', 'week');
	  addFormatToken('W', ['WW', 2], 'Wo', 'isoWeek'); // ALIASES

	  addUnitAlias('week', 'w');
	  addUnitAlias('isoWeek', 'W'); // PRIORITIES

	  addUnitPriority('week', 5);
	  addUnitPriority('isoWeek', 5); // PARSING

	  addRegexToken('w', match1to2);
	  addRegexToken('ww', match1to2, match2);
	  addRegexToken('W', match1to2);
	  addRegexToken('WW', match1to2, match2);
	  addWeekParseToken(['w', 'ww', 'W', 'WW'], function (input, week, config, token) {
	    week[token.substr(0, 1)] = toInt(input);
	  }); // HELPERS
	  // LOCALES

	  function localeWeek(mom) {
	    return weekOfYear(mom, this._week.dow, this._week.doy).week;
	  }

	  var defaultLocaleWeek = {
	    dow: 0,
	    // Sunday is the first day of the week.
	    doy: 6 // The week that contains Jan 1st is the first week of the year.

	  };

	  function localeFirstDayOfWeek() {
	    return this._week.dow;
	  }

	  function localeFirstDayOfYear() {
	    return this._week.doy;
	  } // MOMENTS


	  function getSetWeek(input) {
	    var week = this.localeData().week(this);
	    return input == null ? week : this.add((input - week) * 7, 'd');
	  }

	  function getSetISOWeek(input) {
	    var week = weekOfYear(this, 1, 4).week;
	    return input == null ? week : this.add((input - week) * 7, 'd');
	  } // FORMATTING


	  addFormatToken('d', 0, 'do', 'day');
	  addFormatToken('dd', 0, 0, function (format) {
	    return this.localeData().weekdaysMin(this, format);
	  });
	  addFormatToken('ddd', 0, 0, function (format) {
	    return this.localeData().weekdaysShort(this, format);
	  });
	  addFormatToken('dddd', 0, 0, function (format) {
	    return this.localeData().weekdays(this, format);
	  });
	  addFormatToken('e', 0, 0, 'weekday');
	  addFormatToken('E', 0, 0, 'isoWeekday'); // ALIASES

	  addUnitAlias('day', 'd');
	  addUnitAlias('weekday', 'e');
	  addUnitAlias('isoWeekday', 'E'); // PRIORITY

	  addUnitPriority('day', 11);
	  addUnitPriority('weekday', 11);
	  addUnitPriority('isoWeekday', 11); // PARSING

	  addRegexToken('d', match1to2);
	  addRegexToken('e', match1to2);
	  addRegexToken('E', match1to2);
	  addRegexToken('dd', function (isStrict, locale) {
	    return locale.weekdaysMinRegex(isStrict);
	  });
	  addRegexToken('ddd', function (isStrict, locale) {
	    return locale.weekdaysShortRegex(isStrict);
	  });
	  addRegexToken('dddd', function (isStrict, locale) {
	    return locale.weekdaysRegex(isStrict);
	  });
	  addWeekParseToken(['dd', 'ddd', 'dddd'], function (input, week, config, token) {
	    var weekday = config._locale.weekdaysParse(input, token, config._strict); // if we didn't get a weekday name, mark the date as invalid


	    if (weekday != null) {
	      week.d = weekday;
	    } else {
	      getParsingFlags(config).invalidWeekday = input;
	    }
	  });
	  addWeekParseToken(['d', 'e', 'E'], function (input, week, config, token) {
	    week[token] = toInt(input);
	  }); // HELPERS

	  function parseWeekday(input, locale) {
	    if (typeof input !== 'string') {
	      return input;
	    }

	    if (!isNaN(input)) {
	      return parseInt(input, 10);
	    }

	    input = locale.weekdaysParse(input);

	    if (typeof input === 'number') {
	      return input;
	    }

	    return null;
	  }

	  function parseIsoWeekday(input, locale) {
	    if (typeof input === 'string') {
	      return locale.weekdaysParse(input) % 7 || 7;
	    }

	    return isNaN(input) ? null : input;
	  } // LOCALES


	  var defaultLocaleWeekdays = 'Sunday_Monday_Tuesday_Wednesday_Thursday_Friday_Saturday'.split('_');

	  function localeWeekdays(m, format) {
	    if (!m) {
	      return isArray(this._weekdays) ? this._weekdays : this._weekdays['standalone'];
	    }

	    return isArray(this._weekdays) ? this._weekdays[m.day()] : this._weekdays[this._weekdays.isFormat.test(format) ? 'format' : 'standalone'][m.day()];
	  }

	  var defaultLocaleWeekdaysShort = 'Sun_Mon_Tue_Wed_Thu_Fri_Sat'.split('_');

	  function localeWeekdaysShort(m) {
	    return m ? this._weekdaysShort[m.day()] : this._weekdaysShort;
	  }

	  var defaultLocaleWeekdaysMin = 'Su_Mo_Tu_We_Th_Fr_Sa'.split('_');

	  function localeWeekdaysMin(m) {
	    return m ? this._weekdaysMin[m.day()] : this._weekdaysMin;
	  }

	  function handleStrictParse$1(weekdayName, format, strict) {
	    var i,
	        ii,
	        mom,
	        llc = weekdayName.toLocaleLowerCase();

	    if (!this._weekdaysParse) {
	      this._weekdaysParse = [];
	      this._shortWeekdaysParse = [];
	      this._minWeekdaysParse = [];

	      for (i = 0; i < 7; ++i) {
	        mom = createUTC([2000, 1]).day(i);
	        this._minWeekdaysParse[i] = this.weekdaysMin(mom, '').toLocaleLowerCase();
	        this._shortWeekdaysParse[i] = this.weekdaysShort(mom, '').toLocaleLowerCase();
	        this._weekdaysParse[i] = this.weekdays(mom, '').toLocaleLowerCase();
	      }
	    }

	    if (strict) {
	      if (format === 'dddd') {
	        ii = indexOf.call(this._weekdaysParse, llc);
	        return ii !== -1 ? ii : null;
	      } else if (format === 'ddd') {
	        ii = indexOf.call(this._shortWeekdaysParse, llc);
	        return ii !== -1 ? ii : null;
	      } else {
	        ii = indexOf.call(this._minWeekdaysParse, llc);
	        return ii !== -1 ? ii : null;
	      }
	    } else {
	      if (format === 'dddd') {
	        ii = indexOf.call(this._weekdaysParse, llc);

	        if (ii !== -1) {
	          return ii;
	        }

	        ii = indexOf.call(this._shortWeekdaysParse, llc);

	        if (ii !== -1) {
	          return ii;
	        }

	        ii = indexOf.call(this._minWeekdaysParse, llc);
	        return ii !== -1 ? ii : null;
	      } else if (format === 'ddd') {
	        ii = indexOf.call(this._shortWeekdaysParse, llc);

	        if (ii !== -1) {
	          return ii;
	        }

	        ii = indexOf.call(this._weekdaysParse, llc);

	        if (ii !== -1) {
	          return ii;
	        }

	        ii = indexOf.call(this._minWeekdaysParse, llc);
	        return ii !== -1 ? ii : null;
	      } else {
	        ii = indexOf.call(this._minWeekdaysParse, llc);

	        if (ii !== -1) {
	          return ii;
	        }

	        ii = indexOf.call(this._weekdaysParse, llc);

	        if (ii !== -1) {
	          return ii;
	        }

	        ii = indexOf.call(this._shortWeekdaysParse, llc);
	        return ii !== -1 ? ii : null;
	      }
	    }
	  }

	  function localeWeekdaysParse(weekdayName, format, strict) {
	    var i, mom, regex;

	    if (this._weekdaysParseExact) {
	      return handleStrictParse$1.call(this, weekdayName, format, strict);
	    }

	    if (!this._weekdaysParse) {
	      this._weekdaysParse = [];
	      this._minWeekdaysParse = [];
	      this._shortWeekdaysParse = [];
	      this._fullWeekdaysParse = [];
	    }

	    for (i = 0; i < 7; i++) {
	      // make the regex if we don't have it already
	      mom = createUTC([2000, 1]).day(i);

	      if (strict && !this._fullWeekdaysParse[i]) {
	        this._fullWeekdaysParse[i] = new RegExp('^' + this.weekdays(mom, '').replace('.', '\\.?') + '$', 'i');
	        this._shortWeekdaysParse[i] = new RegExp('^' + this.weekdaysShort(mom, '').replace('.', '\\.?') + '$', 'i');
	        this._minWeekdaysParse[i] = new RegExp('^' + this.weekdaysMin(mom, '').replace('.', '\\.?') + '$', 'i');
	      }

	      if (!this._weekdaysParse[i]) {
	        regex = '^' + this.weekdays(mom, '') + '|^' + this.weekdaysShort(mom, '') + '|^' + this.weekdaysMin(mom, '');
	        this._weekdaysParse[i] = new RegExp(regex.replace('.', ''), 'i');
	      } // test the regex


	      if (strict && format === 'dddd' && this._fullWeekdaysParse[i].test(weekdayName)) {
	        return i;
	      } else if (strict && format === 'ddd' && this._shortWeekdaysParse[i].test(weekdayName)) {
	        return i;
	      } else if (strict && format === 'dd' && this._minWeekdaysParse[i].test(weekdayName)) {
	        return i;
	      } else if (!strict && this._weekdaysParse[i].test(weekdayName)) {
	        return i;
	      }
	    }
	  } // MOMENTS


	  function getSetDayOfWeek(input) {
	    if (!this.isValid()) {
	      return input != null ? this : NaN;
	    }

	    var day = this._isUTC ? this._d.getUTCDay() : this._d.getDay();

	    if (input != null) {
	      input = parseWeekday(input, this.localeData());
	      return this.add(input - day, 'd');
	    } else {
	      return day;
	    }
	  }

	  function getSetLocaleDayOfWeek(input) {
	    if (!this.isValid()) {
	      return input != null ? this : NaN;
	    }

	    var weekday = (this.day() + 7 - this.localeData()._week.dow) % 7;
	    return input == null ? weekday : this.add(input - weekday, 'd');
	  }

	  function getSetISODayOfWeek(input) {
	    if (!this.isValid()) {
	      return input != null ? this : NaN;
	    } // behaves the same as moment#day except
	    // as a getter, returns 7 instead of 0 (1-7 range instead of 0-6)
	    // as a setter, sunday should belong to the previous week.


	    if (input != null) {
	      var weekday = parseIsoWeekday(input, this.localeData());
	      return this.day(this.day() % 7 ? weekday : weekday - 7);
	    } else {
	      return this.day() || 7;
	    }
	  }

	  var defaultWeekdaysRegex = matchWord;

	  function weekdaysRegex(isStrict) {
	    if (this._weekdaysParseExact) {
	      if (!hasOwnProp(this, '_weekdaysRegex')) {
	        computeWeekdaysParse.call(this);
	      }

	      if (isStrict) {
	        return this._weekdaysStrictRegex;
	      } else {
	        return this._weekdaysRegex;
	      }
	    } else {
	      if (!hasOwnProp(this, '_weekdaysRegex')) {
	        this._weekdaysRegex = defaultWeekdaysRegex;
	      }

	      return this._weekdaysStrictRegex && isStrict ? this._weekdaysStrictRegex : this._weekdaysRegex;
	    }
	  }

	  var defaultWeekdaysShortRegex = matchWord;

	  function weekdaysShortRegex(isStrict) {
	    if (this._weekdaysParseExact) {
	      if (!hasOwnProp(this, '_weekdaysRegex')) {
	        computeWeekdaysParse.call(this);
	      }

	      if (isStrict) {
	        return this._weekdaysShortStrictRegex;
	      } else {
	        return this._weekdaysShortRegex;
	      }
	    } else {
	      if (!hasOwnProp(this, '_weekdaysShortRegex')) {
	        this._weekdaysShortRegex = defaultWeekdaysShortRegex;
	      }

	      return this._weekdaysShortStrictRegex && isStrict ? this._weekdaysShortStrictRegex : this._weekdaysShortRegex;
	    }
	  }

	  var defaultWeekdaysMinRegex = matchWord;

	  function weekdaysMinRegex(isStrict) {
	    if (this._weekdaysParseExact) {
	      if (!hasOwnProp(this, '_weekdaysRegex')) {
	        computeWeekdaysParse.call(this);
	      }

	      if (isStrict) {
	        return this._weekdaysMinStrictRegex;
	      } else {
	        return this._weekdaysMinRegex;
	      }
	    } else {
	      if (!hasOwnProp(this, '_weekdaysMinRegex')) {
	        this._weekdaysMinRegex = defaultWeekdaysMinRegex;
	      }

	      return this._weekdaysMinStrictRegex && isStrict ? this._weekdaysMinStrictRegex : this._weekdaysMinRegex;
	    }
	  }

	  function computeWeekdaysParse() {
	    function cmpLenRev(a, b) {
	      return b.length - a.length;
	    }

	    var minPieces = [],
	        shortPieces = [],
	        longPieces = [],
	        mixedPieces = [],
	        i,
	        mom,
	        minp,
	        shortp,
	        longp;

	    for (i = 0; i < 7; i++) {
	      // make the regex if we don't have it already
	      mom = createUTC([2000, 1]).day(i);
	      minp = this.weekdaysMin(mom, '');
	      shortp = this.weekdaysShort(mom, '');
	      longp = this.weekdays(mom, '');
	      minPieces.push(minp);
	      shortPieces.push(shortp);
	      longPieces.push(longp);
	      mixedPieces.push(minp);
	      mixedPieces.push(shortp);
	      mixedPieces.push(longp);
	    } // Sorting makes sure if one weekday (or abbr) is a prefix of another it
	    // will match the longer piece.


	    minPieces.sort(cmpLenRev);
	    shortPieces.sort(cmpLenRev);
	    longPieces.sort(cmpLenRev);
	    mixedPieces.sort(cmpLenRev);

	    for (i = 0; i < 7; i++) {
	      shortPieces[i] = regexEscape(shortPieces[i]);
	      longPieces[i] = regexEscape(longPieces[i]);
	      mixedPieces[i] = regexEscape(mixedPieces[i]);
	    }

	    this._weekdaysRegex = new RegExp('^(' + mixedPieces.join('|') + ')', 'i');
	    this._weekdaysShortRegex = this._weekdaysRegex;
	    this._weekdaysMinRegex = this._weekdaysRegex;
	    this._weekdaysStrictRegex = new RegExp('^(' + longPieces.join('|') + ')', 'i');
	    this._weekdaysShortStrictRegex = new RegExp('^(' + shortPieces.join('|') + ')', 'i');
	    this._weekdaysMinStrictRegex = new RegExp('^(' + minPieces.join('|') + ')', 'i');
	  } // FORMATTING


	  function hFormat() {
	    return this.hours() % 12 || 12;
	  }

	  function kFormat() {
	    return this.hours() || 24;
	  }

	  addFormatToken('H', ['HH', 2], 0, 'hour');
	  addFormatToken('h', ['hh', 2], 0, hFormat);
	  addFormatToken('k', ['kk', 2], 0, kFormat);
	  addFormatToken('hmm', 0, 0, function () {
	    return '' + hFormat.apply(this) + zeroFill(this.minutes(), 2);
	  });
	  addFormatToken('hmmss', 0, 0, function () {
	    return '' + hFormat.apply(this) + zeroFill(this.minutes(), 2) + zeroFill(this.seconds(), 2);
	  });
	  addFormatToken('Hmm', 0, 0, function () {
	    return '' + this.hours() + zeroFill(this.minutes(), 2);
	  });
	  addFormatToken('Hmmss', 0, 0, function () {
	    return '' + this.hours() + zeroFill(this.minutes(), 2) + zeroFill(this.seconds(), 2);
	  });

	  function meridiem(token, lowercase) {
	    addFormatToken(token, 0, 0, function () {
	      return this.localeData().meridiem(this.hours(), this.minutes(), lowercase);
	    });
	  }

	  meridiem('a', true);
	  meridiem('A', false); // ALIASES

	  addUnitAlias('hour', 'h'); // PRIORITY

	  addUnitPriority('hour', 13); // PARSING

	  function matchMeridiem(isStrict, locale) {
	    return locale._meridiemParse;
	  }

	  addRegexToken('a', matchMeridiem);
	  addRegexToken('A', matchMeridiem);
	  addRegexToken('H', match1to2);
	  addRegexToken('h', match1to2);
	  addRegexToken('k', match1to2);
	  addRegexToken('HH', match1to2, match2);
	  addRegexToken('hh', match1to2, match2);
	  addRegexToken('kk', match1to2, match2);
	  addRegexToken('hmm', match3to4);
	  addRegexToken('hmmss', match5to6);
	  addRegexToken('Hmm', match3to4);
	  addRegexToken('Hmmss', match5to6);
	  addParseToken(['H', 'HH'], HOUR);
	  addParseToken(['k', 'kk'], function (input, array, config) {
	    var kInput = toInt(input);
	    array[HOUR] = kInput === 24 ? 0 : kInput;
	  });
	  addParseToken(['a', 'A'], function (input, array, config) {
	    config._isPm = config._locale.isPM(input);
	    config._meridiem = input;
	  });
	  addParseToken(['h', 'hh'], function (input, array, config) {
	    array[HOUR] = toInt(input);
	    getParsingFlags(config).bigHour = true;
	  });
	  addParseToken('hmm', function (input, array, config) {
	    var pos = input.length - 2;
	    array[HOUR] = toInt(input.substr(0, pos));
	    array[MINUTE] = toInt(input.substr(pos));
	    getParsingFlags(config).bigHour = true;
	  });
	  addParseToken('hmmss', function (input, array, config) {
	    var pos1 = input.length - 4;
	    var pos2 = input.length - 2;
	    array[HOUR] = toInt(input.substr(0, pos1));
	    array[MINUTE] = toInt(input.substr(pos1, 2));
	    array[SECOND] = toInt(input.substr(pos2));
	    getParsingFlags(config).bigHour = true;
	  });
	  addParseToken('Hmm', function (input, array, config) {
	    var pos = input.length - 2;
	    array[HOUR] = toInt(input.substr(0, pos));
	    array[MINUTE] = toInt(input.substr(pos));
	  });
	  addParseToken('Hmmss', function (input, array, config) {
	    var pos1 = input.length - 4;
	    var pos2 = input.length - 2;
	    array[HOUR] = toInt(input.substr(0, pos1));
	    array[MINUTE] = toInt(input.substr(pos1, 2));
	    array[SECOND] = toInt(input.substr(pos2));
	  }); // LOCALES

	  function localeIsPM(input) {
	    // IE8 Quirks Mode & IE7 Standards Mode do not allow accessing strings like arrays
	    // Using charAt should be more compatible.
	    return (input + '').toLowerCase().charAt(0) === 'p';
	  }

	  var defaultLocaleMeridiemParse = /[ap]\.?m?\.?/i;

	  function localeMeridiem(hours, minutes, isLower) {
	    if (hours > 11) {
	      return isLower ? 'pm' : 'PM';
	    } else {
	      return isLower ? 'am' : 'AM';
	    }
	  } // MOMENTS
	  // Setting the hour should keep the time, because the user explicitly
	  // specified which hour they want. So trying to maintain the same hour (in
	  // a new timezone) makes sense. Adding/subtracting hours does not follow
	  // this rule.


	  var getSetHour = makeGetSet('Hours', true);
	  var baseConfig = {
	    calendar: defaultCalendar,
	    longDateFormat: defaultLongDateFormat,
	    invalidDate: defaultInvalidDate,
	    ordinal: defaultOrdinal,
	    dayOfMonthOrdinalParse: defaultDayOfMonthOrdinalParse,
	    relativeTime: defaultRelativeTime,
	    months: defaultLocaleMonths,
	    monthsShort: defaultLocaleMonthsShort,
	    week: defaultLocaleWeek,
	    weekdays: defaultLocaleWeekdays,
	    weekdaysMin: defaultLocaleWeekdaysMin,
	    weekdaysShort: defaultLocaleWeekdaysShort,
	    meridiemParse: defaultLocaleMeridiemParse
	  }; // internal storage for locale config files

	  var locales = {};
	  var localeFamilies = {};
	  var globalLocale;

	  function normalizeLocale(key) {
	    return key ? key.toLowerCase().replace('_', '-') : key;
	  } // pick the locale from the array
	  // try ['en-au', 'en-gb'] as 'en-au', 'en-gb', 'en', as in move through the list trying each
	  // substring from most specific to least, but move to the next array item if it's a more specific variant than the current root


	  function chooseLocale(names) {
	    var i = 0,
	        j,
	        next,
	        locale,
	        split;

	    while (i < names.length) {
	      split = normalizeLocale(names[i]).split('-');
	      j = split.length;
	      next = normalizeLocale(names[i + 1]);
	      next = next ? next.split('-') : null;

	      while (j > 0) {
	        locale = loadLocale(split.slice(0, j).join('-'));

	        if (locale) {
	          return locale;
	        }

	        if (next && next.length >= j && compareArrays(split, next, true) >= j - 1) {
	          //the next array item is better than a shallower substring of this one
	          break;
	        }

	        j--;
	      }

	      i++;
	    }

	    return globalLocale;
	  }

	  function loadLocale(name) {
	    var oldLocale = null; // TODO: Find a better way to register and load all the locales in Node

	    if (!locales[name] && typeof module !== 'undefined' && module && module.exports) {
	      try {
	        oldLocale = globalLocale._abbr;
	        var aliasedRequire = require;
	        aliasedRequire('./locale/' + name);
	        getSetGlobalLocale(oldLocale);
	      } catch (e) {}
	    }

	    return locales[name];
	  } // This function will load locale and then set the global locale.  If
	  // no arguments are passed in, it will simply return the current global
	  // locale key.


	  function getSetGlobalLocale(key, values) {
	    var data;

	    if (key) {
	      if (isUndefined(values)) {
	        data = getLocale(key);
	      } else {
	        data = defineLocale(key, values);
	      }

	      if (data) {
	        // moment.duration._locale = moment._locale = data;
	        globalLocale = data;
	      } else {
	        if (typeof console !== 'undefined' && console.warn) {
	          //warn user if arguments are passed but the locale could not be set
	          console.warn('Locale ' + key + ' not found. Did you forget to load it?');
	        }
	      }
	    }

	    return globalLocale._abbr;
	  }

	  function defineLocale(name, config) {
	    if (config !== null) {
	      var locale,
	          parentConfig = baseConfig;
	      config.abbr = name;

	      if (locales[name] != null) {
	        deprecateSimple('defineLocaleOverride', 'use moment.updateLocale(localeName, config) to change ' + 'an existing locale. moment.defineLocale(localeName, ' + 'config) should only be used for creating a new locale ' + 'See http://momentjs.com/guides/#/warnings/define-locale/ for more info.');
	        parentConfig = locales[name]._config;
	      } else if (config.parentLocale != null) {
	        if (locales[config.parentLocale] != null) {
	          parentConfig = locales[config.parentLocale]._config;
	        } else {
	          locale = loadLocale(config.parentLocale);

	          if (locale != null) {
	            parentConfig = locale._config;
	          } else {
	            if (!localeFamilies[config.parentLocale]) {
	              localeFamilies[config.parentLocale] = [];
	            }

	            localeFamilies[config.parentLocale].push({
	              name: name,
	              config: config
	            });
	            return null;
	          }
	        }
	      }

	      locales[name] = new Locale(mergeConfigs(parentConfig, config));

	      if (localeFamilies[name]) {
	        localeFamilies[name].forEach(function (x) {
	          defineLocale(x.name, x.config);
	        });
	      } // backwards compat for now: also set the locale
	      // make sure we set the locale AFTER all child locales have been
	      // created, so we won't end up with the child locale set.


	      getSetGlobalLocale(name);
	      return locales[name];
	    } else {
	      // useful for testing
	      delete locales[name];
	      return null;
	    }
	  }

	  function updateLocale(name, config) {
	    if (config != null) {
	      var locale,
	          tmpLocale,
	          parentConfig = baseConfig; // MERGE

	      tmpLocale = loadLocale(name);

	      if (tmpLocale != null) {
	        parentConfig = tmpLocale._config;
	      }

	      config = mergeConfigs(parentConfig, config);
	      locale = new Locale(config);
	      locale.parentLocale = locales[name];
	      locales[name] = locale; // backwards compat for now: also set the locale

	      getSetGlobalLocale(name);
	    } else {
	      // pass null for config to unupdate, useful for tests
	      if (locales[name] != null) {
	        if (locales[name].parentLocale != null) {
	          locales[name] = locales[name].parentLocale;
	        } else if (locales[name] != null) {
	          delete locales[name];
	        }
	      }
	    }

	    return locales[name];
	  } // returns locale data


	  function getLocale(key) {
	    var locale;

	    if (key && key._locale && key._locale._abbr) {
	      key = key._locale._abbr;
	    }

	    if (!key) {
	      return globalLocale;
	    }

	    if (!isArray(key)) {
	      //short-circuit everything else
	      locale = loadLocale(key);

	      if (locale) {
	        return locale;
	      }

	      key = [key];
	    }

	    return chooseLocale(key);
	  }

	  function listLocales() {
	    return keys(locales);
	  }

	  function checkOverflow(m) {
	    var overflow;
	    var a = m._a;

	    if (a && getParsingFlags(m).overflow === -2) {
	      overflow = a[MONTH] < 0 || a[MONTH] > 11 ? MONTH : a[DATE] < 1 || a[DATE] > daysInMonth(a[YEAR], a[MONTH]) ? DATE : a[HOUR] < 0 || a[HOUR] > 24 || a[HOUR] === 24 && (a[MINUTE] !== 0 || a[SECOND] !== 0 || a[MILLISECOND] !== 0) ? HOUR : a[MINUTE] < 0 || a[MINUTE] > 59 ? MINUTE : a[SECOND] < 0 || a[SECOND] > 59 ? SECOND : a[MILLISECOND] < 0 || a[MILLISECOND] > 999 ? MILLISECOND : -1;

	      if (getParsingFlags(m)._overflowDayOfYear && (overflow < YEAR || overflow > DATE)) {
	        overflow = DATE;
	      }

	      if (getParsingFlags(m)._overflowWeeks && overflow === -1) {
	        overflow = WEEK;
	      }

	      if (getParsingFlags(m)._overflowWeekday && overflow === -1) {
	        overflow = WEEKDAY;
	      }

	      getParsingFlags(m).overflow = overflow;
	    }

	    return m;
	  } // Pick the first defined of two or three arguments.


	  function defaults(a, b, c) {
	    if (a != null) {
	      return a;
	    }

	    if (b != null) {
	      return b;
	    }

	    return c;
	  }

	  function currentDateArray(config) {
	    // hooks is actually the exported moment object
	    var nowValue = new Date(hooks.now());

	    if (config._useUTC) {
	      return [nowValue.getUTCFullYear(), nowValue.getUTCMonth(), nowValue.getUTCDate()];
	    }

	    return [nowValue.getFullYear(), nowValue.getMonth(), nowValue.getDate()];
	  } // convert an array to a date.
	  // the array should mirror the parameters below
	  // note: all values past the year are optional and will default to the lowest possible value.
	  // [year, month, day , hour, minute, second, millisecond]


	  function configFromArray(config) {
	    var i,
	        date,
	        input = [],
	        currentDate,
	        expectedWeekday,
	        yearToUse;

	    if (config._d) {
	      return;
	    }

	    currentDate = currentDateArray(config); //compute day of the year from weeks and weekdays

	    if (config._w && config._a[DATE] == null && config._a[MONTH] == null) {
	      dayOfYearFromWeekInfo(config);
	    } //if the day of the year is set, figure out what it is


	    if (config._dayOfYear != null) {
	      yearToUse = defaults(config._a[YEAR], currentDate[YEAR]);

	      if (config._dayOfYear > daysInYear(yearToUse) || config._dayOfYear === 0) {
	        getParsingFlags(config)._overflowDayOfYear = true;
	      }

	      date = createUTCDate(yearToUse, 0, config._dayOfYear);
	      config._a[MONTH] = date.getUTCMonth();
	      config._a[DATE] = date.getUTCDate();
	    } // Default to current date.
	    // * if no year, month, day of month are given, default to today
	    // * if day of month is given, default month and year
	    // * if month is given, default only year
	    // * if year is given, don't default anything


	    for (i = 0; i < 3 && config._a[i] == null; ++i) {
	      config._a[i] = input[i] = currentDate[i];
	    } // Zero out whatever was not defaulted, including time


	    for (; i < 7; i++) {
	      config._a[i] = input[i] = config._a[i] == null ? i === 2 ? 1 : 0 : config._a[i];
	    } // Check for 24:00:00.000


	    if (config._a[HOUR] === 24 && config._a[MINUTE] === 0 && config._a[SECOND] === 0 && config._a[MILLISECOND] === 0) {
	      config._nextDay = true;
	      config._a[HOUR] = 0;
	    }

	    config._d = (config._useUTC ? createUTCDate : createDate).apply(null, input);
	    expectedWeekday = config._useUTC ? config._d.getUTCDay() : config._d.getDay(); // Apply timezone offset from input. The actual utcOffset can be changed
	    // with parseZone.

	    if (config._tzm != null) {
	      config._d.setUTCMinutes(config._d.getUTCMinutes() - config._tzm);
	    }

	    if (config._nextDay) {
	      config._a[HOUR] = 24;
	    } // check for mismatching day of week


	    if (config._w && typeof config._w.d !== 'undefined' && config._w.d !== expectedWeekday) {
	      getParsingFlags(config).weekdayMismatch = true;
	    }
	  }

	  function dayOfYearFromWeekInfo(config) {
	    var w, weekYear, week, weekday, dow, doy, temp, weekdayOverflow;
	    w = config._w;

	    if (w.GG != null || w.W != null || w.E != null) {
	      dow = 1;
	      doy = 4; // TODO: We need to take the current isoWeekYear, but that depends on
	      // how we interpret now (local, utc, fixed offset). So create
	      // a now version of current config (take local/utc/offset flags, and
	      // create now).

	      weekYear = defaults(w.GG, config._a[YEAR], weekOfYear(createLocal(), 1, 4).year);
	      week = defaults(w.W, 1);
	      weekday = defaults(w.E, 1);

	      if (weekday < 1 || weekday > 7) {
	        weekdayOverflow = true;
	      }
	    } else {
	      dow = config._locale._week.dow;
	      doy = config._locale._week.doy;
	      var curWeek = weekOfYear(createLocal(), dow, doy);
	      weekYear = defaults(w.gg, config._a[YEAR], curWeek.year); // Default to current week.

	      week = defaults(w.w, curWeek.week);

	      if (w.d != null) {
	        // weekday -- low day numbers are considered next week
	        weekday = w.d;

	        if (weekday < 0 || weekday > 6) {
	          weekdayOverflow = true;
	        }
	      } else if (w.e != null) {
	        // local weekday -- counting starts from begining of week
	        weekday = w.e + dow;

	        if (w.e < 0 || w.e > 6) {
	          weekdayOverflow = true;
	        }
	      } else {
	        // default to begining of week
	        weekday = dow;
	      }
	    }

	    if (week < 1 || week > weeksInYear(weekYear, dow, doy)) {
	      getParsingFlags(config)._overflowWeeks = true;
	    } else if (weekdayOverflow != null) {
	      getParsingFlags(config)._overflowWeekday = true;
	    } else {
	      temp = dayOfYearFromWeeks(weekYear, week, weekday, dow, doy);
	      config._a[YEAR] = temp.year;
	      config._dayOfYear = temp.dayOfYear;
	    }
	  } // iso 8601 regex
	  // 0000-00-00 0000-W00 or 0000-W00-0 + T + 00 or 00:00 or 00:00:00 or 00:00:00.000 + +00:00 or +0000 or +00)


	  var extendedIsoRegex = /^\s*((?:[+-]\d{6}|\d{4})-(?:\d\d-\d\d|W\d\d-\d|W\d\d|\d\d\d|\d\d))(?:(T| )(\d\d(?::\d\d(?::\d\d(?:[.,]\d+)?)?)?)([\+\-]\d\d(?::?\d\d)?|\s*Z)?)?$/;
	  var basicIsoRegex = /^\s*((?:[+-]\d{6}|\d{4})(?:\d\d\d\d|W\d\d\d|W\d\d|\d\d\d|\d\d))(?:(T| )(\d\d(?:\d\d(?:\d\d(?:[.,]\d+)?)?)?)([\+\-]\d\d(?::?\d\d)?|\s*Z)?)?$/;
	  var tzRegex = /Z|[+-]\d\d(?::?\d\d)?/;
	  var isoDates = [['YYYYYY-MM-DD', /[+-]\d{6}-\d\d-\d\d/], ['YYYY-MM-DD', /\d{4}-\d\d-\d\d/], ['GGGG-[W]WW-E', /\d{4}-W\d\d-\d/], ['GGGG-[W]WW', /\d{4}-W\d\d/, false], ['YYYY-DDD', /\d{4}-\d{3}/], ['YYYY-MM', /\d{4}-\d\d/, false], ['YYYYYYMMDD', /[+-]\d{10}/], ['YYYYMMDD', /\d{8}/], // YYYYMM is NOT allowed by the standard
	  ['GGGG[W]WWE', /\d{4}W\d{3}/], ['GGGG[W]WW', /\d{4}W\d{2}/, false], ['YYYYDDD', /\d{7}/]]; // iso time formats and regexes

	  var isoTimes = [['HH:mm:ss.SSSS', /\d\d:\d\d:\d\d\.\d+/], ['HH:mm:ss,SSSS', /\d\d:\d\d:\d\d,\d+/], ['HH:mm:ss', /\d\d:\d\d:\d\d/], ['HH:mm', /\d\d:\d\d/], ['HHmmss.SSSS', /\d\d\d\d\d\d\.\d+/], ['HHmmss,SSSS', /\d\d\d\d\d\d,\d+/], ['HHmmss', /\d\d\d\d\d\d/], ['HHmm', /\d\d\d\d/], ['HH', /\d\d/]];
	  var aspNetJsonRegex = /^\/?Date\((\-?\d+)/i; // date from iso format

	  function configFromISO(config) {
	    var i,
	        l,
	        string = config._i,
	        match = extendedIsoRegex.exec(string) || basicIsoRegex.exec(string),
	        allowTime,
	        dateFormat,
	        timeFormat,
	        tzFormat;

	    if (match) {
	      getParsingFlags(config).iso = true;

	      for (i = 0, l = isoDates.length; i < l; i++) {
	        if (isoDates[i][1].exec(match[1])) {
	          dateFormat = isoDates[i][0];
	          allowTime = isoDates[i][2] !== false;
	          break;
	        }
	      }

	      if (dateFormat == null) {
	        config._isValid = false;
	        return;
	      }

	      if (match[3]) {
	        for (i = 0, l = isoTimes.length; i < l; i++) {
	          if (isoTimes[i][1].exec(match[3])) {
	            // match[2] should be 'T' or space
	            timeFormat = (match[2] || ' ') + isoTimes[i][0];
	            break;
	          }
	        }

	        if (timeFormat == null) {
	          config._isValid = false;
	          return;
	        }
	      }

	      if (!allowTime && timeFormat != null) {
	        config._isValid = false;
	        return;
	      }

	      if (match[4]) {
	        if (tzRegex.exec(match[4])) {
	          tzFormat = 'Z';
	        } else {
	          config._isValid = false;
	          return;
	        }
	      }

	      config._f = dateFormat + (timeFormat || '') + (tzFormat || '');
	      configFromStringAndFormat(config);
	    } else {
	      config._isValid = false;
	    }
	  } // RFC 2822 regex: For details see https://tools.ietf.org/html/rfc2822#section-3.3


	  var rfc2822 = /^(?:(Mon|Tue|Wed|Thu|Fri|Sat|Sun),?\s)?(\d{1,2})\s(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s(\d{2,4})\s(\d\d):(\d\d)(?::(\d\d))?\s(?:(UT|GMT|[ECMP][SD]T)|([Zz])|([+-]\d{4}))$/;

	  function extractFromRFC2822Strings(yearStr, monthStr, dayStr, hourStr, minuteStr, secondStr) {
	    var result = [untruncateYear(yearStr), defaultLocaleMonthsShort.indexOf(monthStr), parseInt(dayStr, 10), parseInt(hourStr, 10), parseInt(minuteStr, 10)];

	    if (secondStr) {
	      result.push(parseInt(secondStr, 10));
	    }

	    return result;
	  }

	  function untruncateYear(yearStr) {
	    var year = parseInt(yearStr, 10);

	    if (year <= 49) {
	      return 2000 + year;
	    } else if (year <= 999) {
	      return 1900 + year;
	    }

	    return year;
	  }

	  function preprocessRFC2822(s) {
	    // Remove comments and folding whitespace and replace multiple-spaces with a single space
	    return s.replace(/\([^)]*\)|[\n\t]/g, ' ').replace(/(\s\s+)/g, ' ').replace(/^\s\s*/, '').replace(/\s\s*$/, '');
	  }

	  function checkWeekday(weekdayStr, parsedInput, config) {
	    if (weekdayStr) {
	      // TODO: Replace the vanilla JS Date object with an indepentent day-of-week check.
	      var weekdayProvided = defaultLocaleWeekdaysShort.indexOf(weekdayStr),
	          weekdayActual = new Date(parsedInput[0], parsedInput[1], parsedInput[2]).getDay();

	      if (weekdayProvided !== weekdayActual) {
	        getParsingFlags(config).weekdayMismatch = true;
	        config._isValid = false;
	        return false;
	      }
	    }

	    return true;
	  }

	  var obsOffsets = {
	    UT: 0,
	    GMT: 0,
	    EDT: -4 * 60,
	    EST: -5 * 60,
	    CDT: -5 * 60,
	    CST: -6 * 60,
	    MDT: -6 * 60,
	    MST: -7 * 60,
	    PDT: -7 * 60,
	    PST: -8 * 60
	  };

	  function calculateOffset(obsOffset, militaryOffset, numOffset) {
	    if (obsOffset) {
	      return obsOffsets[obsOffset];
	    } else if (militaryOffset) {
	      // the only allowed military tz is Z
	      return 0;
	    } else {
	      var hm = parseInt(numOffset, 10);
	      var m = hm % 100,
	          h = (hm - m) / 100;
	      return h * 60 + m;
	    }
	  } // date and time from ref 2822 format


	  function configFromRFC2822(config) {
	    var match = rfc2822.exec(preprocessRFC2822(config._i));

	    if (match) {
	      var parsedArray = extractFromRFC2822Strings(match[4], match[3], match[2], match[5], match[6], match[7]);

	      if (!checkWeekday(match[1], parsedArray, config)) {
	        return;
	      }

	      config._a = parsedArray;
	      config._tzm = calculateOffset(match[8], match[9], match[10]);
	      config._d = createUTCDate.apply(null, config._a);

	      config._d.setUTCMinutes(config._d.getUTCMinutes() - config._tzm);

	      getParsingFlags(config).rfc2822 = true;
	    } else {
	      config._isValid = false;
	    }
	  } // date from iso format or fallback


	  function configFromString(config) {
	    var matched = aspNetJsonRegex.exec(config._i);

	    if (matched !== null) {
	      config._d = new Date(+matched[1]);
	      return;
	    }

	    configFromISO(config);

	    if (config._isValid === false) {
	      delete config._isValid;
	    } else {
	      return;
	    }

	    configFromRFC2822(config);

	    if (config._isValid === false) {
	      delete config._isValid;
	    } else {
	      return;
	    } // Final attempt, use Input Fallback


	    hooks.createFromInputFallback(config);
	  }

	  hooks.createFromInputFallback = deprecate('value provided is not in a recognized RFC2822 or ISO format. moment construction falls back to js Date(), ' + 'which is not reliable across all browsers and versions. Non RFC2822/ISO date formats are ' + 'discouraged and will be removed in an upcoming major release. Please refer to ' + 'http://momentjs.com/guides/#/warnings/js-date/ for more info.', function (config) {
	    config._d = new Date(config._i + (config._useUTC ? ' UTC' : ''));
	  }); // constant that refers to the ISO standard

	  hooks.ISO_8601 = function () {}; // constant that refers to the RFC 2822 form


	  hooks.RFC_2822 = function () {}; // date from string and format string


	  function configFromStringAndFormat(config) {
	    // TODO: Move this to another part of the creation flow to prevent circular deps
	    if (config._f === hooks.ISO_8601) {
	      configFromISO(config);
	      return;
	    }

	    if (config._f === hooks.RFC_2822) {
	      configFromRFC2822(config);
	      return;
	    }

	    config._a = [];
	    getParsingFlags(config).empty = true; // This array is used to make a Date, either with `new Date` or `Date.UTC`

	    var string = '' + config._i,
	        i,
	        parsedInput,
	        tokens,
	        token,
	        skipped,
	        stringLength = string.length,
	        totalParsedInputLength = 0;
	    tokens = expandFormat(config._f, config._locale).match(formattingTokens) || [];

	    for (i = 0; i < tokens.length; i++) {
	      token = tokens[i];
	      parsedInput = (string.match(getParseRegexForToken(token, config)) || [])[0]; // console.log('token', token, 'parsedInput', parsedInput,
	      //         'regex', getParseRegexForToken(token, config));

	      if (parsedInput) {
	        skipped = string.substr(0, string.indexOf(parsedInput));

	        if (skipped.length > 0) {
	          getParsingFlags(config).unusedInput.push(skipped);
	        }

	        string = string.slice(string.indexOf(parsedInput) + parsedInput.length);
	        totalParsedInputLength += parsedInput.length;
	      } // don't parse if it's not a known token


	      if (formatTokenFunctions[token]) {
	        if (parsedInput) {
	          getParsingFlags(config).empty = false;
	        } else {
	          getParsingFlags(config).unusedTokens.push(token);
	        }

	        addTimeToArrayFromToken(token, parsedInput, config);
	      } else if (config._strict && !parsedInput) {
	        getParsingFlags(config).unusedTokens.push(token);
	      }
	    } // add remaining unparsed input length to the string


	    getParsingFlags(config).charsLeftOver = stringLength - totalParsedInputLength;

	    if (string.length > 0) {
	      getParsingFlags(config).unusedInput.push(string);
	    } // clear _12h flag if hour is <= 12


	    if (config._a[HOUR] <= 12 && getParsingFlags(config).bigHour === true && config._a[HOUR] > 0) {
	      getParsingFlags(config).bigHour = undefined;
	    }

	    getParsingFlags(config).parsedDateParts = config._a.slice(0);
	    getParsingFlags(config).meridiem = config._meridiem; // handle meridiem

	    config._a[HOUR] = meridiemFixWrap(config._locale, config._a[HOUR], config._meridiem);
	    configFromArray(config);
	    checkOverflow(config);
	  }

	  function meridiemFixWrap(locale, hour, meridiem) {
	    var isPm;

	    if (meridiem == null) {
	      // nothing to do
	      return hour;
	    }

	    if (locale.meridiemHour != null) {
	      return locale.meridiemHour(hour, meridiem);
	    } else if (locale.isPM != null) {
	      // Fallback
	      isPm = locale.isPM(meridiem);

	      if (isPm && hour < 12) {
	        hour += 12;
	      }

	      if (!isPm && hour === 12) {
	        hour = 0;
	      }

	      return hour;
	    } else {
	      // this is not supposed to happen
	      return hour;
	    }
	  } // date from string and array of format strings


	  function configFromStringAndArray(config) {
	    var tempConfig, bestMoment, scoreToBeat, i, currentScore;

	    if (config._f.length === 0) {
	      getParsingFlags(config).invalidFormat = true;
	      config._d = new Date(NaN);
	      return;
	    }

	    for (i = 0; i < config._f.length; i++) {
	      currentScore = 0;
	      tempConfig = copyConfig({}, config);

	      if (config._useUTC != null) {
	        tempConfig._useUTC = config._useUTC;
	      }

	      tempConfig._f = config._f[i];
	      configFromStringAndFormat(tempConfig);

	      if (!isValid(tempConfig)) {
	        continue;
	      } // if there is any input that was not parsed add a penalty for that format


	      currentScore += getParsingFlags(tempConfig).charsLeftOver; //or tokens

	      currentScore += getParsingFlags(tempConfig).unusedTokens.length * 10;
	      getParsingFlags(tempConfig).score = currentScore;

	      if (scoreToBeat == null || currentScore < scoreToBeat) {
	        scoreToBeat = currentScore;
	        bestMoment = tempConfig;
	      }
	    }

	    extend(config, bestMoment || tempConfig);
	  }

	  function configFromObject(config) {
	    if (config._d) {
	      return;
	    }

	    var i = normalizeObjectUnits(config._i);
	    config._a = map([i.year, i.month, i.day || i.date, i.hour, i.minute, i.second, i.millisecond], function (obj) {
	      return obj && parseInt(obj, 10);
	    });
	    configFromArray(config);
	  }

	  function createFromConfig(config) {
	    var res = new Moment(checkOverflow(prepareConfig(config)));

	    if (res._nextDay) {
	      // Adding is smart enough around DST
	      res.add(1, 'd');
	      res._nextDay = undefined;
	    }

	    return res;
	  }

	  function prepareConfig(config) {
	    var input = config._i,
	        format = config._f;
	    config._locale = config._locale || getLocale(config._l);

	    if (input === null || format === undefined && input === '') {
	      return createInvalid({
	        nullInput: true
	      });
	    }

	    if (typeof input === 'string') {
	      config._i = input = config._locale.preparse(input);
	    }

	    if (isMoment(input)) {
	      return new Moment(checkOverflow(input));
	    } else if (isDate(input)) {
	      config._d = input;
	    } else if (isArray(format)) {
	      configFromStringAndArray(config);
	    } else if (format) {
	      configFromStringAndFormat(config);
	    } else {
	      configFromInput(config);
	    }

	    if (!isValid(config)) {
	      config._d = null;
	    }

	    return config;
	  }

	  function configFromInput(config) {
	    var input = config._i;

	    if (isUndefined(input)) {
	      config._d = new Date(hooks.now());
	    } else if (isDate(input)) {
	      config._d = new Date(input.valueOf());
	    } else if (typeof input === 'string') {
	      configFromString(config);
	    } else if (isArray(input)) {
	      config._a = map(input.slice(0), function (obj) {
	        return parseInt(obj, 10);
	      });
	      configFromArray(config);
	    } else if (isObject(input)) {
	      configFromObject(config);
	    } else if (isNumber(input)) {
	      // from milliseconds
	      config._d = new Date(input);
	    } else {
	      hooks.createFromInputFallback(config);
	    }
	  }

	  function createLocalOrUTC(input, format, locale, strict, isUTC) {
	    var c = {};

	    if (locale === true || locale === false) {
	      strict = locale;
	      locale = undefined;
	    }

	    if (isObject(input) && isObjectEmpty(input) || isArray(input) && input.length === 0) {
	      input = undefined;
	    } // object construction must be done this way.
	    // https://github.com/moment/moment/issues/1423


	    c._isAMomentObject = true;
	    c._useUTC = c._isUTC = isUTC;
	    c._l = locale;
	    c._i = input;
	    c._f = format;
	    c._strict = strict;
	    return createFromConfig(c);
	  }

	  function createLocal(input, format, locale, strict) {
	    return createLocalOrUTC(input, format, locale, strict, false);
	  }

	  var prototypeMin = deprecate('moment().min is deprecated, use moment.max instead. http://momentjs.com/guides/#/warnings/min-max/', function () {
	    var other = createLocal.apply(null, arguments);

	    if (this.isValid() && other.isValid()) {
	      return other < this ? this : other;
	    } else {
	      return createInvalid();
	    }
	  });
	  var prototypeMax = deprecate('moment().max is deprecated, use moment.min instead. http://momentjs.com/guides/#/warnings/min-max/', function () {
	    var other = createLocal.apply(null, arguments);

	    if (this.isValid() && other.isValid()) {
	      return other > this ? this : other;
	    } else {
	      return createInvalid();
	    }
	  }); // Pick a moment m from moments so that m[fn](other) is true for all
	  // other. This relies on the function fn to be transitive.
	  //
	  // moments should either be an array of moment objects or an array, whose
	  // first element is an array of moment objects.

	  function pickBy(fn, moments) {
	    var res, i;

	    if (moments.length === 1 && isArray(moments[0])) {
	      moments = moments[0];
	    }

	    if (!moments.length) {
	      return createLocal();
	    }

	    res = moments[0];

	    for (i = 1; i < moments.length; ++i) {
	      if (!moments[i].isValid() || moments[i][fn](res)) {
	        res = moments[i];
	      }
	    }

	    return res;
	  } // TODO: Use [].sort instead?


	  function min() {
	    var args = [].slice.call(arguments, 0);
	    return pickBy('isBefore', args);
	  }

	  function max() {
	    var args = [].slice.call(arguments, 0);
	    return pickBy('isAfter', args);
	  }

	  var now = function now() {
	    return Date.now ? Date.now() : +new Date();
	  };

	  var ordering = ['year', 'quarter', 'month', 'week', 'day', 'hour', 'minute', 'second', 'millisecond'];

	  function isDurationValid(m) {
	    for (var key in m) {
	      if (!(indexOf.call(ordering, key) !== -1 && (m[key] == null || !isNaN(m[key])))) {
	        return false;
	      }
	    }

	    var unitHasDecimal = false;

	    for (var i = 0; i < ordering.length; ++i) {
	      if (m[ordering[i]]) {
	        if (unitHasDecimal) {
	          return false; // only allow non-integers for smallest unit
	        }

	        if (parseFloat(m[ordering[i]]) !== toInt(m[ordering[i]])) {
	          unitHasDecimal = true;
	        }
	      }
	    }

	    return true;
	  }

	  function isValid$1() {
	    return this._isValid;
	  }

	  function createInvalid$1() {
	    return createDuration(NaN);
	  }

	  function Duration(duration) {
	    var normalizedInput = normalizeObjectUnits(duration),
	        years = normalizedInput.year || 0,
	        quarters = normalizedInput.quarter || 0,
	        months = normalizedInput.month || 0,
	        weeks = normalizedInput.week || 0,
	        days = normalizedInput.day || 0,
	        hours = normalizedInput.hour || 0,
	        minutes = normalizedInput.minute || 0,
	        seconds = normalizedInput.second || 0,
	        milliseconds = normalizedInput.millisecond || 0;
	    this._isValid = isDurationValid(normalizedInput); // representation for dateAddRemove

	    this._milliseconds = +milliseconds + seconds * 1e3 + // 1000
	    minutes * 6e4 + // 1000 * 60
	    hours * 1000 * 60 * 60; //using 1000 * 60 * 60 instead of 36e5 to avoid floating point rounding errors https://github.com/moment/moment/issues/2978
	    // Because of dateAddRemove treats 24 hours as different from a
	    // day when working around DST, we need to store them separately

	    this._days = +days + weeks * 7; // It is impossible to translate months into days without knowing
	    // which months you are are talking about, so we have to store
	    // it separately.

	    this._months = +months + quarters * 3 + years * 12;
	    this._data = {};
	    this._locale = getLocale();

	    this._bubble();
	  }

	  function isDuration(obj) {
	    return obj instanceof Duration;
	  }

	  function absRound(number) {
	    if (number < 0) {
	      return Math.round(-1 * number) * -1;
	    } else {
	      return Math.round(number);
	    }
	  } // FORMATTING


	  function offset(token, separator) {
	    addFormatToken(token, 0, 0, function () {
	      var offset = this.utcOffset();
	      var sign = '+';

	      if (offset < 0) {
	        offset = -offset;
	        sign = '-';
	      }

	      return sign + zeroFill(~~(offset / 60), 2) + separator + zeroFill(~~offset % 60, 2);
	    });
	  }

	  offset('Z', ':');
	  offset('ZZ', ''); // PARSING

	  addRegexToken('Z', matchShortOffset);
	  addRegexToken('ZZ', matchShortOffset);
	  addParseToken(['Z', 'ZZ'], function (input, array, config) {
	    config._useUTC = true;
	    config._tzm = offsetFromString(matchShortOffset, input);
	  }); // HELPERS
	  // timezone chunker
	  // '+10:00' > ['10',  '00']
	  // '-1530'  > ['-15', '30']

	  var chunkOffset = /([\+\-]|\d\d)/gi;

	  function offsetFromString(matcher, string) {
	    var matches = (string || '').match(matcher);

	    if (matches === null) {
	      return null;
	    }

	    var chunk = matches[matches.length - 1] || [];
	    var parts = (chunk + '').match(chunkOffset) || ['-', 0, 0];
	    var minutes = +(parts[1] * 60) + toInt(parts[2]);
	    return minutes === 0 ? 0 : parts[0] === '+' ? minutes : -minutes;
	  } // Return a moment from input, that is local/utc/zone equivalent to model.


	  function cloneWithOffset(input, model) {
	    var res, diff;

	    if (model._isUTC) {
	      res = model.clone();
	      diff = (isMoment(input) || isDate(input) ? input.valueOf() : createLocal(input).valueOf()) - res.valueOf(); // Use low-level api, because this fn is low-level api.

	      res._d.setTime(res._d.valueOf() + diff);

	      hooks.updateOffset(res, false);
	      return res;
	    } else {
	      return createLocal(input).local();
	    }
	  }

	  function getDateOffset(m) {
	    // On Firefox.24 Date#getTimezoneOffset returns a floating point.
	    // https://github.com/moment/moment/pull/1871
	    return -Math.round(m._d.getTimezoneOffset() / 15) * 15;
	  } // HOOKS
	  // This function will be called whenever a moment is mutated.
	  // It is intended to keep the offset in sync with the timezone.


	  hooks.updateOffset = function () {}; // MOMENTS
	  // keepLocalTime = true means only change the timezone, without
	  // affecting the local hour. So 5:31:26 +0300 --[utcOffset(2, true)]-->
	  // 5:31:26 +0200 It is possible that 5:31:26 doesn't exist with offset
	  // +0200, so we adjust the time as needed, to be valid.
	  //
	  // Keeping the time actually adds/subtracts (one hour)
	  // from the actual represented time. That is why we call updateOffset
	  // a second time. In case it wants us to change the offset again
	  // _changeInProgress == true case, then we have to adjust, because
	  // there is no such time in the given timezone.


	  function getSetOffset(input, keepLocalTime, keepMinutes) {
	    var offset = this._offset || 0,
	        localAdjust;

	    if (!this.isValid()) {
	      return input != null ? this : NaN;
	    }

	    if (input != null) {
	      if (typeof input === 'string') {
	        input = offsetFromString(matchShortOffset, input);

	        if (input === null) {
	          return this;
	        }
	      } else if (Math.abs(input) < 16 && !keepMinutes) {
	        input = input * 60;
	      }

	      if (!this._isUTC && keepLocalTime) {
	        localAdjust = getDateOffset(this);
	      }

	      this._offset = input;
	      this._isUTC = true;

	      if (localAdjust != null) {
	        this.add(localAdjust, 'm');
	      }

	      if (offset !== input) {
	        if (!keepLocalTime || this._changeInProgress) {
	          addSubtract(this, createDuration(input - offset, 'm'), 1, false);
	        } else if (!this._changeInProgress) {
	          this._changeInProgress = true;
	          hooks.updateOffset(this, true);
	          this._changeInProgress = null;
	        }
	      }

	      return this;
	    } else {
	      return this._isUTC ? offset : getDateOffset(this);
	    }
	  }

	  function getSetZone(input, keepLocalTime) {
	    if (input != null) {
	      if (typeof input !== 'string') {
	        input = -input;
	      }

	      this.utcOffset(input, keepLocalTime);
	      return this;
	    } else {
	      return -this.utcOffset();
	    }
	  }

	  function setOffsetToUTC(keepLocalTime) {
	    return this.utcOffset(0, keepLocalTime);
	  }

	  function setOffsetToLocal(keepLocalTime) {
	    if (this._isUTC) {
	      this.utcOffset(0, keepLocalTime);
	      this._isUTC = false;

	      if (keepLocalTime) {
	        this.subtract(getDateOffset(this), 'm');
	      }
	    }

	    return this;
	  }

	  function setOffsetToParsedOffset() {
	    if (this._tzm != null) {
	      this.utcOffset(this._tzm, false, true);
	    } else if (typeof this._i === 'string') {
	      var tZone = offsetFromString(matchOffset, this._i);

	      if (tZone != null) {
	        this.utcOffset(tZone);
	      } else {
	        this.utcOffset(0, true);
	      }
	    }

	    return this;
	  }

	  function hasAlignedHourOffset(input) {
	    if (!this.isValid()) {
	      return false;
	    }

	    input = input ? createLocal(input).utcOffset() : 0;
	    return (this.utcOffset() - input) % 60 === 0;
	  }

	  function isDaylightSavingTime() {
	    return this.utcOffset() > this.clone().month(0).utcOffset() || this.utcOffset() > this.clone().month(5).utcOffset();
	  }

	  function isDaylightSavingTimeShifted() {
	    if (!isUndefined(this._isDSTShifted)) {
	      return this._isDSTShifted;
	    }

	    var c = {};
	    copyConfig(c, this);
	    c = prepareConfig(c);

	    if (c._a) {
	      var other = c._isUTC ? createUTC(c._a) : createLocal(c._a);
	      this._isDSTShifted = this.isValid() && compareArrays(c._a, other.toArray()) > 0;
	    } else {
	      this._isDSTShifted = false;
	    }

	    return this._isDSTShifted;
	  }

	  function isLocal() {
	    return this.isValid() ? !this._isUTC : false;
	  }

	  function isUtcOffset() {
	    return this.isValid() ? this._isUTC : false;
	  }

	  function isUtc() {
	    return this.isValid() ? this._isUTC && this._offset === 0 : false;
	  } // ASP.NET json date format regex


	  var aspNetRegex = /^(\-|\+)?(?:(\d*)[. ])?(\d+)\:(\d+)(?:\:(\d+)(\.\d*)?)?$/; // from http://docs.closure-library.googlecode.com/git/closure_goog_date_date.js.source.html
	  // somewhat more in line with 4.4.3.2 2004 spec, but allows decimal anywhere
	  // and further modified to allow for strings containing both week and day

	  var isoRegex = /^(-|\+)?P(?:([-+]?[0-9,.]*)Y)?(?:([-+]?[0-9,.]*)M)?(?:([-+]?[0-9,.]*)W)?(?:([-+]?[0-9,.]*)D)?(?:T(?:([-+]?[0-9,.]*)H)?(?:([-+]?[0-9,.]*)M)?(?:([-+]?[0-9,.]*)S)?)?$/;

	  function createDuration(input, key) {
	    var duration = input,
	        // matching against regexp is expensive, do it on demand
	    match = null,
	        sign,
	        ret,
	        diffRes;

	    if (isDuration(input)) {
	      duration = {
	        ms: input._milliseconds,
	        d: input._days,
	        M: input._months
	      };
	    } else if (isNumber(input)) {
	      duration = {};

	      if (key) {
	        duration[key] = input;
	      } else {
	        duration.milliseconds = input;
	      }
	    } else if (!!(match = aspNetRegex.exec(input))) {
	      sign = match[1] === '-' ? -1 : 1;
	      duration = {
	        y: 0,
	        d: toInt(match[DATE]) * sign,
	        h: toInt(match[HOUR]) * sign,
	        m: toInt(match[MINUTE]) * sign,
	        s: toInt(match[SECOND]) * sign,
	        ms: toInt(absRound(match[MILLISECOND] * 1000)) * sign // the millisecond decimal point is included in the match

	      };
	    } else if (!!(match = isoRegex.exec(input))) {
	      sign = match[1] === '-' ? -1 : match[1] === '+' ? 1 : 1;
	      duration = {
	        y: parseIso(match[2], sign),
	        M: parseIso(match[3], sign),
	        w: parseIso(match[4], sign),
	        d: parseIso(match[5], sign),
	        h: parseIso(match[6], sign),
	        m: parseIso(match[7], sign),
	        s: parseIso(match[8], sign)
	      };
	    } else if (duration == null) {
	      // checks for null or undefined
	      duration = {};
	    } else if (_typeof(duration) === 'object' && ('from' in duration || 'to' in duration)) {
	      diffRes = momentsDifference(createLocal(duration.from), createLocal(duration.to));
	      duration = {};
	      duration.ms = diffRes.milliseconds;
	      duration.M = diffRes.months;
	    }

	    ret = new Duration(duration);

	    if (isDuration(input) && hasOwnProp(input, '_locale')) {
	      ret._locale = input._locale;
	    }

	    return ret;
	  }

	  createDuration.fn = Duration.prototype;
	  createDuration.invalid = createInvalid$1;

	  function parseIso(inp, sign) {
	    // We'd normally use ~~inp for this, but unfortunately it also
	    // converts floats to ints.
	    // inp may be undefined, so careful calling replace on it.
	    var res = inp && parseFloat(inp.replace(',', '.')); // apply sign while we're at it

	    return (isNaN(res) ? 0 : res) * sign;
	  }

	  function positiveMomentsDifference(base, other) {
	    var res = {
	      milliseconds: 0,
	      months: 0
	    };
	    res.months = other.month() - base.month() + (other.year() - base.year()) * 12;

	    if (base.clone().add(res.months, 'M').isAfter(other)) {
	      --res.months;
	    }

	    res.milliseconds = +other - +base.clone().add(res.months, 'M');
	    return res;
	  }

	  function momentsDifference(base, other) {
	    var res;

	    if (!(base.isValid() && other.isValid())) {
	      return {
	        milliseconds: 0,
	        months: 0
	      };
	    }

	    other = cloneWithOffset(other, base);

	    if (base.isBefore(other)) {
	      res = positiveMomentsDifference(base, other);
	    } else {
	      res = positiveMomentsDifference(other, base);
	      res.milliseconds = -res.milliseconds;
	      res.months = -res.months;
	    }

	    return res;
	  } // TODO: remove 'name' arg after deprecation is removed


	  function createAdder(direction, name) {
	    return function (val, period) {
	      var dur, tmp; //invert the arguments, but complain about it

	      if (period !== null && !isNaN(+period)) {
	        deprecateSimple(name, 'moment().' + name + '(period, number) is deprecated. Please use moment().' + name + '(number, period). ' + 'See http://momentjs.com/guides/#/warnings/add-inverted-param/ for more info.');
	        tmp = val;
	        val = period;
	        period = tmp;
	      }

	      val = typeof val === 'string' ? +val : val;
	      dur = createDuration(val, period);
	      addSubtract(this, dur, direction);
	      return this;
	    };
	  }

	  function addSubtract(mom, duration, isAdding, updateOffset) {
	    var milliseconds = duration._milliseconds,
	        days = absRound(duration._days),
	        months = absRound(duration._months);

	    if (!mom.isValid()) {
	      // No op
	      return;
	    }

	    updateOffset = updateOffset == null ? true : updateOffset;

	    if (months) {
	      setMonth(mom, get(mom, 'Month') + months * isAdding);
	    }

	    if (days) {
	      set$1(mom, 'Date', get(mom, 'Date') + days * isAdding);
	    }

	    if (milliseconds) {
	      mom._d.setTime(mom._d.valueOf() + milliseconds * isAdding);
	    }

	    if (updateOffset) {
	      hooks.updateOffset(mom, days || months);
	    }
	  }

	  var add = createAdder(1, 'add');
	  var subtract = createAdder(-1, 'subtract');

	  function getCalendarFormat(myMoment, now) {
	    var diff = myMoment.diff(now, 'days', true);
	    return diff < -6 ? 'sameElse' : diff < -1 ? 'lastWeek' : diff < 0 ? 'lastDay' : diff < 1 ? 'sameDay' : diff < 2 ? 'nextDay' : diff < 7 ? 'nextWeek' : 'sameElse';
	  }

	  function calendar$1(time, formats) {
	    // We want to compare the start of today, vs this.
	    // Getting start-of-today depends on whether we're local/utc/offset or not.
	    var now = time || createLocal(),
	        sod = cloneWithOffset(now, this).startOf('day'),
	        format = hooks.calendarFormat(this, sod) || 'sameElse';
	    var output = formats && (isFunction(formats[format]) ? formats[format].call(this, now) : formats[format]);
	    return this.format(output || this.localeData().calendar(format, this, createLocal(now)));
	  }

	  function clone() {
	    return new Moment(this);
	  }

	  function isAfter(input, units) {
	    var localInput = isMoment(input) ? input : createLocal(input);

	    if (!(this.isValid() && localInput.isValid())) {
	      return false;
	    }

	    units = normalizeUnits(!isUndefined(units) ? units : 'millisecond');

	    if (units === 'millisecond') {
	      return this.valueOf() > localInput.valueOf();
	    } else {
	      return localInput.valueOf() < this.clone().startOf(units).valueOf();
	    }
	  }

	  function isBefore(input, units) {
	    var localInput = isMoment(input) ? input : createLocal(input);

	    if (!(this.isValid() && localInput.isValid())) {
	      return false;
	    }

	    units = normalizeUnits(!isUndefined(units) ? units : 'millisecond');

	    if (units === 'millisecond') {
	      return this.valueOf() < localInput.valueOf();
	    } else {
	      return this.clone().endOf(units).valueOf() < localInput.valueOf();
	    }
	  }

	  function isBetween(from, to, units, inclusivity) {
	    inclusivity = inclusivity || '()';
	    return (inclusivity[0] === '(' ? this.isAfter(from, units) : !this.isBefore(from, units)) && (inclusivity[1] === ')' ? this.isBefore(to, units) : !this.isAfter(to, units));
	  }

	  function isSame(input, units) {
	    var localInput = isMoment(input) ? input : createLocal(input),
	        inputMs;

	    if (!(this.isValid() && localInput.isValid())) {
	      return false;
	    }

	    units = normalizeUnits(units || 'millisecond');

	    if (units === 'millisecond') {
	      return this.valueOf() === localInput.valueOf();
	    } else {
	      inputMs = localInput.valueOf();
	      return this.clone().startOf(units).valueOf() <= inputMs && inputMs <= this.clone().endOf(units).valueOf();
	    }
	  }

	  function isSameOrAfter(input, units) {
	    return this.isSame(input, units) || this.isAfter(input, units);
	  }

	  function isSameOrBefore(input, units) {
	    return this.isSame(input, units) || this.isBefore(input, units);
	  }

	  function diff(input, units, asFloat) {
	    var that, zoneDelta, output;

	    if (!this.isValid()) {
	      return NaN;
	    }

	    that = cloneWithOffset(input, this);

	    if (!that.isValid()) {
	      return NaN;
	    }

	    zoneDelta = (that.utcOffset() - this.utcOffset()) * 6e4;
	    units = normalizeUnits(units);

	    switch (units) {
	      case 'year':
	        output = monthDiff(this, that) / 12;
	        break;

	      case 'month':
	        output = monthDiff(this, that);
	        break;

	      case 'quarter':
	        output = monthDiff(this, that) / 3;
	        break;

	      case 'second':
	        output = (this - that) / 1e3;
	        break;
	      // 1000

	      case 'minute':
	        output = (this - that) / 6e4;
	        break;
	      // 1000 * 60

	      case 'hour':
	        output = (this - that) / 36e5;
	        break;
	      // 1000 * 60 * 60

	      case 'day':
	        output = (this - that - zoneDelta) / 864e5;
	        break;
	      // 1000 * 60 * 60 * 24, negate dst

	      case 'week':
	        output = (this - that - zoneDelta) / 6048e5;
	        break;
	      // 1000 * 60 * 60 * 24 * 7, negate dst

	      default:
	        output = this - that;
	    }

	    return asFloat ? output : absFloor(output);
	  }

	  function monthDiff(a, b) {
	    // difference in months
	    var wholeMonthDiff = (b.year() - a.year()) * 12 + (b.month() - a.month()),
	        // b is in (anchor - 1 month, anchor + 1 month)
	    anchor = a.clone().add(wholeMonthDiff, 'months'),
	        anchor2,
	        adjust;

	    if (b - anchor < 0) {
	      anchor2 = a.clone().add(wholeMonthDiff - 1, 'months'); // linear across the month

	      adjust = (b - anchor) / (anchor - anchor2);
	    } else {
	      anchor2 = a.clone().add(wholeMonthDiff + 1, 'months'); // linear across the month

	      adjust = (b - anchor) / (anchor2 - anchor);
	    } //check for negative zero, return zero if negative zero


	    return -(wholeMonthDiff + adjust) || 0;
	  }

	  hooks.defaultFormat = 'YYYY-MM-DDTHH:mm:ssZ';
	  hooks.defaultFormatUtc = 'YYYY-MM-DDTHH:mm:ss[Z]';

	  function toString() {
	    return this.clone().locale('en').format('ddd MMM DD YYYY HH:mm:ss [GMT]ZZ');
	  }

	  function toISOString(keepOffset) {
	    if (!this.isValid()) {
	      return null;
	    }

	    var utc = keepOffset !== true;
	    var m = utc ? this.clone().utc() : this;

	    if (m.year() < 0 || m.year() > 9999) {
	      return formatMoment(m, utc ? 'YYYYYY-MM-DD[T]HH:mm:ss.SSS[Z]' : 'YYYYYY-MM-DD[T]HH:mm:ss.SSSZ');
	    }

	    if (isFunction(Date.prototype.toISOString)) {
	      // native implementation is ~50x faster, use it when we can
	      if (utc) {
	        return this.toDate().toISOString();
	      } else {
	        return new Date(this.valueOf() + this.utcOffset() * 60 * 1000).toISOString().replace('Z', formatMoment(m, 'Z'));
	      }
	    }

	    return formatMoment(m, utc ? 'YYYY-MM-DD[T]HH:mm:ss.SSS[Z]' : 'YYYY-MM-DD[T]HH:mm:ss.SSSZ');
	  }
	  /**
	   * Return a human readable representation of a moment that can
	   * also be evaluated to get a new moment which is the same
	   *
	   * @link https://nodejs.org/dist/latest/docs/api/util.html#util_custom_inspect_function_on_objects
	   */


	  function inspect() {
	    if (!this.isValid()) {
	      return 'moment.invalid(/* ' + this._i + ' */)';
	    }

	    var func = 'moment';
	    var zone = '';

	    if (!this.isLocal()) {
	      func = this.utcOffset() === 0 ? 'moment.utc' : 'moment.parseZone';
	      zone = 'Z';
	    }

	    var prefix = '[' + func + '("]';
	    var year = 0 <= this.year() && this.year() <= 9999 ? 'YYYY' : 'YYYYYY';
	    var datetime = '-MM-DD[T]HH:mm:ss.SSS';
	    var suffix = zone + '[")]';
	    return this.format(prefix + year + datetime + suffix);
	  }

	  function format(inputString) {
	    if (!inputString) {
	      inputString = this.isUtc() ? hooks.defaultFormatUtc : hooks.defaultFormat;
	    }

	    var output = formatMoment(this, inputString);
	    return this.localeData().postformat(output);
	  }

	  function from(time, withoutSuffix) {
	    if (this.isValid() && (isMoment(time) && time.isValid() || createLocal(time).isValid())) {
	      return createDuration({
	        to: this,
	        from: time
	      }).locale(this.locale()).humanize(!withoutSuffix);
	    } else {
	      return this.localeData().invalidDate();
	    }
	  }

	  function fromNow(withoutSuffix) {
	    return this.from(createLocal(), withoutSuffix);
	  }

	  function to(time, withoutSuffix) {
	    if (this.isValid() && (isMoment(time) && time.isValid() || createLocal(time).isValid())) {
	      return createDuration({
	        from: this,
	        to: time
	      }).locale(this.locale()).humanize(!withoutSuffix);
	    } else {
	      return this.localeData().invalidDate();
	    }
	  }

	  function toNow(withoutSuffix) {
	    return this.to(createLocal(), withoutSuffix);
	  } // If passed a locale key, it will set the locale for this
	  // instance.  Otherwise, it will return the locale configuration
	  // variables for this instance.


	  function locale(key) {
	    var newLocaleData;

	    if (key === undefined) {
	      return this._locale._abbr;
	    } else {
	      newLocaleData = getLocale(key);

	      if (newLocaleData != null) {
	        this._locale = newLocaleData;
	      }

	      return this;
	    }
	  }

	  var lang = deprecate('moment().lang() is deprecated. Instead, use moment().localeData() to get the language configuration. Use moment().locale() to change languages.', function (key) {
	    if (key === undefined) {
	      return this.localeData();
	    } else {
	      return this.locale(key);
	    }
	  });

	  function localeData() {
	    return this._locale;
	  }

	  function startOf(units) {
	    units = normalizeUnits(units); // the following switch intentionally omits break keywords
	    // to utilize falling through the cases.

	    switch (units) {
	      case 'year':
	        this.month(0);

	      /* falls through */

	      case 'quarter':
	      case 'month':
	        this.date(1);

	      /* falls through */

	      case 'week':
	      case 'isoWeek':
	      case 'day':
	      case 'date':
	        this.hours(0);

	      /* falls through */

	      case 'hour':
	        this.minutes(0);

	      /* falls through */

	      case 'minute':
	        this.seconds(0);

	      /* falls through */

	      case 'second':
	        this.milliseconds(0);
	    } // weeks are a special case


	    if (units === 'week') {
	      this.weekday(0);
	    }

	    if (units === 'isoWeek') {
	      this.isoWeekday(1);
	    } // quarters are also special


	    if (units === 'quarter') {
	      this.month(Math.floor(this.month() / 3) * 3);
	    }

	    return this;
	  }

	  function endOf(units) {
	    units = normalizeUnits(units);

	    if (units === undefined || units === 'millisecond') {
	      return this;
	    } // 'date' is an alias for 'day', so it should be considered as such.


	    if (units === 'date') {
	      units = 'day';
	    }

	    return this.startOf(units).add(1, units === 'isoWeek' ? 'week' : units).subtract(1, 'ms');
	  }

	  function valueOf() {
	    return this._d.valueOf() - (this._offset || 0) * 60000;
	  }

	  function unix() {
	    return Math.floor(this.valueOf() / 1000);
	  }

	  function toDate() {
	    return new Date(this.valueOf());
	  }

	  function toArray() {
	    var m = this;
	    return [m.year(), m.month(), m.date(), m.hour(), m.minute(), m.second(), m.millisecond()];
	  }

	  function toObject() {
	    var m = this;
	    return {
	      years: m.year(),
	      months: m.month(),
	      date: m.date(),
	      hours: m.hours(),
	      minutes: m.minutes(),
	      seconds: m.seconds(),
	      milliseconds: m.milliseconds()
	    };
	  }

	  function toJSON() {
	    // new Date(NaN).toJSON() === null
	    return this.isValid() ? this.toISOString() : null;
	  }

	  function isValid$2() {
	    return isValid(this);
	  }

	  function parsingFlags() {
	    return extend({}, getParsingFlags(this));
	  }

	  function invalidAt() {
	    return getParsingFlags(this).overflow;
	  }

	  function creationData() {
	    return {
	      input: this._i,
	      format: this._f,
	      locale: this._locale,
	      isUTC: this._isUTC,
	      strict: this._strict
	    };
	  } // FORMATTING


	  addFormatToken(0, ['gg', 2], 0, function () {
	    return this.weekYear() % 100;
	  });
	  addFormatToken(0, ['GG', 2], 0, function () {
	    return this.isoWeekYear() % 100;
	  });

	  function addWeekYearFormatToken(token, getter) {
	    addFormatToken(0, [token, token.length], 0, getter);
	  }

	  addWeekYearFormatToken('gggg', 'weekYear');
	  addWeekYearFormatToken('ggggg', 'weekYear');
	  addWeekYearFormatToken('GGGG', 'isoWeekYear');
	  addWeekYearFormatToken('GGGGG', 'isoWeekYear'); // ALIASES

	  addUnitAlias('weekYear', 'gg');
	  addUnitAlias('isoWeekYear', 'GG'); // PRIORITY

	  addUnitPriority('weekYear', 1);
	  addUnitPriority('isoWeekYear', 1); // PARSING

	  addRegexToken('G', matchSigned);
	  addRegexToken('g', matchSigned);
	  addRegexToken('GG', match1to2, match2);
	  addRegexToken('gg', match1to2, match2);
	  addRegexToken('GGGG', match1to4, match4);
	  addRegexToken('gggg', match1to4, match4);
	  addRegexToken('GGGGG', match1to6, match6);
	  addRegexToken('ggggg', match1to6, match6);
	  addWeekParseToken(['gggg', 'ggggg', 'GGGG', 'GGGGG'], function (input, week, config, token) {
	    week[token.substr(0, 2)] = toInt(input);
	  });
	  addWeekParseToken(['gg', 'GG'], function (input, week, config, token) {
	    week[token] = hooks.parseTwoDigitYear(input);
	  }); // MOMENTS

	  function getSetWeekYear(input) {
	    return getSetWeekYearHelper.call(this, input, this.week(), this.weekday(), this.localeData()._week.dow, this.localeData()._week.doy);
	  }

	  function getSetISOWeekYear(input) {
	    return getSetWeekYearHelper.call(this, input, this.isoWeek(), this.isoWeekday(), 1, 4);
	  }

	  function getISOWeeksInYear() {
	    return weeksInYear(this.year(), 1, 4);
	  }

	  function getWeeksInYear() {
	    var weekInfo = this.localeData()._week;

	    return weeksInYear(this.year(), weekInfo.dow, weekInfo.doy);
	  }

	  function getSetWeekYearHelper(input, week, weekday, dow, doy) {
	    var weeksTarget;

	    if (input == null) {
	      return weekOfYear(this, dow, doy).year;
	    } else {
	      weeksTarget = weeksInYear(input, dow, doy);

	      if (week > weeksTarget) {
	        week = weeksTarget;
	      }

	      return setWeekAll.call(this, input, week, weekday, dow, doy);
	    }
	  }

	  function setWeekAll(weekYear, week, weekday, dow, doy) {
	    var dayOfYearData = dayOfYearFromWeeks(weekYear, week, weekday, dow, doy),
	        date = createUTCDate(dayOfYearData.year, 0, dayOfYearData.dayOfYear);
	    this.year(date.getUTCFullYear());
	    this.month(date.getUTCMonth());
	    this.date(date.getUTCDate());
	    return this;
	  } // FORMATTING


	  addFormatToken('Q', 0, 'Qo', 'quarter'); // ALIASES

	  addUnitAlias('quarter', 'Q'); // PRIORITY

	  addUnitPriority('quarter', 7); // PARSING

	  addRegexToken('Q', match1);
	  addParseToken('Q', function (input, array) {
	    array[MONTH] = (toInt(input) - 1) * 3;
	  }); // MOMENTS

	  function getSetQuarter(input) {
	    return input == null ? Math.ceil((this.month() + 1) / 3) : this.month((input - 1) * 3 + this.month() % 3);
	  } // FORMATTING


	  addFormatToken('D', ['DD', 2], 'Do', 'date'); // ALIASES

	  addUnitAlias('date', 'D'); // PRIORITY

	  addUnitPriority('date', 9); // PARSING

	  addRegexToken('D', match1to2);
	  addRegexToken('DD', match1to2, match2);
	  addRegexToken('Do', function (isStrict, locale) {
	    // TODO: Remove "ordinalParse" fallback in next major release.
	    return isStrict ? locale._dayOfMonthOrdinalParse || locale._ordinalParse : locale._dayOfMonthOrdinalParseLenient;
	  });
	  addParseToken(['D', 'DD'], DATE);
	  addParseToken('Do', function (input, array) {
	    array[DATE] = toInt(input.match(match1to2)[0]);
	  }); // MOMENTS

	  var getSetDayOfMonth = makeGetSet('Date', true); // FORMATTING

	  addFormatToken('DDD', ['DDDD', 3], 'DDDo', 'dayOfYear'); // ALIASES

	  addUnitAlias('dayOfYear', 'DDD'); // PRIORITY

	  addUnitPriority('dayOfYear', 4); // PARSING

	  addRegexToken('DDD', match1to3);
	  addRegexToken('DDDD', match3);
	  addParseToken(['DDD', 'DDDD'], function (input, array, config) {
	    config._dayOfYear = toInt(input);
	  }); // HELPERS
	  // MOMENTS

	  function getSetDayOfYear(input) {
	    var dayOfYear = Math.round((this.clone().startOf('day') - this.clone().startOf('year')) / 864e5) + 1;
	    return input == null ? dayOfYear : this.add(input - dayOfYear, 'd');
	  } // FORMATTING


	  addFormatToken('m', ['mm', 2], 0, 'minute'); // ALIASES

	  addUnitAlias('minute', 'm'); // PRIORITY

	  addUnitPriority('minute', 14); // PARSING

	  addRegexToken('m', match1to2);
	  addRegexToken('mm', match1to2, match2);
	  addParseToken(['m', 'mm'], MINUTE); // MOMENTS

	  var getSetMinute = makeGetSet('Minutes', false); // FORMATTING

	  addFormatToken('s', ['ss', 2], 0, 'second'); // ALIASES

	  addUnitAlias('second', 's'); // PRIORITY

	  addUnitPriority('second', 15); // PARSING

	  addRegexToken('s', match1to2);
	  addRegexToken('ss', match1to2, match2);
	  addParseToken(['s', 'ss'], SECOND); // MOMENTS

	  var getSetSecond = makeGetSet('Seconds', false); // FORMATTING

	  addFormatToken('S', 0, 0, function () {
	    return ~~(this.millisecond() / 100);
	  });
	  addFormatToken(0, ['SS', 2], 0, function () {
	    return ~~(this.millisecond() / 10);
	  });
	  addFormatToken(0, ['SSS', 3], 0, 'millisecond');
	  addFormatToken(0, ['SSSS', 4], 0, function () {
	    return this.millisecond() * 10;
	  });
	  addFormatToken(0, ['SSSSS', 5], 0, function () {
	    return this.millisecond() * 100;
	  });
	  addFormatToken(0, ['SSSSSS', 6], 0, function () {
	    return this.millisecond() * 1000;
	  });
	  addFormatToken(0, ['SSSSSSS', 7], 0, function () {
	    return this.millisecond() * 10000;
	  });
	  addFormatToken(0, ['SSSSSSSS', 8], 0, function () {
	    return this.millisecond() * 100000;
	  });
	  addFormatToken(0, ['SSSSSSSSS', 9], 0, function () {
	    return this.millisecond() * 1000000;
	  }); // ALIASES

	  addUnitAlias('millisecond', 'ms'); // PRIORITY

	  addUnitPriority('millisecond', 16); // PARSING

	  addRegexToken('S', match1to3, match1);
	  addRegexToken('SS', match1to3, match2);
	  addRegexToken('SSS', match1to3, match3);
	  var token;

	  for (token = 'SSSS'; token.length <= 9; token += 'S') {
	    addRegexToken(token, matchUnsigned);
	  }

	  function parseMs(input, array) {
	    array[MILLISECOND] = toInt(('0.' + input) * 1000);
	  }

	  for (token = 'S'; token.length <= 9; token += 'S') {
	    addParseToken(token, parseMs);
	  } // MOMENTS


	  var getSetMillisecond = makeGetSet('Milliseconds', false); // FORMATTING

	  addFormatToken('z', 0, 0, 'zoneAbbr');
	  addFormatToken('zz', 0, 0, 'zoneName'); // MOMENTS

	  function getZoneAbbr() {
	    return this._isUTC ? 'UTC' : '';
	  }

	  function getZoneName() {
	    return this._isUTC ? 'Coordinated Universal Time' : '';
	  }

	  var proto = Moment.prototype;
	  proto.add = add;
	  proto.calendar = calendar$1;
	  proto.clone = clone;
	  proto.diff = diff;
	  proto.endOf = endOf;
	  proto.format = format;
	  proto.from = from;
	  proto.fromNow = fromNow;
	  proto.to = to;
	  proto.toNow = toNow;
	  proto.get = stringGet;
	  proto.invalidAt = invalidAt;
	  proto.isAfter = isAfter;
	  proto.isBefore = isBefore;
	  proto.isBetween = isBetween;
	  proto.isSame = isSame;
	  proto.isSameOrAfter = isSameOrAfter;
	  proto.isSameOrBefore = isSameOrBefore;
	  proto.isValid = isValid$2;
	  proto.lang = lang;
	  proto.locale = locale;
	  proto.localeData = localeData;
	  proto.max = prototypeMax;
	  proto.min = prototypeMin;
	  proto.parsingFlags = parsingFlags;
	  proto.set = stringSet;
	  proto.startOf = startOf;
	  proto.subtract = subtract;
	  proto.toArray = toArray;
	  proto.toObject = toObject;
	  proto.toDate = toDate;
	  proto.toISOString = toISOString;
	  proto.inspect = inspect;
	  proto.toJSON = toJSON;
	  proto.toString = toString;
	  proto.unix = unix;
	  proto.valueOf = valueOf;
	  proto.creationData = creationData;
	  proto.year = getSetYear;
	  proto.isLeapYear = getIsLeapYear;
	  proto.weekYear = getSetWeekYear;
	  proto.isoWeekYear = getSetISOWeekYear;
	  proto.quarter = proto.quarters = getSetQuarter;
	  proto.month = getSetMonth;
	  proto.daysInMonth = getDaysInMonth;
	  proto.week = proto.weeks = getSetWeek;
	  proto.isoWeek = proto.isoWeeks = getSetISOWeek;
	  proto.weeksInYear = getWeeksInYear;
	  proto.isoWeeksInYear = getISOWeeksInYear;
	  proto.date = getSetDayOfMonth;
	  proto.day = proto.days = getSetDayOfWeek;
	  proto.weekday = getSetLocaleDayOfWeek;
	  proto.isoWeekday = getSetISODayOfWeek;
	  proto.dayOfYear = getSetDayOfYear;
	  proto.hour = proto.hours = getSetHour;
	  proto.minute = proto.minutes = getSetMinute;
	  proto.second = proto.seconds = getSetSecond;
	  proto.millisecond = proto.milliseconds = getSetMillisecond;
	  proto.utcOffset = getSetOffset;
	  proto.utc = setOffsetToUTC;
	  proto.local = setOffsetToLocal;
	  proto.parseZone = setOffsetToParsedOffset;
	  proto.hasAlignedHourOffset = hasAlignedHourOffset;
	  proto.isDST = isDaylightSavingTime;
	  proto.isLocal = isLocal;
	  proto.isUtcOffset = isUtcOffset;
	  proto.isUtc = isUtc;
	  proto.isUTC = isUtc;
	  proto.zoneAbbr = getZoneAbbr;
	  proto.zoneName = getZoneName;
	  proto.dates = deprecate('dates accessor is deprecated. Use date instead.', getSetDayOfMonth);
	  proto.months = deprecate('months accessor is deprecated. Use month instead', getSetMonth);
	  proto.years = deprecate('years accessor is deprecated. Use year instead', getSetYear);
	  proto.zone = deprecate('moment().zone is deprecated, use moment().utcOffset instead. http://momentjs.com/guides/#/warnings/zone/', getSetZone);
	  proto.isDSTShifted = deprecate('isDSTShifted is deprecated. See http://momentjs.com/guides/#/warnings/dst-shifted/ for more information', isDaylightSavingTimeShifted);

	  function createUnix(input) {
	    return createLocal(input * 1000);
	  }

	  function createInZone() {
	    return createLocal.apply(null, arguments).parseZone();
	  }

	  function preParsePostFormat(string) {
	    return string;
	  }

	  var proto$1 = Locale.prototype;
	  proto$1.calendar = calendar;
	  proto$1.longDateFormat = longDateFormat;
	  proto$1.invalidDate = invalidDate;
	  proto$1.ordinal = ordinal;
	  proto$1.preparse = preParsePostFormat;
	  proto$1.postformat = preParsePostFormat;
	  proto$1.relativeTime = relativeTime;
	  proto$1.pastFuture = pastFuture;
	  proto$1.set = set;
	  proto$1.months = localeMonths;
	  proto$1.monthsShort = localeMonthsShort;
	  proto$1.monthsParse = localeMonthsParse;
	  proto$1.monthsRegex = monthsRegex;
	  proto$1.monthsShortRegex = monthsShortRegex;
	  proto$1.week = localeWeek;
	  proto$1.firstDayOfYear = localeFirstDayOfYear;
	  proto$1.firstDayOfWeek = localeFirstDayOfWeek;
	  proto$1.weekdays = localeWeekdays;
	  proto$1.weekdaysMin = localeWeekdaysMin;
	  proto$1.weekdaysShort = localeWeekdaysShort;
	  proto$1.weekdaysParse = localeWeekdaysParse;
	  proto$1.weekdaysRegex = weekdaysRegex;
	  proto$1.weekdaysShortRegex = weekdaysShortRegex;
	  proto$1.weekdaysMinRegex = weekdaysMinRegex;
	  proto$1.isPM = localeIsPM;
	  proto$1.meridiem = localeMeridiem;

	  function get$1(format, index, field, setter) {
	    var locale = getLocale();
	    var utc = createUTC().set(setter, index);
	    return locale[field](utc, format);
	  }

	  function listMonthsImpl(format, index, field) {
	    if (isNumber(format)) {
	      index = format;
	      format = undefined;
	    }

	    format = format || '';

	    if (index != null) {
	      return get$1(format, index, field, 'month');
	    }

	    var i;
	    var out = [];

	    for (i = 0; i < 12; i++) {
	      out[i] = get$1(format, i, field, 'month');
	    }

	    return out;
	  } // ()
	  // (5)
	  // (fmt, 5)
	  // (fmt)
	  // (true)
	  // (true, 5)
	  // (true, fmt, 5)
	  // (true, fmt)


	  function listWeekdaysImpl(localeSorted, format, index, field) {
	    if (typeof localeSorted === 'boolean') {
	      if (isNumber(format)) {
	        index = format;
	        format = undefined;
	      }

	      format = format || '';
	    } else {
	      format = localeSorted;
	      index = format;
	      localeSorted = false;

	      if (isNumber(format)) {
	        index = format;
	        format = undefined;
	      }

	      format = format || '';
	    }

	    var locale = getLocale(),
	        shift = localeSorted ? locale._week.dow : 0;

	    if (index != null) {
	      return get$1(format, (index + shift) % 7, field, 'day');
	    }

	    var i;
	    var out = [];

	    for (i = 0; i < 7; i++) {
	      out[i] = get$1(format, (i + shift) % 7, field, 'day');
	    }

	    return out;
	  }

	  function listMonths(format, index) {
	    return listMonthsImpl(format, index, 'months');
	  }

	  function listMonthsShort(format, index) {
	    return listMonthsImpl(format, index, 'monthsShort');
	  }

	  function listWeekdays(localeSorted, format, index) {
	    return listWeekdaysImpl(localeSorted, format, index, 'weekdays');
	  }

	  function listWeekdaysShort(localeSorted, format, index) {
	    return listWeekdaysImpl(localeSorted, format, index, 'weekdaysShort');
	  }

	  function listWeekdaysMin(localeSorted, format, index) {
	    return listWeekdaysImpl(localeSorted, format, index, 'weekdaysMin');
	  }

	  getSetGlobalLocale('en', {
	    dayOfMonthOrdinalParse: /\d{1,2}(th|st|nd|rd)/,
	    ordinal: function ordinal(number) {
	      var b = number % 10,
	          output = toInt(number % 100 / 10) === 1 ? 'th' : b === 1 ? 'st' : b === 2 ? 'nd' : b === 3 ? 'rd' : 'th';
	      return number + output;
	    }
	  }); // Side effect imports

	  hooks.lang = deprecate('moment.lang is deprecated. Use moment.locale instead.', getSetGlobalLocale);
	  hooks.langData = deprecate('moment.langData is deprecated. Use moment.localeData instead.', getLocale);
	  var mathAbs = Math.abs;

	  function abs() {
	    var data = this._data;
	    this._milliseconds = mathAbs(this._milliseconds);
	    this._days = mathAbs(this._days);
	    this._months = mathAbs(this._months);
	    data.milliseconds = mathAbs(data.milliseconds);
	    data.seconds = mathAbs(data.seconds);
	    data.minutes = mathAbs(data.minutes);
	    data.hours = mathAbs(data.hours);
	    data.months = mathAbs(data.months);
	    data.years = mathAbs(data.years);
	    return this;
	  }

	  function addSubtract$1(duration, input, value, direction) {
	    var other = createDuration(input, value);
	    duration._milliseconds += direction * other._milliseconds;
	    duration._days += direction * other._days;
	    duration._months += direction * other._months;
	    return duration._bubble();
	  } // supports only 2.0-style add(1, 's') or add(duration)


	  function add$1(input, value) {
	    return addSubtract$1(this, input, value, 1);
	  } // supports only 2.0-style subtract(1, 's') or subtract(duration)


	  function subtract$1(input, value) {
	    return addSubtract$1(this, input, value, -1);
	  }

	  function absCeil(number) {
	    if (number < 0) {
	      return Math.floor(number);
	    } else {
	      return Math.ceil(number);
	    }
	  }

	  function bubble() {
	    var milliseconds = this._milliseconds;
	    var days = this._days;
	    var months = this._months;
	    var data = this._data;
	    var seconds, minutes, hours, years, monthsFromDays; // if we have a mix of positive and negative values, bubble down first
	    // check: https://github.com/moment/moment/issues/2166

	    if (!(milliseconds >= 0 && days >= 0 && months >= 0 || milliseconds <= 0 && days <= 0 && months <= 0)) {
	      milliseconds += absCeil(monthsToDays(months) + days) * 864e5;
	      days = 0;
	      months = 0;
	    } // The following code bubbles up values, see the tests for
	    // examples of what that means.


	    data.milliseconds = milliseconds % 1000;
	    seconds = absFloor(milliseconds / 1000);
	    data.seconds = seconds % 60;
	    minutes = absFloor(seconds / 60);
	    data.minutes = minutes % 60;
	    hours = absFloor(minutes / 60);
	    data.hours = hours % 24;
	    days += absFloor(hours / 24); // convert days to months

	    monthsFromDays = absFloor(daysToMonths(days));
	    months += monthsFromDays;
	    days -= absCeil(monthsToDays(monthsFromDays)); // 12 months -> 1 year

	    years = absFloor(months / 12);
	    months %= 12;
	    data.days = days;
	    data.months = months;
	    data.years = years;
	    return this;
	  }

	  function daysToMonths(days) {
	    // 400 years have 146097 days (taking into account leap year rules)
	    // 400 years have 12 months === 4800
	    return days * 4800 / 146097;
	  }

	  function monthsToDays(months) {
	    // the reverse of daysToMonths
	    return months * 146097 / 4800;
	  }

	  function as(units) {
	    if (!this.isValid()) {
	      return NaN;
	    }

	    var days;
	    var months;
	    var milliseconds = this._milliseconds;
	    units = normalizeUnits(units);

	    if (units === 'month' || units === 'year') {
	      days = this._days + milliseconds / 864e5;
	      months = this._months + daysToMonths(days);
	      return units === 'month' ? months : months / 12;
	    } else {
	      // handle milliseconds separately because of floating point math errors (issue #1867)
	      days = this._days + Math.round(monthsToDays(this._months));

	      switch (units) {
	        case 'week':
	          return days / 7 + milliseconds / 6048e5;

	        case 'day':
	          return days + milliseconds / 864e5;

	        case 'hour':
	          return days * 24 + milliseconds / 36e5;

	        case 'minute':
	          return days * 1440 + milliseconds / 6e4;

	        case 'second':
	          return days * 86400 + milliseconds / 1000;
	        // Math.floor prevents floating point math errors here

	        case 'millisecond':
	          return Math.floor(days * 864e5) + milliseconds;

	        default:
	          throw new Error('Unknown unit ' + units);
	      }
	    }
	  } // TODO: Use this.as('ms')?


	  function valueOf$1() {
	    if (!this.isValid()) {
	      return NaN;
	    }

	    return this._milliseconds + this._days * 864e5 + this._months % 12 * 2592e6 + toInt(this._months / 12) * 31536e6;
	  }

	  function makeAs(alias) {
	    return function () {
	      return this.as(alias);
	    };
	  }

	  var asMilliseconds = makeAs('ms');
	  var asSeconds = makeAs('s');
	  var asMinutes = makeAs('m');
	  var asHours = makeAs('h');
	  var asDays = makeAs('d');
	  var asWeeks = makeAs('w');
	  var asMonths = makeAs('M');
	  var asYears = makeAs('y');

	  function clone$1() {
	    return createDuration(this);
	  }

	  function get$2(units) {
	    units = normalizeUnits(units);
	    return this.isValid() ? this[units + 's']() : NaN;
	  }

	  function makeGetter(name) {
	    return function () {
	      return this.isValid() ? this._data[name] : NaN;
	    };
	  }

	  var milliseconds = makeGetter('milliseconds');
	  var seconds = makeGetter('seconds');
	  var minutes = makeGetter('minutes');
	  var hours = makeGetter('hours');
	  var days = makeGetter('days');
	  var months = makeGetter('months');
	  var years = makeGetter('years');

	  function weeks() {
	    return absFloor(this.days() / 7);
	  }

	  var round = Math.round;
	  var thresholds = {
	    ss: 44,
	    // a few seconds to seconds
	    s: 45,
	    // seconds to minute
	    m: 45,
	    // minutes to hour
	    h: 22,
	    // hours to day
	    d: 26,
	    // days to month
	    M: 11 // months to year

	  }; // helper function for moment.fn.from, moment.fn.fromNow, and moment.duration.fn.humanize

	  function substituteTimeAgo(string, number, withoutSuffix, isFuture, locale) {
	    return locale.relativeTime(number || 1, !!withoutSuffix, string, isFuture);
	  }

	  function relativeTime$1(posNegDuration, withoutSuffix, locale) {
	    var duration = createDuration(posNegDuration).abs();
	    var seconds = round(duration.as('s'));
	    var minutes = round(duration.as('m'));
	    var hours = round(duration.as('h'));
	    var days = round(duration.as('d'));
	    var months = round(duration.as('M'));
	    var years = round(duration.as('y'));
	    var a = seconds <= thresholds.ss && ['s', seconds] || seconds < thresholds.s && ['ss', seconds] || minutes <= 1 && ['m'] || minutes < thresholds.m && ['mm', minutes] || hours <= 1 && ['h'] || hours < thresholds.h && ['hh', hours] || days <= 1 && ['d'] || days < thresholds.d && ['dd', days] || months <= 1 && ['M'] || months < thresholds.M && ['MM', months] || years <= 1 && ['y'] || ['yy', years];
	    a[2] = withoutSuffix;
	    a[3] = +posNegDuration > 0;
	    a[4] = locale;
	    return substituteTimeAgo.apply(null, a);
	  } // This function allows you to set the rounding function for relative time strings


	  function getSetRelativeTimeRounding(roundingFunction) {
	    if (roundingFunction === undefined) {
	      return round;
	    }

	    if (typeof roundingFunction === 'function') {
	      round = roundingFunction;
	      return true;
	    }

	    return false;
	  } // This function allows you to set a threshold for relative time strings


	  function getSetRelativeTimeThreshold(threshold, limit) {
	    if (thresholds[threshold] === undefined) {
	      return false;
	    }

	    if (limit === undefined) {
	      return thresholds[threshold];
	    }

	    thresholds[threshold] = limit;

	    if (threshold === 's') {
	      thresholds.ss = limit - 1;
	    }

	    return true;
	  }

	  function humanize(withSuffix) {
	    if (!this.isValid()) {
	      return this.localeData().invalidDate();
	    }

	    var locale = this.localeData();
	    var output = relativeTime$1(this, !withSuffix, locale);

	    if (withSuffix) {
	      output = locale.pastFuture(+this, output);
	    }

	    return locale.postformat(output);
	  }

	  var abs$1 = Math.abs;

	  function sign(x) {
	    return (x > 0) - (x < 0) || +x;
	  }

	  function toISOString$1() {
	    // for ISO strings we do not use the normal bubbling rules:
	    //  * milliseconds bubble up until they become hours
	    //  * days do not bubble at all
	    //  * months bubble up until they become years
	    // This is because there is no context-free conversion between hours and days
	    // (think of clock changes)
	    // and also not between days and months (28-31 days per month)
	    if (!this.isValid()) {
	      return this.localeData().invalidDate();
	    }

	    var seconds = abs$1(this._milliseconds) / 1000;
	    var days = abs$1(this._days);
	    var months = abs$1(this._months);
	    var minutes, hours, years; // 3600 seconds -> 60 minutes -> 1 hour

	    minutes = absFloor(seconds / 60);
	    hours = absFloor(minutes / 60);
	    seconds %= 60;
	    minutes %= 60; // 12 months -> 1 year

	    years = absFloor(months / 12);
	    months %= 12; // inspired by https://github.com/dordille/moment-isoduration/blob/master/moment.isoduration.js

	    var Y = years;
	    var M = months;
	    var D = days;
	    var h = hours;
	    var m = minutes;
	    var s = seconds ? seconds.toFixed(3).replace(/\.?0+$/, '') : '';
	    var total = this.asSeconds();

	    if (!total) {
	      // this is the same as C#'s (Noda) and python (isodate)...
	      // but not other JS (goog.date)
	      return 'P0D';
	    }

	    var totalSign = total < 0 ? '-' : '';
	    var ymSign = sign(this._months) !== sign(total) ? '-' : '';
	    var daysSign = sign(this._days) !== sign(total) ? '-' : '';
	    var hmsSign = sign(this._milliseconds) !== sign(total) ? '-' : '';
	    return totalSign + 'P' + (Y ? ymSign + Y + 'Y' : '') + (M ? ymSign + M + 'M' : '') + (D ? daysSign + D + 'D' : '') + (h || m || s ? 'T' : '') + (h ? hmsSign + h + 'H' : '') + (m ? hmsSign + m + 'M' : '') + (s ? hmsSign + s + 'S' : '');
	  }

	  var proto$2 = Duration.prototype;
	  proto$2.isValid = isValid$1;
	  proto$2.abs = abs;
	  proto$2.add = add$1;
	  proto$2.subtract = subtract$1;
	  proto$2.as = as;
	  proto$2.asMilliseconds = asMilliseconds;
	  proto$2.asSeconds = asSeconds;
	  proto$2.asMinutes = asMinutes;
	  proto$2.asHours = asHours;
	  proto$2.asDays = asDays;
	  proto$2.asWeeks = asWeeks;
	  proto$2.asMonths = asMonths;
	  proto$2.asYears = asYears;
	  proto$2.valueOf = valueOf$1;
	  proto$2._bubble = bubble;
	  proto$2.clone = clone$1;
	  proto$2.get = get$2;
	  proto$2.milliseconds = milliseconds;
	  proto$2.seconds = seconds;
	  proto$2.minutes = minutes;
	  proto$2.hours = hours;
	  proto$2.days = days;
	  proto$2.weeks = weeks;
	  proto$2.months = months;
	  proto$2.years = years;
	  proto$2.humanize = humanize;
	  proto$2.toISOString = toISOString$1;
	  proto$2.toString = toISOString$1;
	  proto$2.toJSON = toISOString$1;
	  proto$2.locale = locale;
	  proto$2.localeData = localeData;
	  proto$2.toIsoString = deprecate('toIsoString() is deprecated. Please use toISOString() instead (notice the capitals)', toISOString$1);
	  proto$2.lang = lang; // Side effect imports
	  // FORMATTING

	  addFormatToken('X', 0, 0, 'unix');
	  addFormatToken('x', 0, 0, 'valueOf'); // PARSING

	  addRegexToken('x', matchSigned);
	  addRegexToken('X', matchTimestamp);
	  addParseToken('X', function (input, array, config) {
	    config._d = new Date(parseFloat(input, 10) * 1000);
	  });
	  addParseToken('x', function (input, array, config) {
	    config._d = new Date(toInt(input));
	  }); // Side effect imports

	  hooks.version = '2.22.2';
	  setHookCallback(createLocal);
	  hooks.fn = proto;
	  hooks.min = min;
	  hooks.max = max;
	  hooks.now = now;
	  hooks.utc = createUTC;
	  hooks.unix = createUnix;
	  hooks.months = listMonths;
	  hooks.isDate = isDate;
	  hooks.locale = getSetGlobalLocale;
	  hooks.invalid = createInvalid;
	  hooks.duration = createDuration;
	  hooks.isMoment = isMoment;
	  hooks.weekdays = listWeekdays;
	  hooks.parseZone = createInZone;
	  hooks.localeData = getLocale;
	  hooks.isDuration = isDuration;
	  hooks.monthsShort = listMonthsShort;
	  hooks.weekdaysMin = listWeekdaysMin;
	  hooks.defineLocale = defineLocale;
	  hooks.updateLocale = updateLocale;
	  hooks.locales = listLocales;
	  hooks.weekdaysShort = listWeekdaysShort;
	  hooks.normalizeUnits = normalizeUnits;
	  hooks.relativeTimeRounding = getSetRelativeTimeRounding;
	  hooks.relativeTimeThreshold = getSetRelativeTimeThreshold;
	  hooks.calendarFormat = getCalendarFormat;
	  hooks.prototype = proto; // currently HTML5 input type only supports 24-hour formats

	  hooks.HTML5_FMT = {
	    DATETIME_LOCAL: 'YYYY-MM-DDTHH:mm',
	    // <input type="datetime-local" />
	    DATETIME_LOCAL_SECONDS: 'YYYY-MM-DDTHH:mm:ss',
	    // <input type="datetime-local" step="1" />
	    DATETIME_LOCAL_MS: 'YYYY-MM-DDTHH:mm:ss.SSS',
	    // <input type="datetime-local" step="0.001" />
	    DATE: 'YYYY-MM-DD',
	    // <input type="date" />
	    TIME: 'HH:mm',
	    // <input type="time" />
	    TIME_SECONDS: 'HH:mm:ss',
	    // <input type="time" step="1" />
	    TIME_MS: 'HH:mm:ss.SSS',
	    // <input type="time" step="0.001" />
	    WEEK: 'YYYY-[W]WW',
	    // <input type="week" />
	    MONTH: 'YYYY-MM' // <input type="month" />

	  };
	  return hooks;
	});

	var Moment = /*#__PURE__*/Object.freeze({

	});

	// @@search logic
	require('./_fix-re-wks')('search', 1, function (defined, SEARCH, $search) {
	  // 21.1.3.15 String.prototype.search(regexp)
	  return [function search(regexp) {

	    var O = defined(this);
	    var fn = regexp == undefined ? undefined : regexp[SEARCH];
	    return fn !== undefined ? fn.call(regexp, O) : new RegExp(regexp)[SEARCH](String(O));
	  }, $search];
	});

	var dP$8 = require('./_object-dp');

	var anObject$5 = require('./_an-object');

	var getKeys$1 = require('./_object-keys');

	module.exports = require('./_descriptors') ? Object.defineProperties : function defineProperties(O, Properties) {
	  anObject$5(O);
	  var keys = getKeys$1(Properties);
	  var length = keys.length;
	  var i = 0;
	  var P;

	  while (length > i) {
	    dP$8.f(O, P = keys[i++], Properties[P]);
	  }

	  return O;
	};

	var _objectDps = /*#__PURE__*/Object.freeze({

	});

	// 19.1.2.3 / 15.2.3.7 Object.defineProperties(O, Properties)


	$export$1($export$1.S + $export$1.F * !DESCRIPTORS, 'Object', {
	  defineProperties: _objectDps
	});

	!function (t, e) {
	  "object" == (typeof exports === "undefined" ? "undefined" : _typeof(exports)) && "object" == (typeof module === "undefined" ? "undefined" : _typeof(module)) ? module.exports = e(require("moment")) : "function" == typeof define && define.amd ? define("moment-range", ["moment"], e) : "object" == (typeof exports === "undefined" ? "undefined" : _typeof(exports)) ? exports["moment-range"] = e(require("moment")) : t["moment-range"] = e(t.moment);
	}(undefined, function (t) {
	  return function (t) {
	    function e(r) {
	      if (n[r]) return n[r].exports;
	      var o = n[r] = {
	        i: r,
	        l: !1,
	        exports: {}
	      };
	      return t[r].call(o.exports, o, o.exports, e), o.l = !0, o.exports;
	    }

	    var n = {};
	    return e.m = t, e.c = n, e.i = function (t) {
	      return t;
	    }, e.d = function (t, n, r) {
	      e.o(t, n) || Object.defineProperty(t, n, {
	        configurable: !1,
	        enumerable: !0,
	        get: r
	      });
	    }, e.n = function (t) {
	      var n = t && t.__esModule ? function () {
	        return t.default;
	      } : function () {
	        return t;
	      };
	      return e.d(n, "a", n), n;
	    }, e.o = function (t, e) {
	      return Object.prototype.hasOwnProperty.call(t, e);
	    }, e.p = "", e(e.s = 3);
	  }([function (t, e, n) {

	    var r = n(5)();

	    t.exports = function (t) {
	      return t !== r && null !== t;
	    };
	  }, function (t, e, n) {

	    t.exports = n(18)() ? Symbol : n(20);
	  }, function (e, n) {
	    e.exports = t;
	  }, function (t, e, n) {

	    function r(t) {
	      return t && t.__esModule ? t : {
	        default: t
	      };
	    }

	    function o(t, e, n) {
	      return e in t ? Object.defineProperty(t, e, {
	        value: n,
	        enumerable: !0,
	        configurable: !0,
	        writable: !0
	      }) : t[e] = n, t;
	    }

	    function i(t, e) {
	      if (!(t instanceof e)) throw new TypeError("Cannot call a class as a function");
	    }

	    function u(t) {
	      return t.range = function (e, n) {
	        var r = this;
	        return "string" == typeof e && y.hasOwnProperty(e) ? new h(t(r).startOf(e), t(r).endOf(e)) : new h(e, n);
	      }, t.rangeFromInterval = function (e) {
	        var n = arguments.length > 1 && void 0 !== arguments[1] ? arguments[1] : 1,
	            r = arguments.length > 2 && void 0 !== arguments[2] ? arguments[2] : t();
	        if (t.isMoment(r) || (r = t(r)), !r.isValid()) throw new Error("Invalid date.");
	        var o = r.clone().add(n, e),
	            i = [];
	        return i.push(t.min(r, o)), i.push(t.max(r, o)), new h(i);
	      }, t.rangeFromISOString = function (e) {
	        var n = a(e),
	            r = t.parseZone(n[0]),
	            o = t.parseZone(n[1]);
	        return new h(r, o);
	      }, t.parseZoneRange = t.rangeFromISOString, t.fn.range = t.range, t.range.constructor = h, t.isRange = function (t) {
	        return t instanceof h;
	      }, t.fn.within = function (t) {
	        return t.contains(this.toDate());
	      }, t;
	    }

	    function a(t) {
	      return t.split("/");
	    }

	    Object.defineProperty(e, "__esModule", {
	      value: !0
	    }), e.DateRange = void 0;

	    var s = function () {
	      function t(t, e) {
	        var n = [],
	            r = !0,
	            o = !1,
	            i = void 0;

	        try {
	          for (var u, a = t[Symbol.iterator](); !(r = (u = a.next()).done) && (n.push(u.value), !e || n.length !== e); r = !0) {
	          }
	        } catch (t) {
	          o = !0, i = t;
	        } finally {
	          try {
	            !r && a.return && a.return();
	          } finally {
	            if (o) throw i;
	          }
	        }

	        return n;
	      }

	      return function (e, n) {
	        if (Array.isArray(e)) return e;
	        if (Symbol.iterator in Object(e)) return t(e, n);
	        throw new TypeError("Invalid attempt to destructure non-iterable instance");
	      };
	    }(),
	        c = "function" == typeof Symbol && "symbol" == _typeof(Symbol.iterator) ? function (t) {
	      return _typeof(t);
	    } : function (t) {
	      return t && "function" == typeof Symbol && t.constructor === Symbol && t !== Symbol.prototype ? "symbol" : _typeof(t);
	    },
	        f = function () {
	      function t(t, e) {
	        for (var n = 0; n < e.length; n++) {
	          var r = e[n];
	          r.enumerable = r.enumerable || !1, r.configurable = !0, "value" in r && (r.writable = !0), Object.defineProperty(t, r.key, r);
	        }
	      }

	      return function (e, n, r) {
	        return n && t(e.prototype, n), r && t(e, r), e;
	      };
	    }();

	    e.extendMoment = u;

	    var l = n(2),
	        v = r(l),
	        d = n(1),
	        p = r(d),
	        y = {
	      year: !0,
	      quarter: !0,
	      month: !0,
	      week: !0,
	      day: !0,
	      hour: !0,
	      minute: !0,
	      second: !0
	    },
	        h = e.DateRange = function () {
	      function t(e, n) {
	        i(this, t);
	        var r = e,
	            o = n;
	        if (1 === arguments.length || void 0 === n) if ("object" === (void 0 === e ? "undefined" : c(e)) && 2 === e.length) {
	          var u = s(e, 2);
	          r = u[0], o = u[1];
	        } else if ("string" == typeof e) {
	          var f = a(e),
	              l = s(f, 2);
	          r = l[0], o = l[1];
	        }
	        this.start = r || 0 === r ? (0, v.default)(r) : (0, v.default)(-864e13), this.end = o || 0 === o ? (0, v.default)(o) : (0, v.default)(864e13);
	      }

	      return f(t, [{
	        key: "adjacent",
	        value: function value(t) {
	          var e = this.start.isSame(t.end),
	              n = this.end.isSame(t.start);
	          return e && t.start.valueOf() <= this.start.valueOf() || n && t.end.valueOf() >= this.end.valueOf();
	        }
	      }, {
	        key: "add",
	        value: function value(t) {
	          var e = arguments.length > 1 && void 0 !== arguments[1] ? arguments[1] : {
	            adjacent: !1
	          };
	          return this.overlaps(t, e) ? new this.constructor(v.default.min(this.start, t.start), v.default.max(this.end, t.end)) : null;
	        }
	      }, {
	        key: "by",
	        value: function value(t) {
	          var e = arguments.length > 1 && void 0 !== arguments[1] ? arguments[1] : {
	            excludeEnd: !1,
	            step: 1
	          },
	              n = this;
	          return o({}, p.default.iterator, function () {
	            var r = e.step || 1,
	                o = Math.abs(n.start.diff(n.end, t)) / r,
	                i = e.excludeEnd || !1,
	                u = 0;
	            return e.hasOwnProperty("exclusive") && (i = e.exclusive), {
	              next: function next() {
	                var e = n.start.clone().add(u * r, t),
	                    a = i ? !(u < o) : !(u <= o);
	                return u++, {
	                  done: a,
	                  value: a ? void 0 : e
	                };
	              }
	            };
	          });
	        }
	      }, {
	        key: "byRange",
	        value: function value(t) {
	          var e = arguments.length > 1 && void 0 !== arguments[1] ? arguments[1] : {
	            excludeEnd: !1,
	            step: 1
	          },
	              n = this,
	              r = e.step || 1,
	              i = this.valueOf() / t.valueOf() / r,
	              u = Math.floor(i),
	              a = e.excludeEnd || !1,
	              s = 0;
	          return e.hasOwnProperty("exclusive") && (a = e.exclusive), o({}, p.default.iterator, function () {
	            return u === 1 / 0 ? {
	              done: !0
	            } : {
	              next: function next() {
	                var e = (0, v.default)(n.start.valueOf() + t.valueOf() * s * r),
	                    o = u === i && a ? !(s < u) : !(s <= u);
	                return s++, {
	                  done: o,
	                  value: o ? void 0 : e
	                };
	              }
	            };
	          });
	        }
	      }, {
	        key: "center",
	        value: function value() {
	          var t = this.start.valueOf() + this.diff() / 2;
	          return (0, v.default)(t);
	        }
	      }, {
	        key: "clone",
	        value: function value() {
	          return new this.constructor(this.start.clone(), this.end.clone());
	        }
	      }, {
	        key: "contains",
	        value: function value(e) {
	          var n = arguments.length > 1 && void 0 !== arguments[1] ? arguments[1] : {
	            excludeStart: !1,
	            excludeEnd: !1
	          },
	              r = this.start.valueOf(),
	              o = this.end.valueOf(),
	              i = e.valueOf(),
	              u = e.valueOf(),
	              a = n.excludeStart || !1,
	              s = n.excludeEnd || !1;
	          n.hasOwnProperty("exclusive") && (a = s = n.exclusive), e instanceof t && (i = e.start.valueOf(), u = e.end.valueOf());
	          var c = r < i || r <= i && !a,
	              f = o > u || o >= u && !s;
	          return c && f;
	        }
	      }, {
	        key: "diff",
	        value: function value(t, e) {
	          return this.end.diff(this.start, t, e);
	        }
	      }, {
	        key: "duration",
	        value: function value(t, e) {
	          return this.diff(t, e);
	        }
	      }, {
	        key: "intersect",
	        value: function value(t) {
	          var e = this.start.valueOf(),
	              n = this.end.valueOf(),
	              r = t.start.valueOf(),
	              o = t.end.valueOf(),
	              i = e == n,
	              u = r == o;

	          if (i) {
	            var a = e;
	            if (a == r || a == o) return null;
	            if (a > r && a < o) return this.clone();
	          } else if (u) {
	            var s = r;
	            if (s == e || s == n) return null;
	            if (s > e && s < n) return new this.constructor(s, s);
	          }

	          return e <= r && r < n && n < o ? new this.constructor(r, n) : r < e && e < o && o <= n ? new this.constructor(e, o) : r < e && e <= n && n < o ? this.clone() : e <= r && r <= o && o <= n ? new this.constructor(r, o) : null;
	        }
	      }, {
	        key: "isEqual",
	        value: function value(t) {
	          return this.start.isSame(t.start) && this.end.isSame(t.end);
	        }
	      }, {
	        key: "isSame",
	        value: function value(t) {
	          return this.isEqual(t);
	        }
	      }, {
	        key: "overlaps",
	        value: function value(t) {
	          var e = arguments.length > 1 && void 0 !== arguments[1] ? arguments[1] : {
	            adjacent: !1
	          },
	              n = null !== this.intersect(t);
	          return e.adjacent && !n ? this.adjacent(t) : n;
	        }
	      }, {
	        key: "reverseBy",
	        value: function value(t) {
	          var e = arguments.length > 1 && void 0 !== arguments[1] ? arguments[1] : {
	            excludeStart: !1,
	            step: 1
	          },
	              n = this;
	          return o({}, p.default.iterator, function () {
	            var r = e.step || 1,
	                o = Math.abs(n.start.diff(n.end, t)) / r,
	                i = e.excludeStart || !1,
	                u = 0;
	            return e.hasOwnProperty("exclusive") && (i = e.exclusive), {
	              next: function next() {
	                var e = n.end.clone().subtract(u * r, t),
	                    a = i ? !(u < o) : !(u <= o);
	                return u++, {
	                  done: a,
	                  value: a ? void 0 : e
	                };
	              }
	            };
	          });
	        }
	      }, {
	        key: "reverseByRange",
	        value: function value(t) {
	          var e = arguments.length > 1 && void 0 !== arguments[1] ? arguments[1] : {
	            excludeStart: !1,
	            step: 1
	          },
	              n = this,
	              r = e.step || 1,
	              i = this.valueOf() / t.valueOf() / r,
	              u = Math.floor(i),
	              a = e.excludeStart || !1,
	              s = 0;
	          return e.hasOwnProperty("exclusive") && (a = e.exclusive), o({}, p.default.iterator, function () {
	            return u === 1 / 0 ? {
	              done: !0
	            } : {
	              next: function next() {
	                var e = (0, v.default)(n.end.valueOf() - t.valueOf() * s * r),
	                    o = u === i && a ? !(s < u) : !(s <= u);
	                return s++, {
	                  done: o,
	                  value: o ? void 0 : e
	                };
	              }
	            };
	          });
	        }
	      }, {
	        key: "snapTo",
	        value: function value(t) {
	          var e = this.clone();
	          return e.start.isSame((0, v.default)(-864e13)) || (e.start = e.start.startOf(t)), e.end.isSame((0, v.default)(864e13)) || (e.end = e.end.endOf(t)), e;
	        }
	      }, {
	        key: "subtract",
	        value: function value(t) {
	          var e = this.start.valueOf(),
	              n = this.end.valueOf(),
	              r = t.start.valueOf(),
	              o = t.end.valueOf();
	          return null === this.intersect(t) ? [this] : r <= e && e < n && n <= o ? [] : r <= e && e < o && o < n ? [new this.constructor(o, n)] : e < r && r < n && n <= o ? [new this.constructor(e, r)] : e < r && r < o && o < n ? [new this.constructor(e, r), new this.constructor(o, n)] : e < r && r < n && o < n ? [new this.constructor(e, r), new this.constructor(r, n)] : [];
	        }
	      }, {
	        key: "toDate",
	        value: function value() {
	          return [this.start.toDate(), this.end.toDate()];
	        }
	      }, {
	        key: "toString",
	        value: function value() {
	          return this.start.format() + "/" + this.end.format();
	        }
	      }, {
	        key: "valueOf",
	        value: function value() {
	          return this.end.valueOf() - this.start.valueOf();
	        }
	      }]), t;
	    }();
	  }, function (t, e, n) {

	    var r,
	        o = n(6),
	        i = n(13),
	        u = n(9),
	        a = n(15);
	    r = t.exports = function (t, e) {
	      var n, r, u, s, c;
	      return arguments.length < 2 || "string" != typeof t ? (s = e, e = t, t = null) : s = arguments[2], null == t ? (n = u = !0, r = !1) : (n = a.call(t, "c"), r = a.call(t, "e"), u = a.call(t, "w")), c = {
	        value: e,
	        configurable: n,
	        enumerable: r,
	        writable: u
	      }, s ? o(i(s), c) : c;
	    }, r.gs = function (t, e, n) {
	      var r, s, c, f;
	      return "string" != typeof t ? (c = n, n = e, e = t, t = null) : c = arguments[3], null == e ? e = void 0 : u(e) ? null == n ? n = void 0 : u(n) || (c = n, n = void 0) : (c = e, e = n = void 0), null == t ? (r = !0, s = !1) : (r = a.call(t, "c"), s = a.call(t, "e")), f = {
	        get: e,
	        set: n,
	        configurable: r,
	        enumerable: s
	      }, c ? o(i(c), f) : f;
	    };
	  }, function (t, e, n) {

	    t.exports = function () {};
	  }, function (t, e, n) {

	    t.exports = n(7)() ? Object.assign : n(8);
	  }, function (t, e, n) {

	    t.exports = function () {
	      var t,
	          e = Object.assign;
	      return "function" == typeof e && (t = {
	        foo: "raz"
	      }, e(t, {
	        bar: "dwa"
	      }, {
	        trzy: "trzy"
	      }), t.foo + t.bar + t.trzy === "razdwatrzy");
	    };
	  }, function (t, e, n) {

	    var r = n(10),
	        o = n(14),
	        i = Math.max;

	    t.exports = function (t, e) {
	      var n,
	          u,
	          a,
	          s = i(arguments.length, 2);

	      for (t = Object(o(t)), a = function a(r) {
	        try {
	          t[r] = e[r];
	        } catch (t) {
	          n || (n = t);
	        }
	      }, u = 1; u < s; ++u) {
	        e = arguments[u], r(e).forEach(a);
	      }

	      if (void 0 !== n) throw n;
	      return t;
	    };
	  }, function (t, e, n) {

	    t.exports = function (t) {
	      return "function" == typeof t;
	    };
	  }, function (t, e, n) {

	    t.exports = n(11)() ? Object.keys : n(12);
	  }, function (t, e, n) {

	    t.exports = function () {
	      try {
	        return !0;
	      } catch (t) {
	        return !1;
	      }
	    };
	  }, function (t, e, n) {

	    var r = n(0),
	        o = Object.keys;

	    t.exports = function (t) {
	      return o(r(t) ? Object(t) : t);
	    };
	  }, function (t, e, n) {

	    var r = n(0),
	        o = Array.prototype.forEach,
	        i = Object.create,
	        u = function u(t, e) {
	      var n;

	      for (n in t) {
	        e[n] = t[n];
	      }
	    };

	    t.exports = function (t) {
	      var e = i(null);
	      return o.call(arguments, function (t) {
	        r(t) && u(Object(t), e);
	      }), e;
	    };
	  }, function (t, e, n) {

	    var r = n(0);

	    t.exports = function (t) {
	      if (!r(t)) throw new TypeError("Cannot use null or undefined");
	      return t;
	    };
	  }, function (t, e, n) {

	    t.exports = n(16)() ? String.prototype.contains : n(17);
	  }, function (t, e, n) {

	    var r = "razdwatrzy";

	    t.exports = function () {
	      return "function" == typeof r.contains && !0 === r.contains("dwa") && !1 === r.contains("foo");
	    };
	  }, function (t, e, n) {

	    var r = String.prototype.indexOf;

	    t.exports = function (t) {
	      return r.call(this, t, arguments[1]) > -1;
	    };
	  }, function (t, e, n) {

	    var r = {
	      object: !0,
	      symbol: !0
	    };

	    t.exports = function () {
	      if ("function" != typeof Symbol) return !1;

	      try {
	      } catch (t) {
	        return !1;
	      }

	      return !!r[_typeof(Symbol.iterator)] && !!r[_typeof(Symbol.toPrimitive)] && !!r[_typeof(Symbol.toStringTag)];
	    };
	  }, function (t, e, n) {

	    t.exports = function (t) {
	      return !!t && ("symbol" == _typeof(t) || !!t.constructor && "Symbol" === t.constructor.name && "Symbol" === t[t.constructor.toStringTag]);
	    };
	  }, function (t, e, n) {

	    var r,
	        o,
	        _i,
	        u,
	        a = n(4),
	        s = n(21),
	        c = Object.create,
	        f = Object.defineProperties,
	        l = Object.defineProperty,
	        v = Object.prototype,
	        d = c(null);

	    if ("function" == typeof Symbol) {
	      r = Symbol;

	      try {
	        String(r()), u = !0;
	      } catch (t) {}
	    }

	    var p = function () {
	      var t = c(null);
	      return function (e) {
	        for (var n, r, o = 0; t[e + (o || "")];) {
	          ++o;
	        }

	        return e += o || "", t[e] = !0, n = "@@" + e, l(v, n, a.gs(null, function (t) {
	          r || (r = !0, l(this, n, a(t)), r = !1);
	        })), n;
	      };
	    }();

	    _i = function i(t) {
	      if (this instanceof _i) throw new TypeError("Symbol is not a constructor");
	      return o(t);
	    }, t.exports = o = function t(e) {
	      var n;
	      if (this instanceof t) throw new TypeError("Symbol is not a constructor");
	      return u ? r(e) : (n = c(_i.prototype), e = void 0 === e ? "" : String(e), f(n, {
	        __description__: a("", e),
	        __name__: a("", p(e))
	      }));
	    }, f(o, {
	      for: a(function (t) {
	        return d[t] ? d[t] : d[t] = o(String(t));
	      }),
	      keyFor: a(function (t) {
	        var e;
	        s(t);

	        for (e in d) {
	          if (d[e] === t) return e;
	        }
	      }),
	      hasInstance: a("", r && r.hasInstance || o("hasInstance")),
	      isConcatSpreadable: a("", r && r.isConcatSpreadable || o("isConcatSpreadable")),
	      iterator: a("", r && r.iterator || o("iterator")),
	      match: a("", r && r.match || o("match")),
	      replace: a("", r && r.replace || o("replace")),
	      search: a("", r && r.search || o("search")),
	      species: a("", r && r.species || o("species")),
	      split: a("", r && r.split || o("split")),
	      toPrimitive: a("", r && r.toPrimitive || o("toPrimitive")),
	      toStringTag: a("", r && r.toStringTag || o("toStringTag")),
	      unscopables: a("", r && r.unscopables || o("unscopables"))
	    }), f(_i.prototype, {
	      constructor: a(o),
	      toString: a("", function () {
	        return this.__name__;
	      })
	    }), f(o.prototype, {
	      toString: a(function () {
	        return "Symbol (" + s(this).__description__ + ")";
	      }),
	      valueOf: a(function () {
	        return s(this);
	      })
	    }), l(o.prototype, o.toPrimitive, a("", function () {
	      var t = s(this);
	      return "symbol" == _typeof(t) ? t : t.toString();
	    })), l(o.prototype, o.toStringTag, a("c", "Symbol")), l(_i.prototype, o.toStringTag, a("c", o.prototype[o.toStringTag])), l(_i.prototype, o.toPrimitive, a("c", o.prototype[o.toPrimitive]));
	  }, function (t, e, n) {

	    var r = n(19);

	    t.exports = function (t) {
	      if (!r(t)) throw new TypeError(t + " is not a symbol");
	      return t;
	    };
	  }]);
	});

	var moment = undefined(Moment);
	var TIME_SERIES = {
	  day: function day(range$$1) {
	    return Array.from(range$$1.by('day')).map(function (d) {
	      return d.format('YYYY-MM-DDT00:00:00.000');
	    });
	  },
	  month: function month(range$$1) {
	    return Array.from(range$$1.snapTo('month').by('month')).map(function (d) {
	      return d.format('YYYY-MM-01T00:00:00.000');
	    });
	  },
	  hour: function hour(range$$1) {
	    return Array.from(range$$1.by('hour')).map(function (d) {
	      return d.format('YYYY-MM-DDTHH:00:00.000');
	    });
	  },
	  week: function week(range$$1) {
	    return Array.from(range$$1.snapTo('isoweek').by('week')).map(function (d) {
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

	      var range$$1 = moment.range(dateRange[0], dateRange[1]);

	      if (!TIME_SERIES[timeDimension.granularity]) {
	        throw new Error("Unsupported time granularity: ".concat(timeDimension.granularity));
	      }

	      return TIME_SERIES[timeDimension.granularity](range$$1);
	    }
	  }, {
	    key: "pivot",
	    value: function pivot(pivotConfig) {
	      var _this2 = this;

	      pivotConfig = this.normalizePivotConfig(pivotConfig);
	      var groupByXAxis = groupBy(function (_ref3) {
	        var xValues = _ref3.xValues;
	        return _this2.axisValuesString(xValues);
	      });

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
	          };

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
	      var allYValues = pipe(map(function (_ref6) {
	        var _ref7 = _slicedToArray(_ref6, 2),
	            xValuesString = _ref7[0],
	            rows = _ref7[1];

	        return unnest(rows.map(function (_ref8) {
	          var row = _ref8.row;
	          return _this2.axisValues(pivotConfig.y)(row);
	        }));
	      }), unnest, uniq)(xGrouped);
	      return xGrouped.map(function (_ref9) {
	        var _ref10 = _slicedToArray(_ref9, 2),
	            xValuesString = _ref10[0],
	            rows = _ref10[1];

	        var xValues = rows[0].xValues;
	        var yGrouped = pipe(map(function (_ref11) {
	          var row = _ref11.row;
	          return _this2.axisValues(pivotConfig.y)(row).map(function (yValues) {
	            return {
	              yValues: yValues,
	              row: row
	            };
	          });
	        }), unnest, groupBy(function (_ref12) {
	          var yValues = _ref12.yValues;
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
	      return pipe(map(this.axisValues(pivotConfig.y)), unnest, uniq)(this.loadResponse.data).map(function (axisValues) {
	        return {
	          title: _this4.axisValuesString(pivotConfig.y.find(function (d) {
	            return d === 'measures';
	          }) ? dropLast$1(1, axisValues).concat(_this4.loadResponse.annotation.measures[ResultSet.measureFromAxis(axisValues)].title) : axisValues, ', '),
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
	        regeneratorRuntime.mark(function _callee() {
	          var response, body;
	          return regeneratorRuntime.wrap(function _callee$(_context) {
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
	        return _this.request("/load?query=".concat(JSON.stringify(query)));
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

	var index = (function (apiToken) {
	  return new CubejsApi(apiToken);
	});

	return index;

})));
