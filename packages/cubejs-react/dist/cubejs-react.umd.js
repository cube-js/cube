(function (global, factory) {
	typeof exports === 'object' && typeof module !== 'undefined' ? factory(exports, require('react'), require('prop-types')) :
	typeof define === 'function' && define.amd ? define(['exports', 'react', 'prop-types'], factory) :
	(factory((global.cubejsReact = {}),global.React,global.PropTypes));
}(this, (function (exports,React,PropTypes) { 'use strict';

	React = React && React.hasOwnProperty('default') ? React['default'] : React;

	function createCommonjsModule(fn, module) {
		return module = { exports: {} }, fn(module, module.exports), module.exports;
	}

	var _global = createCommonjsModule(function (module) {
	// https://github.com/zloirock/core-js/issues/86#issuecomment-115759028
	var global = module.exports = typeof window != 'undefined' && window.Math == Math
	  ? window : typeof self != 'undefined' && self.Math == Math ? self
	  // eslint-disable-next-line no-new-func
	  : Function('return this')();
	if (typeof __g == 'number') __g = global; // eslint-disable-line no-undef
	});

	var _core = createCommonjsModule(function (module) {
	var core = module.exports = { version: '2.5.7' };
	if (typeof __e == 'number') __e = core; // eslint-disable-line no-undef
	});
	var _core_1 = _core.version;

	var _isObject = function (it) {
	  return typeof it === 'object' ? it !== null : typeof it === 'function';
	};

	var _anObject = function (it) {
	  if (!_isObject(it)) throw TypeError(it + ' is not an object!');
	  return it;
	};

	var _fails = function (exec) {
	  try {
	    return !!exec();
	  } catch (e) {
	    return true;
	  }
	};

	// Thank's IE8 for his funny defineProperty
	var _descriptors = !_fails(function () {
	  return Object.defineProperty({}, 'a', { get: function () { return 7; } }).a != 7;
	});

	var document$1 = _global.document;
	// typeof document.createElement is 'object' in old IE
	var is = _isObject(document$1) && _isObject(document$1.createElement);
	var _domCreate = function (it) {
	  return is ? document$1.createElement(it) : {};
	};

	var _ie8DomDefine = !_descriptors && !_fails(function () {
	  return Object.defineProperty(_domCreate('div'), 'a', { get: function () { return 7; } }).a != 7;
	});

	// 7.1.1 ToPrimitive(input [, PreferredType])

	// instead of the ES6 spec version, we didn't implement @@toPrimitive case
	// and the second argument - flag - preferred type is a string
	var _toPrimitive = function (it, S) {
	  if (!_isObject(it)) return it;
	  var fn, val;
	  if (S && typeof (fn = it.toString) == 'function' && !_isObject(val = fn.call(it))) return val;
	  if (typeof (fn = it.valueOf) == 'function' && !_isObject(val = fn.call(it))) return val;
	  if (!S && typeof (fn = it.toString) == 'function' && !_isObject(val = fn.call(it))) return val;
	  throw TypeError("Can't convert object to primitive value");
	};

	var dP = Object.defineProperty;

	var f = _descriptors ? Object.defineProperty : function defineProperty(O, P, Attributes) {
	  _anObject(O);
	  P = _toPrimitive(P, true);
	  _anObject(Attributes);
	  if (_ie8DomDefine) try {
	    return dP(O, P, Attributes);
	  } catch (e) { /* empty */ }
	  if ('get' in Attributes || 'set' in Attributes) throw TypeError('Accessors not supported!');
	  if ('value' in Attributes) O[P] = Attributes.value;
	  return O;
	};

	var _objectDp = {
		f: f
	};

	var _propertyDesc = function (bitmap, value) {
	  return {
	    enumerable: !(bitmap & 1),
	    configurable: !(bitmap & 2),
	    writable: !(bitmap & 4),
	    value: value
	  };
	};

	var _hide = _descriptors ? function (object, key, value) {
	  return _objectDp.f(object, key, _propertyDesc(1, value));
	} : function (object, key, value) {
	  object[key] = value;
	  return object;
	};

	var hasOwnProperty = {}.hasOwnProperty;
	var _has = function (it, key) {
	  return hasOwnProperty.call(it, key);
	};

	var id = 0;
	var px = Math.random();
	var _uid = function (key) {
	  return 'Symbol('.concat(key === undefined ? '' : key, ')_', (++id + px).toString(36));
	};

	var _redefine = createCommonjsModule(function (module) {
	var SRC = _uid('src');
	var TO_STRING = 'toString';
	var $toString = Function[TO_STRING];
	var TPL = ('' + $toString).split(TO_STRING);

	_core.inspectSource = function (it) {
	  return $toString.call(it);
	};

	(module.exports = function (O, key, val, safe) {
	  var isFunction = typeof val == 'function';
	  if (isFunction) _has(val, 'name') || _hide(val, 'name', key);
	  if (O[key] === val) return;
	  if (isFunction) _has(val, SRC) || _hide(val, SRC, O[key] ? '' + O[key] : TPL.join(String(key)));
	  if (O === _global) {
	    O[key] = val;
	  } else if (!safe) {
	    delete O[key];
	    _hide(O, key, val);
	  } else if (O[key]) {
	    O[key] = val;
	  } else {
	    _hide(O, key, val);
	  }
	// add fake Function#toString for correct work wrapped methods / constructors with methods like LoDash isNative
	})(Function.prototype, TO_STRING, function toString() {
	  return typeof this == 'function' && this[SRC] || $toString.call(this);
	});
	});

	var _aFunction = function (it) {
	  if (typeof it != 'function') throw TypeError(it + ' is not a function!');
	  return it;
	};

	// optional / simple context binding

	var _ctx = function (fn, that, length) {
	  _aFunction(fn);
	  if (that === undefined) return fn;
	  switch (length) {
	    case 1: return function (a) {
	      return fn.call(that, a);
	    };
	    case 2: return function (a, b) {
	      return fn.call(that, a, b);
	    };
	    case 3: return function (a, b, c) {
	      return fn.call(that, a, b, c);
	    };
	  }
	  return function (/* ...args */) {
	    return fn.apply(that, arguments);
	  };
	};

	var PROTOTYPE = 'prototype';

	var $export = function (type, name, source) {
	  var IS_FORCED = type & $export.F;
	  var IS_GLOBAL = type & $export.G;
	  var IS_STATIC = type & $export.S;
	  var IS_PROTO = type & $export.P;
	  var IS_BIND = type & $export.B;
	  var target = IS_GLOBAL ? _global : IS_STATIC ? _global[name] || (_global[name] = {}) : (_global[name] || {})[PROTOTYPE];
	  var exports = IS_GLOBAL ? _core : _core[name] || (_core[name] = {});
	  var expProto = exports[PROTOTYPE] || (exports[PROTOTYPE] = {});
	  var key, own, out, exp;
	  if (IS_GLOBAL) source = name;
	  for (key in source) {
	    // contains in native
	    own = !IS_FORCED && target && target[key] !== undefined;
	    // export native or passed
	    out = (own ? target : source)[key];
	    // bind timers to global for call from export context
	    exp = IS_BIND && own ? _ctx(out, _global) : IS_PROTO && typeof out == 'function' ? _ctx(Function.call, out) : out;
	    // extend global
	    if (target) _redefine(target, key, out, type & $export.U);
	    // export
	    if (exports[key] != out) _hide(exports, key, exp);
	    if (IS_PROTO && expProto[key] != out) expProto[key] = out;
	  }
	};
	_global.core = _core;
	// type bitmap
	$export.F = 1;   // forced
	$export.G = 2;   // global
	$export.S = 4;   // static
	$export.P = 8;   // proto
	$export.B = 16;  // bind
	$export.W = 32;  // wrap
	$export.U = 64;  // safe
	$export.R = 128; // real proto method for `library`
	var _export = $export;

	var toString = {}.toString;

	var _cof = function (it) {
	  return toString.call(it).slice(8, -1);
	};

	// fallback for non-array-like ES3 and non-enumerable old V8 strings

	// eslint-disable-next-line no-prototype-builtins
	var _iobject = Object('z').propertyIsEnumerable(0) ? Object : function (it) {
	  return _cof(it) == 'String' ? it.split('') : Object(it);
	};

	// 7.2.1 RequireObjectCoercible(argument)
	var _defined = function (it) {
	  if (it == undefined) throw TypeError("Can't call method on  " + it);
	  return it;
	};

	// 7.1.13 ToObject(argument)

	var _toObject = function (it) {
	  return Object(_defined(it));
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

	// 7.2.2 IsArray(argument)

	var _isArray = Array.isArray || function isArray(arg) {
	  return _cof(arg) == 'Array';
	};

	var _library = false;

	var _shared = createCommonjsModule(function (module) {
	var SHARED = '__core-js_shared__';
	var store = _global[SHARED] || (_global[SHARED] = {});

	(module.exports = function (key, value) {
	  return store[key] || (store[key] = value !== undefined ? value : {});
	})('versions', []).push({
	  version: _core.version,
	  mode: _library ? 'pure' : 'global',
	  copyright: '© 2018 Denis Pushkarev (zloirock.ru)'
	});
	});

	var _wks = createCommonjsModule(function (module) {
	var store = _shared('wks');

	var Symbol = _global.Symbol;
	var USE_SYMBOL = typeof Symbol == 'function';

	var $exports = module.exports = function (name) {
	  return store[name] || (store[name] =
	    USE_SYMBOL && Symbol[name] || (USE_SYMBOL ? Symbol : _uid)('Symbol.' + name));
	};

	$exports.store = store;
	});

	var SPECIES = _wks('species');

	var _arraySpeciesConstructor = function (original) {
	  var C;
	  if (_isArray(original)) {
	    C = original.constructor;
	    // cross-realm fallback
	    if (typeof C == 'function' && (C === Array || _isArray(C.prototype))) C = undefined;
	    if (_isObject(C)) {
	      C = C[SPECIES];
	      if (C === null) C = undefined;
	    }
	  } return C === undefined ? Array : C;
	};

	// 9.4.2.3 ArraySpeciesCreate(originalArray, length)


	var _arraySpeciesCreate = function (original, length) {
	  return new (_arraySpeciesConstructor(original))(length);
	};

	// 0 -> Array#forEach
	// 1 -> Array#map
	// 2 -> Array#filter
	// 3 -> Array#some
	// 4 -> Array#every
	// 5 -> Array#find
	// 6 -> Array#findIndex





	var _arrayMethods = function (TYPE, $create) {
	  var IS_MAP = TYPE == 1;
	  var IS_FILTER = TYPE == 2;
	  var IS_SOME = TYPE == 3;
	  var IS_EVERY = TYPE == 4;
	  var IS_FIND_INDEX = TYPE == 6;
	  var NO_HOLES = TYPE == 5 || IS_FIND_INDEX;
	  var create = $create || _arraySpeciesCreate;
	  return function ($this, callbackfn, that) {
	    var O = _toObject($this);
	    var self = _iobject(O);
	    var f = _ctx(callbackfn, that, 3);
	    var length = _toLength(self.length);
	    var index = 0;
	    var result = IS_MAP ? create($this, length) : IS_FILTER ? create($this, 0) : undefined;
	    var val, res;
	    for (;length > index; index++) if (NO_HOLES || index in self) {
	      val = self[index];
	      res = f(val, index, O);
	      if (TYPE) {
	        if (IS_MAP) result[index] = res;   // map
	        else if (res) switch (TYPE) {
	          case 3: return true;             // some
	          case 5: return val;              // find
	          case 6: return index;            // findIndex
	          case 2: result.push(val);        // filter
	        } else if (IS_EVERY) return false; // every
	      }
	    }
	    return IS_FIND_INDEX ? -1 : IS_SOME || IS_EVERY ? IS_EVERY : result;
	  };
	};

	var _strictMethod = function (method, arg) {
	  return !!method && _fails(function () {
	    // eslint-disable-next-line no-useless-call
	    arg ? method.call(null, function () { /* empty */ }, 1) : method.call(null);
	  });
	};

	var $map = _arrayMethods(1);

	_export(_export.P + _export.F * !_strictMethod([].map, true), 'Array', {
	  // 22.1.3.15 / 15.4.4.19 Array.prototype.map(callbackfn [, thisArg])
	  map: function map(callbackfn /* , thisArg */) {
	    return $map(this, callbackfn, arguments[1]);
	  }
	});

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

	function _extends() {
	  _extends = Object.assign || function (target) {
	    for (var i = 1; i < arguments.length; i++) {
	      var source = arguments[i];

	      for (var key in source) {
	        if (Object.prototype.hasOwnProperty.call(source, key)) {
	          target[key] = source[key];
	        }
	      }
	    }

	    return target;
	  };

	  return _extends.apply(this, arguments);
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

	function _inherits(subClass, superClass) {
	  if (typeof superClass !== "function" && superClass !== null) {
	    throw new TypeError("Super expression must either be null or a function");
	  }

	  subClass.prototype = Object.create(superClass && superClass.prototype, {
	    constructor: {
	      value: subClass,
	      writable: true,
	      configurable: true
	    }
	  });
	  if (superClass) _setPrototypeOf(subClass, superClass);
	}

	function _getPrototypeOf(o) {
	  _getPrototypeOf = Object.setPrototypeOf ? Object.getPrototypeOf : function _getPrototypeOf(o) {
	    return o.__proto__ || Object.getPrototypeOf(o);
	  };
	  return _getPrototypeOf(o);
	}

	function _setPrototypeOf(o, p) {
	  _setPrototypeOf = Object.setPrototypeOf || function _setPrototypeOf(o, p) {
	    o.__proto__ = p;
	    return o;
	  };

	  return _setPrototypeOf(o, p);
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

	function _assertThisInitialized(self) {
	  if (self === void 0) {
	    throw new ReferenceError("this hasn't been initialised - super() hasn't been called");
	  }

	  return self;
	}

	function _possibleConstructorReturn(self, call) {
	  if (call && (typeof call === "object" || typeof call === "function")) {
	    return call;
	  }

	  return _assertThisInitialized(self);
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

	// getting tag from 19.1.3.6 Object.prototype.toString()

	var TAG = _wks('toStringTag');
	// ES3 wrong here
	var ARG = _cof(function () { return arguments; }()) == 'Arguments';

	// fallback for IE11 Script Access Denied error
	var tryGet = function (it, key) {
	  try {
	    return it[key];
	  } catch (e) { /* empty */ }
	};

	var _classof = function (it) {
	  var O, T, B;
	  return it === undefined ? 'Undefined' : it === null ? 'Null'
	    // @@toStringTag case
	    : typeof (T = tryGet(O = Object(it), TAG)) == 'string' ? T
	    // builtinTag case
	    : ARG ? _cof(O)
	    // ES3 arguments fallback
	    : (B = _cof(O)) == 'Object' && typeof O.callee == 'function' ? 'Arguments' : B;
	};

	var _anInstance = function (it, Constructor, name, forbiddenField) {
	  if (!(it instanceof Constructor) || (forbiddenField !== undefined && forbiddenField in it)) {
	    throw TypeError(name + ': incorrect invocation!');
	  } return it;
	};

	// call something on iterator step with safe closing on error

	var _iterCall = function (iterator, fn, value, entries) {
	  try {
	    return entries ? fn(_anObject(value)[0], value[1]) : fn(value);
	  // 7.4.6 IteratorClose(iterator, completion)
	  } catch (e) {
	    var ret = iterator['return'];
	    if (ret !== undefined) _anObject(ret.call(iterator));
	    throw e;
	  }
	};

	var _iterators = {};

	// check on default Array iterator

	var ITERATOR = _wks('iterator');
	var ArrayProto = Array.prototype;

	var _isArrayIter = function (it) {
	  return it !== undefined && (_iterators.Array === it || ArrayProto[ITERATOR] === it);
	};

	var ITERATOR$1 = _wks('iterator');

	var core_getIteratorMethod = _core.getIteratorMethod = function (it) {
	  if (it != undefined) return it[ITERATOR$1]
	    || it['@@iterator']
	    || _iterators[_classof(it)];
	};

	var _forOf = createCommonjsModule(function (module) {
	var BREAK = {};
	var RETURN = {};
	var exports = module.exports = function (iterable, entries, fn, that, ITERATOR) {
	  var iterFn = ITERATOR ? function () { return iterable; } : core_getIteratorMethod(iterable);
	  var f = _ctx(fn, that, entries ? 2 : 1);
	  var index = 0;
	  var length, step, iterator, result;
	  if (typeof iterFn != 'function') throw TypeError(iterable + ' is not iterable!');
	  // fast case for arrays with default iterator
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

	// 7.3.20 SpeciesConstructor(O, defaultConstructor)


	var SPECIES$1 = _wks('species');
	var _speciesConstructor = function (O, D) {
	  var C = _anObject(O).constructor;
	  var S;
	  return C === undefined || (S = _anObject(C)[SPECIES$1]) == undefined ? D : _aFunction(S);
	};

	// fast apply, http://jsperf.lnkit.com/fast-apply/5
	var _invoke = function (fn, args, that) {
	  var un = that === undefined;
	  switch (args.length) {
	    case 0: return un ? fn()
	                      : fn.call(that);
	    case 1: return un ? fn(args[0])
	                      : fn.call(that, args[0]);
	    case 2: return un ? fn(args[0], args[1])
	                      : fn.call(that, args[0], args[1]);
	    case 3: return un ? fn(args[0], args[1], args[2])
	                      : fn.call(that, args[0], args[1], args[2]);
	    case 4: return un ? fn(args[0], args[1], args[2], args[3])
	                      : fn.call(that, args[0], args[1], args[2], args[3]);
	  } return fn.apply(that, args);
	};

	var document$2 = _global.document;
	var _html = document$2 && document$2.documentElement;

	var process = _global.process;
	var setTask = _global.setImmediate;
	var clearTask = _global.clearImmediate;
	var MessageChannel = _global.MessageChannel;
	var Dispatch = _global.Dispatch;
	var counter = 0;
	var queue = {};
	var ONREADYSTATECHANGE = 'onreadystatechange';
	var defer, channel, port;
	var run = function () {
	  var id = +this;
	  // eslint-disable-next-line no-prototype-builtins
	  if (queue.hasOwnProperty(id)) {
	    var fn = queue[id];
	    delete queue[id];
	    fn();
	  }
	};
	var listener = function (event) {
	  run.call(event.data);
	};
	// Node.js 0.9+ & IE10+ has setImmediate, otherwise:
	if (!setTask || !clearTask) {
	  setTask = function setImmediate(fn) {
	    var args = [];
	    var i = 1;
	    while (arguments.length > i) args.push(arguments[i++]);
	    queue[++counter] = function () {
	      // eslint-disable-next-line no-new-func
	      _invoke(typeof fn == 'function' ? fn : Function(fn), args);
	    };
	    defer(counter);
	    return counter;
	  };
	  clearTask = function clearImmediate(id) {
	    delete queue[id];
	  };
	  // Node.js 0.8-
	  if (_cof(process) == 'process') {
	    defer = function (id) {
	      process.nextTick(_ctx(run, id, 1));
	    };
	  // Sphere (JS game engine) Dispatch API
	  } else if (Dispatch && Dispatch.now) {
	    defer = function (id) {
	      Dispatch.now(_ctx(run, id, 1));
	    };
	  // Browsers with MessageChannel, includes WebWorkers
	  } else if (MessageChannel) {
	    channel = new MessageChannel();
	    port = channel.port2;
	    channel.port1.onmessage = listener;
	    defer = _ctx(port.postMessage, port, 1);
	  // Browsers with postMessage, skip WebWorkers
	  // IE8 has postMessage, but it's sync & typeof its postMessage is 'object'
	  } else if (_global.addEventListener && typeof postMessage == 'function' && !_global.importScripts) {
	    defer = function (id) {
	      _global.postMessage(id + '', '*');
	    };
	    _global.addEventListener('message', listener, false);
	  // IE8-
	  } else if (ONREADYSTATECHANGE in _domCreate('script')) {
	    defer = function (id) {
	      _html.appendChild(_domCreate('script'))[ONREADYSTATECHANGE] = function () {
	        _html.removeChild(this);
	        run.call(id);
	      };
	    };
	  // Rest old browsers
	  } else {
	    defer = function (id) {
	      setTimeout(_ctx(run, id, 1), 0);
	    };
	  }
	}
	var _task = {
	  set: setTask,
	  clear: clearTask
	};

	var macrotask = _task.set;
	var Observer = _global.MutationObserver || _global.WebKitMutationObserver;
	var process$1 = _global.process;
	var Promise$1 = _global.Promise;
	var isNode = _cof(process$1) == 'process';

	var _microtask = function () {
	  var head, last, notify;

	  var flush = function () {
	    var parent, fn;
	    if (isNode && (parent = process$1.domain)) parent.exit();
	    while (head) {
	      fn = head.fn;
	      head = head.next;
	      try {
	        fn();
	      } catch (e) {
	        if (head) notify();
	        else last = undefined;
	        throw e;
	      }
	    } last = undefined;
	    if (parent) parent.enter();
	  };

	  // Node.js
	  if (isNode) {
	    notify = function () {
	      process$1.nextTick(flush);
	    };
	  // browsers with MutationObserver, except iOS Safari - https://github.com/zloirock/core-js/issues/339
	  } else if (Observer && !(_global.navigator && _global.navigator.standalone)) {
	    var toggle = true;
	    var node = document.createTextNode('');
	    new Observer(flush).observe(node, { characterData: true }); // eslint-disable-line no-new
	    notify = function () {
	      node.data = toggle = !toggle;
	    };
	  // environments with maybe non-completely correct, but existent Promise
	  } else if (Promise$1 && Promise$1.resolve) {
	    // Promise.resolve without an argument throws an error in LG WebOS 2
	    var promise = Promise$1.resolve(undefined);
	    notify = function () {
	      promise.then(flush);
	    };
	  // for other environments - macrotask based on:
	  // - setImmediate
	  // - MessageChannel
	  // - window.postMessag
	  // - onreadystatechange
	  // - setTimeout
	  } else {
	    notify = function () {
	      // strange IE + webpack dev server bug - use .call(global)
	      macrotask.call(_global, flush);
	    };
	  }

	  return function (fn) {
	    var task = { fn: fn, next: undefined };
	    if (last) last.next = task;
	    if (!head) {
	      head = task;
	      notify();
	    } last = task;
	  };
	};

	// 25.4.1.5 NewPromiseCapability(C)


	function PromiseCapability(C) {
	  var resolve, reject;
	  this.promise = new C(function ($$resolve, $$reject) {
	    if (resolve !== undefined || reject !== undefined) throw TypeError('Bad Promise constructor');
	    resolve = $$resolve;
	    reject = $$reject;
	  });
	  this.resolve = _aFunction(resolve);
	  this.reject = _aFunction(reject);
	}

	var f$1 = function (C) {
	  return new PromiseCapability(C);
	};

	var _newPromiseCapability = {
		f: f$1
	};

	var _perform = function (exec) {
	  try {
	    return { e: false, v: exec() };
	  } catch (e) {
	    return { e: true, v: e };
	  }
	};

	var navigator = _global.navigator;

	var _userAgent = navigator && navigator.userAgent || '';

	var _promiseResolve = function (C, x) {
	  _anObject(C);
	  if (_isObject(x) && x.constructor === C) return x;
	  var promiseCapability = _newPromiseCapability.f(C);
	  var resolve = promiseCapability.resolve;
	  resolve(x);
	  return promiseCapability.promise;
	};

	var _redefineAll = function (target, src, safe) {
	  for (var key in src) _redefine(target, key, src[key], safe);
	  return target;
	};

	var def = _objectDp.f;

	var TAG$1 = _wks('toStringTag');

	var _setToStringTag = function (it, tag, stat) {
	  if (it && !_has(it = stat ? it : it.prototype, TAG$1)) def(it, TAG$1, { configurable: true, value: tag });
	};

	var SPECIES$2 = _wks('species');

	var _setSpecies = function (KEY) {
	  var C = _global[KEY];
	  if (_descriptors && C && !C[SPECIES$2]) _objectDp.f(C, SPECIES$2, {
	    configurable: true,
	    get: function () { return this; }
	  });
	};

	var ITERATOR$2 = _wks('iterator');
	var SAFE_CLOSING = false;

	try {
	  var riter = [7][ITERATOR$2]();
	  riter['return'] = function () { SAFE_CLOSING = true; };
	} catch (e) { /* empty */ }

	var _iterDetect = function (exec, skipClosing) {
	  if (!skipClosing && !SAFE_CLOSING) return false;
	  var safe = false;
	  try {
	    var arr = [7];
	    var iter = arr[ITERATOR$2]();
	    iter.next = function () { return { done: safe = true }; };
	    arr[ITERATOR$2] = function () { return iter; };
	    exec(arr);
	  } catch (e) { /* empty */ }
	  return safe;
	};

	var task = _task.set;
	var microtask = _microtask();




	var PROMISE = 'Promise';
	var TypeError$1 = _global.TypeError;
	var process$2 = _global.process;
	var versions = process$2 && process$2.versions;
	var v8 = versions && versions.v8 || '';
	var $Promise = _global[PROMISE];
	var isNode$1 = _classof(process$2) == 'process';
	var empty = function () { /* empty */ };
	var Internal, newGenericPromiseCapability, OwnPromiseCapability, Wrapper;
	var newPromiseCapability = newGenericPromiseCapability = _newPromiseCapability.f;

	var USE_NATIVE = !!function () {
	  try {
	    // correct subclassing with @@species support
	    var promise = $Promise.resolve(1);
	    var FakePromise = (promise.constructor = {})[_wks('species')] = function (exec) {
	      exec(empty, empty);
	    };
	    // unhandled rejections tracking support, NodeJS Promise without it fails @@species test
	    return (isNode$1 || typeof PromiseRejectionEvent == 'function')
	      && promise.then(empty) instanceof FakePromise
	      // v8 6.6 (Node 10 and Chrome 66) have a bug with resolving custom thenables
	      // https://bugs.chromium.org/p/chromium/issues/detail?id=830565
	      // we can't detect it synchronously, so just check versions
	      && v8.indexOf('6.6') !== 0
	      && _userAgent.indexOf('Chrome/66') === -1;
	  } catch (e) { /* empty */ }
	}();

	// helpers
	var isThenable = function (it) {
	  var then;
	  return _isObject(it) && typeof (then = it.then) == 'function' ? then : false;
	};
	var notify = function (promise, isReject) {
	  if (promise._n) return;
	  promise._n = true;
	  var chain = promise._c;
	  microtask(function () {
	    var value = promise._v;
	    var ok = promise._s == 1;
	    var i = 0;
	    var run = function (reaction) {
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
	          if (handler === true) result = value;
	          else {
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
	    while (chain.length > i) run(chain[i++]); // variable length - can't use forEach
	    promise._c = [];
	    promise._n = false;
	    if (isReject && !promise._h) onUnhandled(promise);
	  });
	};
	var onUnhandled = function (promise) {
	  task.call(_global, function () {
	    var value = promise._v;
	    var unhandled = isUnhandled(promise);
	    var result, handler, console;
	    if (unhandled) {
	      result = _perform(function () {
	        if (isNode$1) {
	          process$2.emit('unhandledRejection', value, promise);
	        } else if (handler = _global.onunhandledrejection) {
	          handler({ promise: promise, reason: value });
	        } else if ((console = _global.console) && console.error) {
	          console.error('Unhandled promise rejection', value);
	        }
	      });
	      // Browsers should not trigger `rejectionHandled` event if it was handled here, NodeJS - should
	      promise._h = isNode$1 || isUnhandled(promise) ? 2 : 1;
	    } promise._a = undefined;
	    if (unhandled && result.e) throw result.v;
	  });
	};
	var isUnhandled = function (promise) {
	  return promise._h !== 1 && (promise._a || promise._c).length === 0;
	};
	var onHandleUnhandled = function (promise) {
	  task.call(_global, function () {
	    var handler;
	    if (isNode$1) {
	      process$2.emit('rejectionHandled', promise);
	    } else if (handler = _global.onrejectionhandled) {
	      handler({ promise: promise, reason: promise._v });
	    }
	  });
	};
	var $reject = function (value) {
	  var promise = this;
	  if (promise._d) return;
	  promise._d = true;
	  promise = promise._w || promise; // unwrap
	  promise._v = value;
	  promise._s = 2;
	  if (!promise._a) promise._a = promise._c.slice();
	  notify(promise, true);
	};
	var $resolve = function (value) {
	  var promise = this;
	  var then;
	  if (promise._d) return;
	  promise._d = true;
	  promise = promise._w || promise; // unwrap
	  try {
	    if (promise === value) throw TypeError$1("Promise can't be resolved itself");
	    if (then = isThenable(value)) {
	      microtask(function () {
	        var wrapper = { _w: promise, _d: false }; // wrap
	        try {
	          then.call(value, _ctx($resolve, wrapper, 1), _ctx($reject, wrapper, 1));
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
	    $reject.call({ _w: promise, _d: false }, e); // wrap
	  }
	};

	// constructor polyfill
	if (!USE_NATIVE) {
	  // 25.4.3.1 Promise(executor)
	  $Promise = function Promise(executor) {
	    _anInstance(this, $Promise, PROMISE, '_h');
	    _aFunction(executor);
	    Internal.call(this);
	    try {
	      executor(_ctx($resolve, this, 1), _ctx($reject, this, 1));
	    } catch (err) {
	      $reject.call(this, err);
	    }
	  };
	  // eslint-disable-next-line no-unused-vars
	  Internal = function Promise(executor) {
	    this._c = [];             // <- awaiting reactions
	    this._a = undefined;      // <- checked in isUnhandled reactions
	    this._s = 0;              // <- state
	    this._d = false;          // <- done
	    this._v = undefined;      // <- value
	    this._h = 0;              // <- rejection state, 0 - default, 1 - handled, 2 - unhandled
	    this._n = false;          // <- notify
	  };
	  Internal.prototype = _redefineAll($Promise.prototype, {
	    // 25.4.5.3 Promise.prototype.then(onFulfilled, onRejected)
	    then: function then(onFulfilled, onRejected) {
	      var reaction = newPromiseCapability(_speciesConstructor(this, $Promise));
	      reaction.ok = typeof onFulfilled == 'function' ? onFulfilled : true;
	      reaction.fail = typeof onRejected == 'function' && onRejected;
	      reaction.domain = isNode$1 ? process$2.domain : undefined;
	      this._c.push(reaction);
	      if (this._a) this._a.push(reaction);
	      if (this._s) notify(this, false);
	      return reaction.promise;
	    },
	    // 25.4.5.1 Promise.prototype.catch(onRejected)
	    'catch': function (onRejected) {
	      return this.then(undefined, onRejected);
	    }
	  });
	  OwnPromiseCapability = function () {
	    var promise = new Internal();
	    this.promise = promise;
	    this.resolve = _ctx($resolve, promise, 1);
	    this.reject = _ctx($reject, promise, 1);
	  };
	  _newPromiseCapability.f = newPromiseCapability = function (C) {
	    return C === $Promise || C === Wrapper
	      ? new OwnPromiseCapability(C)
	      : newGenericPromiseCapability(C);
	  };
	}

	_export(_export.G + _export.W + _export.F * !USE_NATIVE, { Promise: $Promise });
	_setToStringTag($Promise, PROMISE);
	_setSpecies(PROMISE);
	Wrapper = _core[PROMISE];

	// statics
	_export(_export.S + _export.F * !USE_NATIVE, PROMISE, {
	  // 25.4.4.5 Promise.reject(r)
	  reject: function reject(r) {
	    var capability = newPromiseCapability(this);
	    var $$reject = capability.reject;
	    $$reject(r);
	    return capability.promise;
	  }
	});
	_export(_export.S + _export.F * (!USE_NATIVE), PROMISE, {
	  // 25.4.4.6 Promise.resolve(x)
	  resolve: function resolve(x) {
	    return _promiseResolve(this, x);
	  }
	});
	_export(_export.S + _export.F * !(USE_NATIVE && _iterDetect(function (iter) {
	  $Promise.all(iter)['catch'](empty);
	})), PROMISE, {
	  // 25.4.4.1 Promise.all(iterable)
	  all: function all(iterable) {
	    var C = this;
	    var capability = newPromiseCapability(C);
	    var resolve = capability.resolve;
	    var reject = capability.reject;
	    var result = _perform(function () {
	      var values = [];
	      var index = 0;
	      var remaining = 1;
	      _forOf(iterable, false, function (promise) {
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
	    var result = _perform(function () {
	      _forOf(iterable, false, function (promise) {
	        C.resolve(promise).then(capability.resolve, reject);
	      });
	    });
	    if (result.e) reject(result.v);
	    return capability.promise;
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
	    return a < 0xd800 || a > 0xdbff || i + 1 === l || (b = s.charCodeAt(i + 1)) < 0xdc00 || b > 0xdfff
	      ? TO_STRING ? s.charAt(i) : a
	      : TO_STRING ? s.slice(i, i + 2) : (a - 0xd800 << 10) + (b - 0xdc00) + 0x10000;
	  };
	};

	// to indexed object, toObject with fallback for non-array-like ES3 strings


	var _toIobject = function (it) {
	  return _iobject(_defined(it));
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
	    var value;
	    // Array#includes uses SameValueZero equality algorithm
	    // eslint-disable-next-line no-self-compare
	    if (IS_INCLUDES && el != el) while (length > index) {
	      value = O[index++];
	      // eslint-disable-next-line no-self-compare
	      if (value != value) return true;
	    // Array#indexOf ignores holes, Array#includes - not
	    } else for (;length > index; index++) if (IS_INCLUDES || index in O) {
	      if (O[index] === el) return IS_INCLUDES || index || 0;
	    } return !IS_INCLUDES && -1;
	  };
	};

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
	  for (key in O) if (key != IE_PROTO) _has(O, key) && result.push(key);
	  // Don't enum bug & hidden keys
	  while (names.length > i) if (_has(O, key = names[i++])) {
	    ~arrayIndexOf(result, key) || result.push(key);
	  }
	  return result;
	};

	// IE 8- don't enum bug keys
	var _enumBugKeys = (
	  'constructor,hasOwnProperty,isPrototypeOf,propertyIsEnumerable,toLocaleString,toString,valueOf'
	).split(',');

	// 19.1.2.14 / 15.2.3.14 Object.keys(O)



	var _objectKeys = Object.keys || function keys(O) {
	  return _objectKeysInternal(O, _enumBugKeys);
	};

	var _objectDps = _descriptors ? Object.defineProperties : function defineProperties(O, Properties) {
	  _anObject(O);
	  var keys = _objectKeys(Properties);
	  var length = keys.length;
	  var i = 0;
	  var P;
	  while (length > i) _objectDp.f(O, P = keys[i++], Properties[P]);
	  return O;
	};

	// 19.1.2.2 / 15.2.3.5 Object.create(O [, Properties])



	var IE_PROTO$1 = _sharedKey('IE_PROTO');
	var Empty = function () { /* empty */ };
	var PROTOTYPE$1 = 'prototype';

	// Create object with fake `null` prototype: use iframe Object with cleared prototype
	var createDict = function () {
	  // Thrash, waste and sodomy: IE GC bug
	  var iframe = _domCreate('iframe');
	  var i = _enumBugKeys.length;
	  var lt = '<';
	  var gt = '>';
	  var iframeDocument;
	  iframe.style.display = 'none';
	  _html.appendChild(iframe);
	  iframe.src = 'javascript:'; // eslint-disable-line no-script-url
	  // createDict = iframe.contentWindow.Object;
	  // html.removeChild(iframe);
	  iframeDocument = iframe.contentWindow.document;
	  iframeDocument.open();
	  iframeDocument.write(lt + 'script' + gt + 'document.F=Object' + lt + '/script' + gt);
	  iframeDocument.close();
	  createDict = iframeDocument.F;
	  while (i--) delete createDict[PROTOTYPE$1][_enumBugKeys[i]];
	  return createDict();
	};

	var _objectCreate = Object.create || function create(O, Properties) {
	  var result;
	  if (O !== null) {
	    Empty[PROTOTYPE$1] = _anObject(O);
	    result = new Empty();
	    Empty[PROTOTYPE$1] = null;
	    // add "__proto__" for Object.getPrototypeOf polyfill
	    result[IE_PROTO$1] = O;
	  } else result = createDict();
	  return Properties === undefined ? result : _objectDps(result, Properties);
	};

	var IteratorPrototype = {};

	// 25.1.2.1.1 %IteratorPrototype%[@@iterator]()
	_hide(IteratorPrototype, _wks('iterator'), function () { return this; });

	var _iterCreate = function (Constructor, NAME, next) {
	  Constructor.prototype = _objectCreate(IteratorPrototype, { next: _propertyDesc(1, next) });
	  _setToStringTag(Constructor, NAME + ' Iterator');
	};

	// 19.1.2.9 / 15.2.3.2 Object.getPrototypeOf(O)


	var IE_PROTO$2 = _sharedKey('IE_PROTO');
	var ObjectProto = Object.prototype;

	var _objectGpo = Object.getPrototypeOf || function (O) {
	  O = _toObject(O);
	  if (_has(O, IE_PROTO$2)) return O[IE_PROTO$2];
	  if (typeof O.constructor == 'function' && O instanceof O.constructor) {
	    return O.constructor.prototype;
	  } return O instanceof Object ? ObjectProto : null;
	};

	var ITERATOR$3 = _wks('iterator');
	var BUGGY = !([].keys && 'next' in [].keys()); // Safari has buggy iterators w/o `next`
	var FF_ITERATOR = '@@iterator';
	var KEYS = 'keys';
	var VALUES = 'values';

	var returnThis = function () { return this; };

	var _iterDefine = function (Base, NAME, Constructor, next, DEFAULT, IS_SET, FORCED) {
	  _iterCreate(Constructor, NAME, next);
	  var getMethod = function (kind) {
	    if (!BUGGY && kind in proto) return proto[kind];
	    switch (kind) {
	      case KEYS: return function keys() { return new Constructor(this, kind); };
	      case VALUES: return function values() { return new Constructor(this, kind); };
	    } return function entries() { return new Constructor(this, kind); };
	  };
	  var TAG = NAME + ' Iterator';
	  var DEF_VALUES = DEFAULT == VALUES;
	  var VALUES_BUG = false;
	  var proto = Base.prototype;
	  var $native = proto[ITERATOR$3] || proto[FF_ITERATOR] || DEFAULT && proto[DEFAULT];
	  var $default = $native || getMethod(DEFAULT);
	  var $entries = DEFAULT ? !DEF_VALUES ? $default : getMethod('entries') : undefined;
	  var $anyNative = NAME == 'Array' ? proto.entries || $native : $native;
	  var methods, key, IteratorPrototype;
	  // Fix native
	  if ($anyNative) {
	    IteratorPrototype = _objectGpo($anyNative.call(new Base()));
	    if (IteratorPrototype !== Object.prototype && IteratorPrototype.next) {
	      // Set @@toStringTag to native iterators
	      _setToStringTag(IteratorPrototype, TAG, true);
	      // fix for some old engines
	      if (!_library && typeof IteratorPrototype[ITERATOR$3] != 'function') _hide(IteratorPrototype, ITERATOR$3, returnThis);
	    }
	  }
	  // fix Array#{values, @@iterator}.name in V8 / FF
	  if (DEF_VALUES && $native && $native.name !== VALUES) {
	    VALUES_BUG = true;
	    $default = function values() { return $native.call(this); };
	  }
	  // Define iterator
	  if ((!_library || FORCED) && (BUGGY || VALUES_BUG || !proto[ITERATOR$3])) {
	    _hide(proto, ITERATOR$3, $default);
	  }
	  // Plug for library
	  _iterators[NAME] = $default;
	  _iterators[TAG] = returnThis;
	  if (DEFAULT) {
	    methods = {
	      values: DEF_VALUES ? $default : getMethod(VALUES),
	      keys: IS_SET ? $default : getMethod(KEYS),
	      entries: $entries
	    };
	    if (FORCED) for (key in methods) {
	      if (!(key in proto)) _redefine(proto, key, methods[key]);
	    } else _export(_export.P + _export.F * (BUGGY || VALUES_BUG), NAME, methods);
	  }
	  return methods;
	};

	var $at = _stringAt(true);

	// 21.1.3.27 String.prototype[@@iterator]()
	_iterDefine(String, 'String', function (iterated) {
	  this._t = String(iterated); // target
	  this._i = 0;                // next index
	// 21.1.5.2.1 %StringIteratorPrototype%.next()
	}, function () {
	  var O = this._t;
	  var index = this._i;
	  var point;
	  if (index >= O.length) return { value: undefined, done: true };
	  point = $at(O, index);
	  this._i += point.length;
	  return { value: point, done: false };
	});

	// 22.1.3.31 Array.prototype[@@unscopables]
	var UNSCOPABLES = _wks('unscopables');
	var ArrayProto$1 = Array.prototype;
	if (ArrayProto$1[UNSCOPABLES] == undefined) _hide(ArrayProto$1, UNSCOPABLES, {});
	var _addToUnscopables = function (key) {
	  ArrayProto$1[UNSCOPABLES][key] = true;
	};

	var _iterStep = function (done, value) {
	  return { value: value, done: !!done };
	};

	// 22.1.3.4 Array.prototype.entries()
	// 22.1.3.13 Array.prototype.keys()
	// 22.1.3.29 Array.prototype.values()
	// 22.1.3.30 Array.prototype[@@iterator]()
	var es6_array_iterator = _iterDefine(Array, 'Array', function (iterated, kind) {
	  this._t = _toIobject(iterated); // target
	  this._i = 0;                   // next index
	  this._k = kind;                // kind
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
	}, 'values');

	// argumentsList[@@iterator] is %ArrayProto_values% (9.4.4.6, 9.4.4.7)
	_iterators.Arguments = _iterators.Array;

	_addToUnscopables('keys');
	_addToUnscopables('values');
	_addToUnscopables('entries');

	var ITERATOR$4 = _wks('iterator');
	var TO_STRING_TAG = _wks('toStringTag');
	var ArrayValues = _iterators.Array;

	var DOMIterables = {
	  CSSRuleList: true, // TODO: Not spec compliant, should be false.
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
	  MediaList: true, // TODO: Not spec compliant, should be false.
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
	  StyleSheetList: true, // TODO: Not spec compliant, should be false.
	  TextTrackCueList: false,
	  TextTrackList: false,
	  TouchList: false
	};

	for (var collections = _objectKeys(DOMIterables), i = 0; i < collections.length; i++) {
	  var NAME = collections[i];
	  var explicit = DOMIterables[NAME];
	  var Collection = _global[NAME];
	  var proto = Collection && Collection.prototype;
	  var key;
	  if (proto) {
	    if (!proto[ITERATOR$4]) _hide(proto, ITERATOR$4, ArrayValues);
	    if (!proto[TO_STRING_TAG]) _hide(proto, TO_STRING_TAG, NAME);
	    _iterators[NAME] = ArrayValues;
	    if (explicit) for (key in es6_array_iterator) if (!proto[key]) _redefine(proto, key, es6_array_iterator[key], true);
	  }
	}

	// most Object methods by ES6 should accept primitives



	var _objectSap = function (KEY, exec) {
	  var fn = (_core.Object || {})[KEY] || Object[KEY];
	  var exp = {};
	  exp[KEY] = exec(fn);
	  _export(_export.S + _export.F * _fails(function () { fn(1); }), 'Object', exp);
	};

	// 19.1.2.14 Object.keys(O)



	_objectSap('keys', function () {
	  return function keys(it) {
	    return _objectKeys(_toObject(it));
	  };
	});

	function _isPlaceholder(a) {
	       return a != null && typeof a === 'object' && a['@@functional/placeholder'] === true;
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
	var always = /*#__PURE__*/_curry1(function always(val) {
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
	var F = /*#__PURE__*/always(false);

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
	var T = /*#__PURE__*/always(true);

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
	var add = /*#__PURE__*/_curry2(function add(a, b) {
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
	var curryN = /*#__PURE__*/_curry2(function curryN(length, fn) {
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

	var _xfBase = {
	  init: function () {
	    return this.xf['@@transducer/init']();
	  },
	  result: function (result) {
	    return this.xf['@@transducer/result'](result);
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
	var max$1 = /*#__PURE__*/_curry2(function max(a, b) {
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
	var _isArrayLike = /*#__PURE__*/_curry1(function isArrayLike(x) {
	  if (_isArray$1(x)) {
	    return true;
	  }
	  if (!x) {
	    return false;
	  }
	  if (typeof x !== 'object') {
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

	var XWrap = /*#__PURE__*/function () {
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
	var bind = /*#__PURE__*/_curry2(function bind(fn, thisObj) {
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

	var XMap = /*#__PURE__*/function () {
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

	var _xmap = /*#__PURE__*/_curry2(function _xmap(f, xf) {
	  return new XMap(f, xf);
	});

	function _has$1(prop, obj) {
	  return Object.prototype.hasOwnProperty.call(obj, prop);
	}

	var toString$1 = Object.prototype.toString;
	var _isArguments = function () {
	  return toString$1.call(arguments) === '[object Arguments]' ? function _isArguments(x) {
	    return toString$1.call(x) === '[object Arguments]';
	  } : function _isArguments(x) {
	    return _has$1('callee', x);
	  };
	};

	// cover IE < 9 keys issues
	var hasEnumBug = ! /*#__PURE__*/{ toString: null }.propertyIsEnumerable('toString');
	var nonEnumerableProps = ['constructor', 'valueOf', 'isPrototypeOf', 'toString', 'propertyIsEnumerable', 'hasOwnProperty', 'toLocaleString'];
	// Safari bug
	var hasArgsEnumBug = /*#__PURE__*/function () {

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
	var keys = /*#__PURE__*/_curry1(_keys);

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
	var map = /*#__PURE__*/_curry2( /*#__PURE__*/_dispatchable(['fantasy-land/map', 'map'], _xmap, function map(fn, functor) {
	  switch (Object.prototype.toString.call(functor)) {
	    case '[object Function]':
	      return curryN(functor.length, function () {
	        return fn.call(this, functor.apply(this, arguments));
	      });
	    case '[object Object]':
	      return _reduce(function (acc, key) {
	        acc[key] = fn(functor[key]);
	        return acc;
	      }, {}, keys(functor));
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
	var path = /*#__PURE__*/_curry2(function path(paths, obj) {
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

	var prop = /*#__PURE__*/_curry2(function prop(p, obj) {
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
	var pluck = /*#__PURE__*/_curry2(function pluck(p, list) {
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
	var reduce = /*#__PURE__*/_curry3(_reduce);

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
	var ap = /*#__PURE__*/_curry2(function ap(applyF, applyX) {
	  return typeof applyX['fantasy-land/ap'] === 'function' ? applyX['fantasy-land/ap'](applyF) : typeof applyF.ap === 'function' ? applyF.ap(applyX) : typeof applyF === 'function' ? function (x) {
	    return applyF(x)(applyX(x));
	  } :
	  // else
	  _reduce(function (acc, f) {
	    return _concat(acc, map(f, applyX));
	  }, [], applyF);
	});

	/**
	 * Determine if the passed argument is an integer.
	 *
	 * @private
	 * @param {*} n
	 * @category Type
	 * @return {Boolean}
	 */

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
	var liftN = /*#__PURE__*/_curry2(function liftN(arity, fn) {
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
	var lift = /*#__PURE__*/_curry1(function lift(fn) {
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
	var curry = /*#__PURE__*/_curry1(function curry(fn) {
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
	var call = /*#__PURE__*/curry(function call(fn) {
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

	var preservingReduced = function (xf) {
	  return {
	    '@@transducer/init': _xfBase.init,
	    '@@transducer/result': function (result) {
	      return xf['@@transducer/result'](result);
	    },
	    '@@transducer/step': function (result, input) {
	      var ret = xf['@@transducer/step'](result, input);
	      return ret['@@transducer/reduced'] ? _forceReduced(ret) : ret;
	    }
	  };
	};

	var _flatCat = function _xcat(xf) {
	  var rxf = preservingReduced(xf);
	  return {
	    '@@transducer/init': _xfBase.init,
	    '@@transducer/result': function (result) {
	      return rxf['@@transducer/result'](result);
	    },
	    '@@transducer/step': function (result, input) {
	      return !_isArrayLike(input) ? _reduce(rxf, result, [input]) : _reduce(rxf, result, input);
	    }
	  };
	};

	var _xchain = /*#__PURE__*/_curry2(function _xchain(f, xf) {
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
	var chain = /*#__PURE__*/_curry2( /*#__PURE__*/_dispatchable(['fantasy-land/chain', 'chain'], _xchain, function chain(fn, monad) {
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
	var type = /*#__PURE__*/_curry1(function type(val) {
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
	var not = /*#__PURE__*/_curry1(function not(a) {
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
	var complement = /*#__PURE__*/lift(not);

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
	var slice = /*#__PURE__*/_curry3( /*#__PURE__*/_checkForMethod('slice', function slice(fromIndex, toIndex, list) {
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
	var tail = /*#__PURE__*/_curry1( /*#__PURE__*/_checkForMethod('tail', /*#__PURE__*/slice(1, Infinity)));

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
	var reverse = /*#__PURE__*/_curry1(function reverse(list) {
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
	var identical = /*#__PURE__*/_curry2(function identical(a, b) {
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
	  }

	  // if *a* array contains any element that is not included in *b*
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
	      if (!(typeof a === typeof b && identical(a.valueOf(), b.valueOf()))) {
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

	  var keysA = keys(a);
	  if (keysA.length !== keys(b).length) {
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
	var equals = /*#__PURE__*/_curry2(function equals(a, b) {
	  return _equals(a, b, [], []);
	});

	function _indexOf(list, a, idx) {
	  var inf, item;
	  // Array.prototype.indexOf doesn't exist below IE9
	  if (typeof list.indexOf === 'function') {
	    switch (typeof a) {
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
	        }
	        // non-zero numbers can utilise Set
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
	  }
	  // anything else not covered above, defer to R.equals
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

	function _isObject$1(x) {
	  return Object.prototype.toString.call(x) === '[object Object]';
	}

	var XFilter = /*#__PURE__*/function () {
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

	var _xfilter = /*#__PURE__*/_curry2(function _xfilter(f, xf) {
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
	var filter = /*#__PURE__*/_curry2( /*#__PURE__*/_dispatchable(['filter'], _xfilter, function (pred, filterable) {
	  return _isObject$1(filterable) ? _reduce(function (acc, key) {
	    if (pred(filterable[key])) {
	      acc[key] = filterable[key];
	    }
	    return acc;
	  }, {}, keys(filterable)) :
	  // else
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
	var reject = /*#__PURE__*/_curry2(function reject(pred, filterable) {
	  return filter(_complement(pred), filterable);
	});

	function _toString(x, seen) {
	  var recur = function recur(y) {
	    var xs = seen.concat([x]);
	    return _contains(y, xs) ? '<Circular>' : _toString(y, xs);
	  };

	  //  mapPairs :: (Object, [String]) -> [String]
	  var mapPairs = function (obj, keys$$1) {
	    return _map(function (k) {
	      return _quote(k) + ': ' + recur(obj[k]);
	    }, keys$$1.slice().sort());
	  };

	  switch (Object.prototype.toString.call(x)) {
	    case '[object Arguments]':
	      return '(function() { return arguments; }(' + _map(recur, x).join(', ') + '))';
	    case '[object Array]':
	      return '[' + _map(recur, x).concat(mapPairs(x, reject(function (k) {
	        return (/^\d+$/.test(k)
	        );
	      }, keys(x)))).join(', ') + ']';
	    case '[object Boolean]':
	      return typeof x === 'object' ? 'new Boolean(' + recur(x.valueOf()) + ')' : x.toString();
	    case '[object Date]':
	      return 'new Date(' + (isNaN(x.valueOf()) ? recur(NaN) : _quote(_toISOString(x))) + ')';
	    case '[object Null]':
	      return 'null';
	    case '[object Number]':
	      return typeof x === 'object' ? 'new Number(' + recur(x.valueOf()) + ')' : 1 / x === -Infinity ? '-0' : x.toString(10);
	    case '[object String]':
	      return typeof x === 'object' ? 'new String(' + recur(x.valueOf()) + ')' : _quote(x);
	    case '[object Undefined]':
	      return 'undefined';
	    default:
	      if (typeof x.toString === 'function') {
	        var repr = x.toString();
	        if (repr !== '[object Object]') {
	          return repr;
	        }
	      }
	      return '{' + mapPairs(x, keys(x)).join(', ') + '}';
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
	var toString$2 = /*#__PURE__*/_curry1(function toString(val) {
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
	var converge = /*#__PURE__*/_curry2(function converge(after, fns) {
	  return curryN(reduce(max$1, 0, pluck('length', fns)), function () {
	    var args = arguments;
	    var context = this;
	    return after.apply(context, _map(function (fn) {
	      return fn.apply(context, args);
	    }, fns));
	  });
	});

	var XReduceBy = /*#__PURE__*/function () {
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

	var _xreduceBy = /*#__PURE__*/_curryN(4, [], function _xreduceBy(valueFn, valueAcc, keyFn, xf) {
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
	var reduceBy = /*#__PURE__*/_curryN(4, [], /*#__PURE__*/_dispatchable([], _xreduceBy, function reduceBy(valueFn, valueAcc, keyFn, list) {
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
	var countBy = /*#__PURE__*/reduceBy(function (acc, elem) {
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
	var dec = /*#__PURE__*/add(-1);

	var XDropRepeatsWith = /*#__PURE__*/function () {
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

	var _xdropRepeatsWith = /*#__PURE__*/_curry2(function _xdropRepeatsWith(pred, xf) {
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
	var nth = /*#__PURE__*/_curry2(function nth(offset, list) {
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
	var last = /*#__PURE__*/nth(-1);

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
	var dropRepeatsWith = /*#__PURE__*/_curry2( /*#__PURE__*/_dispatchable([], _xdropRepeatsWith, function dropRepeatsWith(pred, list) {
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
	var dropRepeats = /*#__PURE__*/_curry1( /*#__PURE__*/_dispatchable([], /*#__PURE__*/_xdropRepeatsWith(equals), /*#__PURE__*/dropRepeatsWith(equals)));

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
	var flip = /*#__PURE__*/_curry1(function flip(fn) {
	  return curryN(fn.length, function (a, b) {
	    var args = Array.prototype.slice.call(arguments, 0);
	    args[0] = b;
	    args[1] = a;
	    return fn.apply(this, args);
	  });
	});

	/**
	 * Creates a new object from a list key-value pairs. If a key appears in
	 * multiple pairs, the rightmost pair is included in the object.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.3.0
	 * @category List
	 * @sig [[k,v]] -> {k: v}
	 * @param {Array} pairs An array of two-element arrays that will be the keys and values of the output object.
	 * @return {Object} The object made by pairing up `keys` and `values`.
	 * @see R.toPairs, R.pair
	 * @example
	 *
	 *      R.fromPairs([['a', 1], ['b', 2], ['c', 3]]); //=> {a: 1, b: 2, c: 3}
	 */
	var fromPairs = /*#__PURE__*/_curry1(function fromPairs(pairs) {
	  var result = {};
	  var idx = 0;
	  while (idx < pairs.length) {
	    result[pairs[idx][0]] = pairs[idx][1];
	    idx += 1;
	  }
	  return result;
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
	var groupBy = /*#__PURE__*/_curry2( /*#__PURE__*/_checkForMethod('groupBy', /*#__PURE__*/reduceBy(function (acc, item) {
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
	var head = /*#__PURE__*/nth(0);

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
	var identity = /*#__PURE__*/_curry1(_identity);

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
	var inc = /*#__PURE__*/add(1);

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
	var indexBy = /*#__PURE__*/reduceBy(function (acc, elem) {
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
	var init = /*#__PURE__*/slice(0, -1);

	var _Set = /*#__PURE__*/function () {
	  function _Set() {
	    /* globals Set */
	    this._nativeSet = typeof Set === 'function' ? new Set() : null;
	    this._items = {};
	  }

	  // until we figure out why jsdoc chokes on this
	  // @param item The item to add to the Set
	  // @returns {boolean} true if the item did not exist prior, otherwise false
	  //
	  _Set.prototype.add = function (item) {
	    return !hasOrAdd(item, true, this);
	  };

	  //
	  // @param item The item to check for existence in the Set
	  // @returns {boolean} true if the item exists in the Set, otherwise false
	  //
	  _Set.prototype.has = function (item) {
	    return hasOrAdd(item, false, this);
	  };

	  //
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
	  var type = typeof item;
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
	      }
	      // these types can all utilise the native Set
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
	      }
	      // scan through all previously applied items
	      if (!_contains(item, set._items[type])) {
	        if (shouldAdd) {
	          set._items[type].push(item);
	        }
	        return false;
	      }
	      return true;
	  }
	}

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
	var uniqBy = /*#__PURE__*/_curry2(function uniqBy(fn, list) {
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
	var uniq = /*#__PURE__*/uniqBy(identity);

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
	var invoker = /*#__PURE__*/_curry2(function invoker(arity, method) {
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
	var join = /*#__PURE__*/invoker(1, 'join');

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
	var juxt = /*#__PURE__*/_curry1(function juxt(fns) {
	  return converge(function () {
	    return Array.prototype.slice.call(arguments, 0);
	  }, fns);
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
	var sum = /*#__PURE__*/reduce(add, 0);

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
	var memoizeWith = /*#__PURE__*/_curry2(function memoizeWith(mFn, fn) {
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
	var memoize = /*#__PURE__*/memoizeWith(function () {
	  return toString$2(arguments);
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
	var multiply = /*#__PURE__*/_curry2(function multiply(a, b) {
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
	var partialRight = /*#__PURE__*/_createPartialApplicator( /*#__PURE__*/flip(_concat));

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
	var partition = /*#__PURE__*/juxt([filter, reject]);

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
	var pickAll = /*#__PURE__*/_curry2(function pickAll(names, obj) {
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
	var product = /*#__PURE__*/reduce(multiply, 1);

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
	var useWith = /*#__PURE__*/_curry2(function useWith(fn, transformers) {
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
	var project = /*#__PURE__*/useWith(_map, [pickAll, identity]); // passing `identity` gives correct arity

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
	var split = /*#__PURE__*/invoker(1, 'split');

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
	var toLower = /*#__PURE__*/invoker(0, 'toLowerCase');

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
	var toPairs = /*#__PURE__*/_curry1(function toPairs(obj) {
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
	var toUpper = /*#__PURE__*/invoker(0, 'toUpperCase');

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
	var transduce = /*#__PURE__*/curryN(4, function transduce(xf, fn, acc, list) {
	  return _reduce(xf(typeof fn === 'function' ? _xwrap(fn) : fn), acc, list);
	});

	var ws = '\x09\x0A\x0B\x0C\x0D\x20\xA0\u1680\u180E\u2000\u2001\u2002\u2003' + '\u2004\u2005\u2006\u2007\u2008\u2009\u200A\u202F\u205F\u3000\u2028' + '\u2029\uFEFF';
	var zeroWidth = '\u200b';
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
	var _trim = !hasProtoTrim || /*#__PURE__*/ws.trim() || ! /*#__PURE__*/zeroWidth.trim() ? function trim(str) {
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
	var union = /*#__PURE__*/_curry2( /*#__PURE__*/compose(uniq, _concat));

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
	var unnest = /*#__PURE__*/chain(_identity);

	var QueryRenderer =
	/*#__PURE__*/
	function (_React$Component) {
	  _inherits(QueryRenderer, _React$Component);

	  function QueryRenderer(props$$1) {
	    var _this;

	    _classCallCheck(this, QueryRenderer);

	    _this = _possibleConstructorReturn(this, _getPrototypeOf(QueryRenderer).call(this, props$$1));
	    _this.state = {};
	    _this.mutexObj = {};
	    return _this;
	  }

	  _createClass(QueryRenderer, [{
	    key: "componentDidMount",
	    value: function componentDidMount() {
	      if (this.props.query) {
	        this.load(this.props.query);
	      }

	      if (this.props.queries) {
	        this.loadQueries(this.props.queries);
	      }
	    }
	  }, {
	    key: "componentDidUpdate",
	    value: function componentDidUpdate(prevProps) {
	      var query = this.props.query;

	      if (!equals(prevProps.query, query)) {
	        this.load(query);
	      }

	      var queries = this.props.queries;

	      if (!equals(prevProps.queries, queries)) {
	        this.loadQueries(queries);
	      }
	    }
	  }, {
	    key: "load",
	    value: function load(query) {
	      var _this2 = this;

	      this.setState({
	        isLoading: true,
	        resultSet: null,
	        error: null,
	        sqlQuery: null
	      });

	      if (query && Object.keys(query).length) {
	        if (this.props.loadSql === 'only') {
	          this.props.cubejsApi.sql(query, {
	            mutexObj: this.mutexObj,
	            mutexKey: 'sql'
	          }).then(function (sqlQuery) {
	            return _this2.setState({
	              sqlQuery: sqlQuery,
	              error: null,
	              isLoading: false
	            });
	          }).catch(function (error) {
	            return _this2.setState({
	              resultSet: null,
	              error: error,
	              isLoading: false
	            });
	          });
	        } else if (this.props.loadSql) {
	          Promise.all([this.props.cubejsApi.sql(query, {
	            mutexObj: this.mutexObj,
	            mutexKey: 'sql'
	          }), this.props.cubejsApi.load(query, {
	            mutexObj: this.mutexObj,
	            mutexKey: 'query'
	          })]).then(function (_ref) {
	            var _ref2 = _slicedToArray(_ref, 2),
	                sqlQuery = _ref2[0],
	                resultSet = _ref2[1];

	            return _this2.setState({
	              sqlQuery: sqlQuery,
	              resultSet: resultSet,
	              error: null,
	              isLoading: false
	            });
	          }).catch(function (error) {
	            return _this2.setState({
	              resultSet: null,
	              error: error,
	              isLoading: false
	            });
	          });
	        } else {
	          this.props.cubejsApi.load(query, {
	            mutexObj: this.mutexObj,
	            mutexKey: 'query'
	          }).then(function (resultSet) {
	            return _this2.setState({
	              resultSet: resultSet,
	              error: null,
	              isLoading: false
	            });
	          }).catch(function (error) {
	            return _this2.setState({
	              resultSet: null,
	              error: error,
	              isLoading: false
	            });
	          });
	        }
	      }
	    }
	  }, {
	    key: "loadQueries",
	    value: function loadQueries(queries) {
	      var _this3 = this;

	      this.setState({
	        isLoading: true,
	        resultSet: null,
	        error: null
	      });
	      var resultPromises = Promise.all(toPairs(queries).map(function (_ref3) {
	        var _ref4 = _slicedToArray(_ref3, 2),
	            name = _ref4[0],
	            query = _ref4[1];

	        return _this3.props.cubejsApi.load(query, {
	          mutexObj: _this3.mutexObj,
	          mutexKey: name
	        }).then(function (r) {
	          return [name, r];
	        });
	      }));
	      resultPromises.then(function (resultSet) {
	        return _this3.setState({
	          resultSet: fromPairs(resultSet),
	          error: null,
	          isLoading: false
	        });
	      }).catch(function (error) {
	        return _this3.setState({
	          resultSet: null,
	          error: error,
	          isLoading: false
	        });
	      });
	    }
	  }, {
	    key: "render",
	    value: function render() {
	      var loadState = {
	        error: this.state.error,
	        resultSet: this.props.queries ? this.state.resultSet || {} : this.state.resultSet,
	        loadingState: {
	          isLoading: this.state.isLoading
	        },
	        sqlQuery: this.state.sqlQuery
	      };

	      if (this.props.render) {
	        return this.props.render(loadState);
	      }

	      return null;
	    }
	  }]);

	  return QueryRenderer;
	}(React.Component);
	QueryRenderer.propTypes = {
	  render: PropTypes.func,
	  afterRender: PropTypes.func,
	  cubejsApi: PropTypes.object,
	  query: PropTypes.object,
	  queries: PropTypes.object,
	  loadSql: PropTypes.any
	};

	var QueryRendererWithTotals = (function (_ref) {
	  var query = _ref.query,
	      restProps = _objectWithoutProperties(_ref, ["query"]);

	  return React.createElement(QueryRenderer, _extends({
	    queries: {
	      totals: _objectSpread({}, query, {
	        dimensions: [],
	        timeDimensions: query.timeDimensions ? query.timeDimensions.map(function (td) {
	          return _objectSpread({}, td, {
	            granularity: null
	          });
	        }) : undefined
	      }),
	      main: query
	    }
	  }, restProps));
	});

	var $filter = _arrayMethods(2);

	_export(_export.P + _export.F * !_strictMethod([].filter, true), 'Array', {
	  // 22.1.3.7 / 15.4.4.20 Array.prototype.filter(callbackfn [, thisArg])
	  filter: function filter(callbackfn /* , thisArg */) {
	    return $filter(this, callbackfn, arguments[1]);
	  }
	});

	var dP$1 = _objectDp.f;
	var FProto = Function.prototype;
	var nameRE = /^\s*function ([^ (]*)/;
	var NAME$1 = 'name';

	// 19.2.4.2 name
	NAME$1 in FProto || _descriptors && dP$1(FProto, NAME$1, {
	  configurable: true,
	  get: function () {
	    try {
	      return ('' + this).match(nameRE)[1];
	    } catch (e) {
	      return '';
	    }
	  }
	});

	var runtime = createCommonjsModule(function (module) {
	/**
	 * Copyright (c) 2014-present, Facebook, Inc.
	 *
	 * This source code is licensed under the MIT license found in the
	 * LICENSE file in the root directory of this source tree.
	 */

	!(function(global) {

	  var Op = Object.prototype;
	  var hasOwn = Op.hasOwnProperty;
	  var undefined; // More compressible than void 0.
	  var $Symbol = typeof Symbol === "function" ? Symbol : {};
	  var iteratorSymbol = $Symbol.iterator || "@@iterator";
	  var asyncIteratorSymbol = $Symbol.asyncIterator || "@@asyncIterator";
	  var toStringTagSymbol = $Symbol.toStringTag || "@@toStringTag";
	  var runtime = global.regeneratorRuntime;
	  if (runtime) {
	    {
	      // If regeneratorRuntime is defined globally and we're in a module,
	      // make the exports object identical to regeneratorRuntime.
	      module.exports = runtime;
	    }
	    // Don't bother evaluating the rest of this file if the runtime was
	    // already defined globally.
	    return;
	  }

	  // Define the runtime globally (as expected by generated code) as either
	  // module.exports (if we're in a module) or a new, empty object.
	  runtime = global.regeneratorRuntime = module.exports;

	  function wrap(innerFn, outerFn, self, tryLocsList) {
	    // If outerFn provided and outerFn.prototype is a Generator, then outerFn.prototype instanceof Generator.
	    var protoGenerator = outerFn && outerFn.prototype instanceof Generator ? outerFn : Generator;
	    var generator = Object.create(protoGenerator.prototype);
	    var context = new Context(tryLocsList || []);

	    // The ._invoke method unifies the implementations of the .next,
	    // .throw, and .return methods.
	    generator._invoke = makeInvokeMethod(innerFn, self, context);

	    return generator;
	  }
	  runtime.wrap = wrap;

	  // Try/catch helper to minimize deoptimizations. Returns a completion
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
	      return { type: "normal", arg: fn.call(obj, arg) };
	    } catch (err) {
	      return { type: "throw", arg: err };
	    }
	  }

	  var GenStateSuspendedStart = "suspendedStart";
	  var GenStateSuspendedYield = "suspendedYield";
	  var GenStateExecuting = "executing";
	  var GenStateCompleted = "completed";

	  // Returning this object from the innerFn has the same effect as
	  // breaking out of the dispatch switch statement.
	  var ContinueSentinel = {};

	  // Dummy constructor functions that we use as the .constructor and
	  // .constructor.prototype properties for functions that return Generator
	  // objects. For full spec compliance, you may wish to configure your
	  // minifier not to mangle the names of these two functions.
	  function Generator() {}
	  function GeneratorFunction() {}
	  function GeneratorFunctionPrototype() {}

	  // This is a polyfill for %IteratorPrototype% for environments that
	  // don't natively support it.
	  var IteratorPrototype = {};
	  IteratorPrototype[iteratorSymbol] = function () {
	    return this;
	  };

	  var getProto = Object.getPrototypeOf;
	  var NativeIteratorPrototype = getProto && getProto(getProto(values([])));
	  if (NativeIteratorPrototype &&
	      NativeIteratorPrototype !== Op &&
	      hasOwn.call(NativeIteratorPrototype, iteratorSymbol)) {
	    // This environment has a native %IteratorPrototype%; use it instead
	    // of the polyfill.
	    IteratorPrototype = NativeIteratorPrototype;
	  }

	  var Gp = GeneratorFunctionPrototype.prototype =
	    Generator.prototype = Object.create(IteratorPrototype);
	  GeneratorFunction.prototype = Gp.constructor = GeneratorFunctionPrototype;
	  GeneratorFunctionPrototype.constructor = GeneratorFunction;
	  GeneratorFunctionPrototype[toStringTagSymbol] =
	    GeneratorFunction.displayName = "GeneratorFunction";

	  // Helper for defining the .next, .throw, and .return methods of the
	  // Iterator interface in terms of a single ._invoke method.
	  function defineIteratorMethods(prototype) {
	    ["next", "throw", "return"].forEach(function(method) {
	      prototype[method] = function(arg) {
	        return this._invoke(method, arg);
	      };
	    });
	  }

	  runtime.isGeneratorFunction = function(genFun) {
	    var ctor = typeof genFun === "function" && genFun.constructor;
	    return ctor
	      ? ctor === GeneratorFunction ||
	        // For the native GeneratorFunction constructor, the best we can
	        // do is to check its .name property.
	        (ctor.displayName || ctor.name) === "GeneratorFunction"
	      : false;
	  };

	  runtime.mark = function(genFun) {
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
	  };

	  // Within the body of any async function, `await x` is transformed to
	  // `yield regeneratorRuntime.awrap(x)`, so that the runtime can test
	  // `hasOwn.call(value, "__await")` to determine if the yielded value is
	  // meant to be awaited.
	  runtime.awrap = function(arg) {
	    return { __await: arg };
	  };

	  function AsyncIterator(generator) {
	    function invoke(method, arg, resolve, reject) {
	      var record = tryCatch(generator[method], generator, arg);
	      if (record.type === "throw") {
	        reject(record.arg);
	      } else {
	        var result = record.arg;
	        var value = result.value;
	        if (value &&
	            typeof value === "object" &&
	            hasOwn.call(value, "__await")) {
	          return Promise.resolve(value.__await).then(function(value) {
	            invoke("next", value, resolve, reject);
	          }, function(err) {
	            invoke("throw", err, resolve, reject);
	          });
	        }

	        return Promise.resolve(value).then(function(unwrapped) {
	          // When a yielded Promise is resolved, its final value becomes
	          // the .value of the Promise<{value,done}> result for the
	          // current iteration.
	          result.value = unwrapped;
	          resolve(result);
	        }, function(error) {
	          // If a rejected Promise was yielded, throw the rejection back
	          // into the async generator function so it can be handled there.
	          return invoke("throw", error, resolve, reject);
	        });
	      }
	    }

	    var previousPromise;

	    function enqueue(method, arg) {
	      function callInvokeWithMethodAndArg() {
	        return new Promise(function(resolve, reject) {
	          invoke(method, arg, resolve, reject);
	        });
	      }

	      return previousPromise =
	        // If enqueue has been called before, then we want to wait until
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
	        previousPromise ? previousPromise.then(
	          callInvokeWithMethodAndArg,
	          // Avoid propagating failures to Promises returned by later
	          // invocations of the iterator.
	          callInvokeWithMethodAndArg
	        ) : callInvokeWithMethodAndArg();
	    }

	    // Define the unified helper method that is used to implement .next,
	    // .throw, and .return (see defineIteratorMethods).
	    this._invoke = enqueue;
	  }

	  defineIteratorMethods(AsyncIterator.prototype);
	  AsyncIterator.prototype[asyncIteratorSymbol] = function () {
	    return this;
	  };
	  runtime.AsyncIterator = AsyncIterator;

	  // Note that simple async functions are implemented on top of
	  // AsyncIterator objects; they just return a Promise for the value of
	  // the final result produced by the iterator.
	  runtime.async = function(innerFn, outerFn, self, tryLocsList) {
	    var iter = new AsyncIterator(
	      wrap(innerFn, outerFn, self, tryLocsList)
	    );

	    return runtime.isGeneratorFunction(outerFn)
	      ? iter // If outerFn is a generator, return the full iterator.
	      : iter.next().then(function(result) {
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
	        }

	        // Be forgiving, per 25.3.3.3.3 of the spec:
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
	          state = context.done
	            ? GenStateCompleted
	            : GenStateSuspendedYield;

	          if (record.arg === ContinueSentinel) {
	            continue;
	          }

	          return {
	            value: record.arg,
	            done: context.done
	          };

	        } else if (record.type === "throw") {
	          state = GenStateCompleted;
	          // Dispatch the exception by looping back around to the
	          // context.dispatchException(context.arg) call above.
	          context.method = "throw";
	          context.arg = record.arg;
	        }
	      }
	    };
	  }

	  // Call delegate.iterator[context.method](context.arg) and handle the
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
	        context.arg = new TypeError(
	          "The iterator does not provide a 'throw' method");
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

	    if (! info) {
	      context.method = "throw";
	      context.arg = new TypeError("iterator result is not an object");
	      context.delegate = null;
	      return ContinueSentinel;
	    }

	    if (info.done) {
	      // Assign the result of the finished delegate to the temporary
	      // variable specified by delegate.resultName (see delegateYield).
	      context[delegate.resultName] = info.value;

	      // Resume execution at the desired location (see delegateYield).
	      context.next = delegate.nextLoc;

	      // If context.method was "throw" but the delegate handled the
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
	    }

	    // The delegate iterator is finished, so forget it and continue with
	    // the outer generator.
	    context.delegate = null;
	    return ContinueSentinel;
	  }

	  // Define Generator.prototype.{next,throw,return} in terms of the
	  // unified ._invoke helper method.
	  defineIteratorMethods(Gp);

	  Gp[toStringTagSymbol] = "Generator";

	  // A Generator should always return itself as the iterator object when the
	  // @@iterator function is called on it. Some browsers' implementations of the
	  // iterator prototype chain incorrectly implement this, causing the Generator
	  // object to not be returned from this call. This ensures that doesn't happen.
	  // See https://github.com/facebook/regenerator/issues/274 for more details.
	  Gp[iteratorSymbol] = function() {
	    return this;
	  };

	  Gp.toString = function() {
	    return "[object Generator]";
	  };

	  function pushTryEntry(locs) {
	    var entry = { tryLoc: locs[0] };

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
	    this.tryEntries = [{ tryLoc: "root" }];
	    tryLocsList.forEach(pushTryEntry, this);
	    this.reset(true);
	  }

	  runtime.keys = function(object) {
	    var keys = [];
	    for (var key in object) {
	      keys.push(key);
	    }
	    keys.reverse();

	    // Rather than returning an object with a next method, we keep
	    // things simple and return the next function itself.
	    return function next() {
	      while (keys.length) {
	        var key = keys.pop();
	        if (key in object) {
	          next.value = key;
	          next.done = false;
	          return next;
	        }
	      }

	      // To avoid creating an additional object, we just hang the .value
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
	        var i = -1, next = function next() {
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
	    }

	    // Return an iterator with no values.
	    return { next: doneResult };
	  }
	  runtime.values = values;

	  function doneResult() {
	    return { value: undefined, done: true };
	  }

	  Context.prototype = {
	    constructor: Context,

	    reset: function(skipTempReset) {
	      this.prev = 0;
	      this.next = 0;
	      // Resetting context._sent for legacy support of Babel's
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
	          if (name.charAt(0) === "t" &&
	              hasOwn.call(this, name) &&
	              !isNaN(+name.slice(1))) {
	            this[name] = undefined;
	          }
	        }
	      }
	    },

	    stop: function() {
	      this.done = true;

	      var rootEntry = this.tryEntries[0];
	      var rootRecord = rootEntry.completion;
	      if (rootRecord.type === "throw") {
	        throw rootRecord.arg;
	      }

	      return this.rval;
	    },

	    dispatchException: function(exception) {
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

	        return !! caught;
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

	    abrupt: function(type, arg) {
	      for (var i = this.tryEntries.length - 1; i >= 0; --i) {
	        var entry = this.tryEntries[i];
	        if (entry.tryLoc <= this.prev &&
	            hasOwn.call(entry, "finallyLoc") &&
	            this.prev < entry.finallyLoc) {
	          var finallyEntry = entry;
	          break;
	        }
	      }

	      if (finallyEntry &&
	          (type === "break" ||
	           type === "continue") &&
	          finallyEntry.tryLoc <= arg &&
	          arg <= finallyEntry.finallyLoc) {
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

	    complete: function(record, afterLoc) {
	      if (record.type === "throw") {
	        throw record.arg;
	      }

	      if (record.type === "break" ||
	          record.type === "continue") {
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

	    finish: function(finallyLoc) {
	      for (var i = this.tryEntries.length - 1; i >= 0; --i) {
	        var entry = this.tryEntries[i];
	        if (entry.finallyLoc === finallyLoc) {
	          this.complete(entry.completion, entry.afterLoc);
	          resetTryEntry(entry);
	          return ContinueSentinel;
	        }
	      }
	    },

	    "catch": function(tryLoc) {
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
	      }

	      // The context.catch method must only be called with a location
	      // argument that corresponds to a known catch block.
	      throw new Error("illegal catch attempt");
	    },

	    delegateYield: function(iterable, resultName, nextLoc) {
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
	})(
	  // In sloppy mode, unbound `this` refers to the global object, fallback to
	  // Function constructor if we're in global strict mode. That is sadly a form
	  // of indirect eval which violates Content Security Policy.
	  (function() {
	    return this || (typeof self === "object" && self);
	  })() || Function("return this")()
	);
	});

	var QueryBuilder =
	/*#__PURE__*/
	function (_React$Component) {
	  _inherits(QueryBuilder, _React$Component);

	  function QueryBuilder(props) {
	    var _this;

	    _classCallCheck(this, QueryBuilder);

	    _this = _possibleConstructorReturn(this, _getPrototypeOf(QueryBuilder).call(this, props));
	    _this.state = {
	      query: props.query,
	      chartType: 'line'
	    };
	    return _this;
	  }

	  _createClass(QueryBuilder, [{
	    key: "componentDidMount",
	    value: function () {
	      var _componentDidMount = _asyncToGenerator(
	      /*#__PURE__*/
	      regeneratorRuntime.mark(function _callee() {
	        var meta;
	        return regeneratorRuntime.wrap(function _callee$(_context) {
	          while (1) {
	            switch (_context.prev = _context.next) {
	              case 0:
	                _context.next = 2;
	                return this.props.cubejsApi.meta();

	              case 2:
	                meta = _context.sent;
	                this.setState({
	                  meta: meta
	                });

	              case 4:
	              case "end":
	                return _context.stop();
	            }
	          }
	        }, _callee, this);
	      }));

	      return function componentDidMount() {
	        return _componentDidMount.apply(this, arguments);
	      };
	    }()
	  }, {
	    key: "render",
	    value: function render() {
	      var _this2 = this;

	      return React.createElement(QueryRenderer, {
	        query: this.state.query,
	        cubejsApi: this.props.cubejsApi,
	        render: function render(queryRendererProps) {
	          if (_this2.props.render) {
	            return _this2.props.render(_this2.prepareRenderProps(queryRendererProps));
	          }
	        }
	      });
	    }
	  }, {
	    key: "prepareRenderProps",
	    value: function prepareRenderProps(queryRendererProps) {
	      var _this3 = this;

	      var getName = function getName(member) {
	        return member.name;
	      };

	      var toTimeDimension = function toTimeDimension(member) {
	        return {
	          dimension: member.dimension.name,
	          granularity: member.granularity,
	          dateRange: member.dateRange
	        };
	      };

	      var updateMethods = function updateMethods(memberType) {
	        var toQuery = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : getName;
	        return {
	          add: function add(member) {
	            return _this3.setState({
	              query: _objectSpread({}, _this3.state.query, _defineProperty({}, memberType, (_this3.state.query[memberType] || []).concat(toQuery(member))))
	            });
	          },
	          remove: function remove(member) {
	            var members = (_this3.state.query[memberType] || []).concat([]);
	            members.splice(member.index, 1);
	            return _this3.setState({
	              query: _objectSpread({}, _this3.state.query, _defineProperty({}, memberType, members))
	            });
	          },
	          update: function update(member, updateWith) {
	            var members = (_this3.state.query[memberType] || []).concat([]);
	            members.splice(member.index, 1, toQuery(updateWith));
	            return _this3.setState({
	              query: _objectSpread({}, _this3.state.query, _defineProperty({}, memberType, members))
	            });
	          }
	        };
	      };

	      var granularities = [{
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
	      return _objectSpread({
	        meta: this.state.meta,
	        query: this.state.query,
	        chartType: this.state.chartType,
	        measures: (this.state.meta && this.state.query.measures || []).map(function (m, i) {
	          return _objectSpread({
	            index: i
	          }, _this3.state.meta.resolveMember(m, 'measures'));
	        }),
	        dimensions: (this.state.meta && this.state.query.dimensions || []).map(function (m, i) {
	          return _objectSpread({
	            index: i
	          }, _this3.state.meta.resolveMember(m, 'dimensions'));
	        }),
	        segments: (this.state.meta && this.state.query.segments || []).map(function (m, i) {
	          return _objectSpread({
	            index: i
	          }, _this3.state.meta.resolveMember(m, 'segments'));
	        }),
	        timeDimensions: (this.state.meta && this.state.query.timeDimensions || []).map(function (m, i) {
	          return _objectSpread({}, m, {
	            dimension: _objectSpread({}, _this3.state.meta.resolveMember(m.dimension, 'dimensions'), {
	              granularities: granularities
	            }),
	            index: i
	          });
	        }),
	        filters: (this.state.meta && this.state.query.filters || []).map(function (m, i) {
	          return _objectSpread({}, m, {
	            dimension: _this3.state.meta.resolveMember(m.dimension, 'dimensions'),
	            index: i
	          });
	        }),
	        availableMeasures: this.state.meta && this.state.meta.membersForQuery(this.state.query, 'measures') || [],
	        availableDimensions: this.state.meta && this.state.meta.membersForQuery(this.state.query, 'dimensions') || [],
	        availableTimeDimensions: (this.state.meta && this.state.meta.membersForQuery(this.state.query, 'dimensions') || []).filter(function (m) {
	          return m.type === 'time';
	        }),
	        availableSegments: this.state.meta && this.state.meta.membersForQuery(this.state.query, 'segments') || [],
	        updateMeasures: updateMethods('measures'),
	        updateDimensions: updateMethods('dimensions'),
	        updateSegments: updateMethods('segments'),
	        updateTimeDimensions: updateMethods('timeDimensions', toTimeDimension),
	        updateChartType: function updateChartType(chartType) {
	          return _this3.setState({
	            chartType: chartType
	          });
	        }
	      }, queryRendererProps);
	    }
	  }]);

	  return QueryBuilder;
	}(React.Component);

	exports.QueryRenderer = QueryRenderer;
	exports.QueryRendererWithTotals = QueryRendererWithTotals;
	exports.QueryBuilder = QueryBuilder;

	Object.defineProperty(exports, '__esModule', { value: true });

})));
