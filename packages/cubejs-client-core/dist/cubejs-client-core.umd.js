(function (global, factory) {
	typeof exports === 'object' && typeof module !== 'undefined' ? module.exports = factory() :
	typeof define === 'function' && define.amd ? define(factory) :
	(global = global || self, global.cubejs = factory());
}(this, function () { 'use strict';

	var commonjsGlobal = typeof window !== 'undefined' ? window : typeof global !== 'undefined' ? global : typeof self !== 'undefined' ? self : {};

	function commonjsRequire () {
		throw new Error('Dynamic requires are not currently supported by rollup-plugin-commonjs');
	}

	function unwrapExports (x) {
		return x && x.__esModule && Object.prototype.hasOwnProperty.call(x, 'default') ? x['default'] : x;
	}

	function createCommonjsModule(fn, module) {
		return module = { exports: {} }, fn(module, module.exports), module.exports;
	}

	var check = function (it) {
	  return it && it.Math == Math && it;
	};

	// https://github.com/zloirock/core-js/issues/86#issuecomment-115759028
	var global_1 =
	  // eslint-disable-next-line no-undef
	  check(typeof globalThis == 'object' && globalThis) ||
	  check(typeof window == 'object' && window) ||
	  check(typeof self == 'object' && self) ||
	  check(typeof commonjsGlobal == 'object' && commonjsGlobal) ||
	  // eslint-disable-next-line no-new-func
	  Function('return this')();

	var isPure = false;

	var fails = function (exec) {
	  try {
	    return !!exec();
	  } catch (error) {
	    return true;
	  }
	};

	// Thank's IE8 for his funny defineProperty
	var descriptors = !fails(function () {
	  return Object.defineProperty({}, 1, { get: function () { return 7; } })[1] != 7;
	});

	var isObject = function (it) {
	  return typeof it === 'object' ? it !== null : typeof it === 'function';
	};

	var document$1 = global_1.document;
	// typeof document.createElement is 'object' in old IE
	var EXISTS = isObject(document$1) && isObject(document$1.createElement);

	var documentCreateElement = function (it) {
	  return EXISTS ? document$1.createElement(it) : {};
	};

	// Thank's IE8 for his funny defineProperty
	var ie8DomDefine = !descriptors && !fails(function () {
	  return Object.defineProperty(documentCreateElement('div'), 'a', {
	    get: function () { return 7; }
	  }).a != 7;
	});

	var anObject = function (it) {
	  if (!isObject(it)) {
	    throw TypeError(String(it) + ' is not an object');
	  } return it;
	};

	// `ToPrimitive` abstract operation
	// https://tc39.github.io/ecma262/#sec-toprimitive
	// instead of the ES6 spec version, we didn't implement @@toPrimitive case
	// and the second argument - flag - preferred type is a string
	var toPrimitive = function (input, PREFERRED_STRING) {
	  if (!isObject(input)) return input;
	  var fn, val;
	  if (PREFERRED_STRING && typeof (fn = input.toString) == 'function' && !isObject(val = fn.call(input))) return val;
	  if (typeof (fn = input.valueOf) == 'function' && !isObject(val = fn.call(input))) return val;
	  if (!PREFERRED_STRING && typeof (fn = input.toString) == 'function' && !isObject(val = fn.call(input))) return val;
	  throw TypeError("Can't convert object to primitive value");
	};

	var nativeDefineProperty = Object.defineProperty;

	// `Object.defineProperty` method
	// https://tc39.github.io/ecma262/#sec-object.defineproperty
	var f = descriptors ? nativeDefineProperty : function defineProperty(O, P, Attributes) {
	  anObject(O);
	  P = toPrimitive(P, true);
	  anObject(Attributes);
	  if (ie8DomDefine) try {
	    return nativeDefineProperty(O, P, Attributes);
	  } catch (error) { /* empty */ }
	  if ('get' in Attributes || 'set' in Attributes) throw TypeError('Accessors not supported');
	  if ('value' in Attributes) O[P] = Attributes.value;
	  return O;
	};

	var objectDefineProperty = {
		f: f
	};

	var createPropertyDescriptor = function (bitmap, value) {
	  return {
	    enumerable: !(bitmap & 1),
	    configurable: !(bitmap & 2),
	    writable: !(bitmap & 4),
	    value: value
	  };
	};

	var createNonEnumerableProperty = descriptors ? function (object, key, value) {
	  return objectDefineProperty.f(object, key, createPropertyDescriptor(1, value));
	} : function (object, key, value) {
	  object[key] = value;
	  return object;
	};

	var setGlobal = function (key, value) {
	  try {
	    createNonEnumerableProperty(global_1, key, value);
	  } catch (error) {
	    global_1[key] = value;
	  } return value;
	};

	var SHARED = '__core-js_shared__';
	var store = global_1[SHARED] || setGlobal(SHARED, {});

	var sharedStore = store;

	var shared = createCommonjsModule(function (module) {
	(module.exports = function (key, value) {
	  return sharedStore[key] || (sharedStore[key] = value !== undefined ? value : {});
	})('versions', []).push({
	  version: '3.6.5',
	  mode: 'global',
	  copyright: '© 2020 Denis Pushkarev (zloirock.ru)'
	});
	});

	var hasOwnProperty = {}.hasOwnProperty;

	var has = function (it, key) {
	  return hasOwnProperty.call(it, key);
	};

	var id = 0;
	var postfix = Math.random();

	var uid = function (key) {
	  return 'Symbol(' + String(key === undefined ? '' : key) + ')_' + (++id + postfix).toString(36);
	};

	var nativeSymbol = !!Object.getOwnPropertySymbols && !fails(function () {
	  // Chrome 38 Symbol has incorrect toString conversion
	  // eslint-disable-next-line no-undef
	  return !String(Symbol());
	});

	var useSymbolAsUid = nativeSymbol
	  // eslint-disable-next-line no-undef
	  && !Symbol.sham
	  // eslint-disable-next-line no-undef
	  && typeof Symbol.iterator == 'symbol';

	var WellKnownSymbolsStore = shared('wks');
	var Symbol$1 = global_1.Symbol;
	var createWellKnownSymbol = useSymbolAsUid ? Symbol$1 : Symbol$1 && Symbol$1.withoutSetter || uid;

	var wellKnownSymbol = function (name) {
	  if (!has(WellKnownSymbolsStore, name)) {
	    if (nativeSymbol && has(Symbol$1, name)) WellKnownSymbolsStore[name] = Symbol$1[name];
	    else WellKnownSymbolsStore[name] = createWellKnownSymbol('Symbol.' + name);
	  } return WellKnownSymbolsStore[name];
	};

	var TO_STRING_TAG = wellKnownSymbol('toStringTag');
	var test = {};

	test[TO_STRING_TAG] = 'z';

	var toStringTagSupport = String(test) === '[object z]';

	var functionToString = Function.toString;

	// this helper broken in `3.4.1-3.4.4`, so we can't use `shared` helper
	if (typeof sharedStore.inspectSource != 'function') {
	  sharedStore.inspectSource = function (it) {
	    return functionToString.call(it);
	  };
	}

	var inspectSource = sharedStore.inspectSource;

	var WeakMap = global_1.WeakMap;

	var nativeWeakMap = typeof WeakMap === 'function' && /native code/.test(inspectSource(WeakMap));

	var keys = shared('keys');

	var sharedKey = function (key) {
	  return keys[key] || (keys[key] = uid(key));
	};

	var hiddenKeys = {};

	var WeakMap$1 = global_1.WeakMap;
	var set, get, has$1;

	var enforce = function (it) {
	  return has$1(it) ? get(it) : set(it, {});
	};

	var getterFor = function (TYPE) {
	  return function (it) {
	    var state;
	    if (!isObject(it) || (state = get(it)).type !== TYPE) {
	      throw TypeError('Incompatible receiver, ' + TYPE + ' required');
	    } return state;
	  };
	};

	if (nativeWeakMap) {
	  var store$1 = new WeakMap$1();
	  var wmget = store$1.get;
	  var wmhas = store$1.has;
	  var wmset = store$1.set;
	  set = function (it, metadata) {
	    wmset.call(store$1, it, metadata);
	    return metadata;
	  };
	  get = function (it) {
	    return wmget.call(store$1, it) || {};
	  };
	  has$1 = function (it) {
	    return wmhas.call(store$1, it);
	  };
	} else {
	  var STATE = sharedKey('state');
	  hiddenKeys[STATE] = true;
	  set = function (it, metadata) {
	    createNonEnumerableProperty(it, STATE, metadata);
	    return metadata;
	  };
	  get = function (it) {
	    return has(it, STATE) ? it[STATE] : {};
	  };
	  has$1 = function (it) {
	    return has(it, STATE);
	  };
	}

	var internalState = {
	  set: set,
	  get: get,
	  has: has$1,
	  enforce: enforce,
	  getterFor: getterFor
	};

	var redefine = createCommonjsModule(function (module) {
	var getInternalState = internalState.get;
	var enforceInternalState = internalState.enforce;
	var TEMPLATE = String(String).split('String');

	(module.exports = function (O, key, value, options) {
	  var unsafe = options ? !!options.unsafe : false;
	  var simple = options ? !!options.enumerable : false;
	  var noTargetGet = options ? !!options.noTargetGet : false;
	  if (typeof value == 'function') {
	    if (typeof key == 'string' && !has(value, 'name')) createNonEnumerableProperty(value, 'name', key);
	    enforceInternalState(value).source = TEMPLATE.join(typeof key == 'string' ? key : '');
	  }
	  if (O === global_1) {
	    if (simple) O[key] = value;
	    else setGlobal(key, value);
	    return;
	  } else if (!unsafe) {
	    delete O[key];
	  } else if (!noTargetGet && O[key]) {
	    simple = true;
	  }
	  if (simple) O[key] = value;
	  else createNonEnumerableProperty(O, key, value);
	// add fake Function#toString for correct work wrapped methods / constructors with methods like LoDash isNative
	})(Function.prototype, 'toString', function toString() {
	  return typeof this == 'function' && getInternalState(this).source || inspectSource(this);
	});
	});

	var toString = {}.toString;

	var classofRaw = function (it) {
	  return toString.call(it).slice(8, -1);
	};

	var TO_STRING_TAG$1 = wellKnownSymbol('toStringTag');
	// ES3 wrong here
	var CORRECT_ARGUMENTS = classofRaw(function () { return arguments; }()) == 'Arguments';

	// fallback for IE11 Script Access Denied error
	var tryGet = function (it, key) {
	  try {
	    return it[key];
	  } catch (error) { /* empty */ }
	};

	// getting tag from ES6+ `Object.prototype.toString`
	var classof = toStringTagSupport ? classofRaw : function (it) {
	  var O, tag, result;
	  return it === undefined ? 'Undefined' : it === null ? 'Null'
	    // @@toStringTag case
	    : typeof (tag = tryGet(O = Object(it), TO_STRING_TAG$1)) == 'string' ? tag
	    // builtinTag case
	    : CORRECT_ARGUMENTS ? classofRaw(O)
	    // ES3 arguments fallback
	    : (result = classofRaw(O)) == 'Object' && typeof O.callee == 'function' ? 'Arguments' : result;
	};

	// `Object.prototype.toString` method implementation
	// https://tc39.github.io/ecma262/#sec-object.prototype.tostring
	var objectToString = toStringTagSupport ? {}.toString : function toString() {
	  return '[object ' + classof(this) + ']';
	};

	// `Object.prototype.toString` method
	// https://tc39.github.io/ecma262/#sec-object.prototype.tostring
	if (!toStringTagSupport) {
	  redefine(Object.prototype, 'toString', objectToString, { unsafe: true });
	}

	var nativePropertyIsEnumerable = {}.propertyIsEnumerable;
	var getOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;

	// Nashorn ~ JDK8 bug
	var NASHORN_BUG = getOwnPropertyDescriptor && !nativePropertyIsEnumerable.call({ 1: 2 }, 1);

	// `Object.prototype.propertyIsEnumerable` method implementation
	// https://tc39.github.io/ecma262/#sec-object.prototype.propertyisenumerable
	var f$1 = NASHORN_BUG ? function propertyIsEnumerable(V) {
	  var descriptor = getOwnPropertyDescriptor(this, V);
	  return !!descriptor && descriptor.enumerable;
	} : nativePropertyIsEnumerable;

	var objectPropertyIsEnumerable = {
		f: f$1
	};

	var split = ''.split;

	// fallback for non-array-like ES3 and non-enumerable old V8 strings
	var indexedObject = fails(function () {
	  // throws an error in rhino, see https://github.com/mozilla/rhino/issues/346
	  // eslint-disable-next-line no-prototype-builtins
	  return !Object('z').propertyIsEnumerable(0);
	}) ? function (it) {
	  return classofRaw(it) == 'String' ? split.call(it, '') : Object(it);
	} : Object;

	// `RequireObjectCoercible` abstract operation
	// https://tc39.github.io/ecma262/#sec-requireobjectcoercible
	var requireObjectCoercible = function (it) {
	  if (it == undefined) throw TypeError("Can't call method on " + it);
	  return it;
	};

	// toObject with fallback for non-array-like ES3 strings



	var toIndexedObject = function (it) {
	  return indexedObject(requireObjectCoercible(it));
	};

	var nativeGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;

	// `Object.getOwnPropertyDescriptor` method
	// https://tc39.github.io/ecma262/#sec-object.getownpropertydescriptor
	var f$2 = descriptors ? nativeGetOwnPropertyDescriptor : function getOwnPropertyDescriptor(O, P) {
	  O = toIndexedObject(O);
	  P = toPrimitive(P, true);
	  if (ie8DomDefine) try {
	    return nativeGetOwnPropertyDescriptor(O, P);
	  } catch (error) { /* empty */ }
	  if (has(O, P)) return createPropertyDescriptor(!objectPropertyIsEnumerable.f.call(O, P), O[P]);
	};

	var objectGetOwnPropertyDescriptor = {
		f: f$2
	};

	var path = global_1;

	var aFunction = function (variable) {
	  return typeof variable == 'function' ? variable : undefined;
	};

	var getBuiltIn = function (namespace, method) {
	  return arguments.length < 2 ? aFunction(path[namespace]) || aFunction(global_1[namespace])
	    : path[namespace] && path[namespace][method] || global_1[namespace] && global_1[namespace][method];
	};

	var ceil = Math.ceil;
	var floor = Math.floor;

	// `ToInteger` abstract operation
	// https://tc39.github.io/ecma262/#sec-tointeger
	var toInteger = function (argument) {
	  return isNaN(argument = +argument) ? 0 : (argument > 0 ? floor : ceil)(argument);
	};

	var min = Math.min;

	// `ToLength` abstract operation
	// https://tc39.github.io/ecma262/#sec-tolength
	var toLength = function (argument) {
	  return argument > 0 ? min(toInteger(argument), 0x1FFFFFFFFFFFFF) : 0; // 2 ** 53 - 1 == 9007199254740991
	};

	var max = Math.max;
	var min$1 = Math.min;

	// Helper for a popular repeating case of the spec:
	// Let integer be ? ToInteger(index).
	// If integer < 0, let result be max((length + integer), 0); else let result be min(integer, length).
	var toAbsoluteIndex = function (index, length) {
	  var integer = toInteger(index);
	  return integer < 0 ? max(integer + length, 0) : min$1(integer, length);
	};

	// `Array.prototype.{ indexOf, includes }` methods implementation
	var createMethod = function (IS_INCLUDES) {
	  return function ($this, el, fromIndex) {
	    var O = toIndexedObject($this);
	    var length = toLength(O.length);
	    var index = toAbsoluteIndex(fromIndex, length);
	    var value;
	    // Array#includes uses SameValueZero equality algorithm
	    // eslint-disable-next-line no-self-compare
	    if (IS_INCLUDES && el != el) while (length > index) {
	      value = O[index++];
	      // eslint-disable-next-line no-self-compare
	      if (value != value) return true;
	    // Array#indexOf ignores holes, Array#includes - not
	    } else for (;length > index; index++) {
	      if ((IS_INCLUDES || index in O) && O[index] === el) return IS_INCLUDES || index || 0;
	    } return !IS_INCLUDES && -1;
	  };
	};

	var arrayIncludes = {
	  // `Array.prototype.includes` method
	  // https://tc39.github.io/ecma262/#sec-array.prototype.includes
	  includes: createMethod(true),
	  // `Array.prototype.indexOf` method
	  // https://tc39.github.io/ecma262/#sec-array.prototype.indexof
	  indexOf: createMethod(false)
	};

	var indexOf = arrayIncludes.indexOf;


	var objectKeysInternal = function (object, names) {
	  var O = toIndexedObject(object);
	  var i = 0;
	  var result = [];
	  var key;
	  for (key in O) !has(hiddenKeys, key) && has(O, key) && result.push(key);
	  // Don't enum bug & hidden keys
	  while (names.length > i) if (has(O, key = names[i++])) {
	    ~indexOf(result, key) || result.push(key);
	  }
	  return result;
	};

	// IE8- don't enum bug keys
	var enumBugKeys = [
	  'constructor',
	  'hasOwnProperty',
	  'isPrototypeOf',
	  'propertyIsEnumerable',
	  'toLocaleString',
	  'toString',
	  'valueOf'
	];

	var hiddenKeys$1 = enumBugKeys.concat('length', 'prototype');

	// `Object.getOwnPropertyNames` method
	// https://tc39.github.io/ecma262/#sec-object.getownpropertynames
	var f$3 = Object.getOwnPropertyNames || function getOwnPropertyNames(O) {
	  return objectKeysInternal(O, hiddenKeys$1);
	};

	var objectGetOwnPropertyNames = {
		f: f$3
	};

	var f$4 = Object.getOwnPropertySymbols;

	var objectGetOwnPropertySymbols = {
		f: f$4
	};

	// all object keys, includes non-enumerable and symbols
	var ownKeys = getBuiltIn('Reflect', 'ownKeys') || function ownKeys(it) {
	  var keys = objectGetOwnPropertyNames.f(anObject(it));
	  var getOwnPropertySymbols = objectGetOwnPropertySymbols.f;
	  return getOwnPropertySymbols ? keys.concat(getOwnPropertySymbols(it)) : keys;
	};

	var copyConstructorProperties = function (target, source) {
	  var keys = ownKeys(source);
	  var defineProperty = objectDefineProperty.f;
	  var getOwnPropertyDescriptor = objectGetOwnPropertyDescriptor.f;
	  for (var i = 0; i < keys.length; i++) {
	    var key = keys[i];
	    if (!has(target, key)) defineProperty(target, key, getOwnPropertyDescriptor(source, key));
	  }
	};

	var replacement = /#|\.prototype\./;

	var isForced = function (feature, detection) {
	  var value = data[normalize(feature)];
	  return value == POLYFILL ? true
	    : value == NATIVE ? false
	    : typeof detection == 'function' ? fails(detection)
	    : !!detection;
	};

	var normalize = isForced.normalize = function (string) {
	  return String(string).replace(replacement, '.').toLowerCase();
	};

	var data = isForced.data = {};
	var NATIVE = isForced.NATIVE = 'N';
	var POLYFILL = isForced.POLYFILL = 'P';

	var isForced_1 = isForced;

	var getOwnPropertyDescriptor$1 = objectGetOwnPropertyDescriptor.f;






	/*
	  options.target      - name of the target object
	  options.global      - target is the global object
	  options.stat        - export as static methods of target
	  options.proto       - export as prototype methods of target
	  options.real        - real prototype method for the `pure` version
	  options.forced      - export even if the native feature is available
	  options.bind        - bind methods to the target, required for the `pure` version
	  options.wrap        - wrap constructors to preventing global pollution, required for the `pure` version
	  options.unsafe      - use the simple assignment of property instead of delete + defineProperty
	  options.sham        - add a flag to not completely full polyfills
	  options.enumerable  - export as enumerable property
	  options.noTargetGet - prevent calling a getter on target
	*/
	var _export = function (options, source) {
	  var TARGET = options.target;
	  var GLOBAL = options.global;
	  var STATIC = options.stat;
	  var FORCED, target, key, targetProperty, sourceProperty, descriptor;
	  if (GLOBAL) {
	    target = global_1;
	  } else if (STATIC) {
	    target = global_1[TARGET] || setGlobal(TARGET, {});
	  } else {
	    target = (global_1[TARGET] || {}).prototype;
	  }
	  if (target) for (key in source) {
	    sourceProperty = source[key];
	    if (options.noTargetGet) {
	      descriptor = getOwnPropertyDescriptor$1(target, key);
	      targetProperty = descriptor && descriptor.value;
	    } else targetProperty = target[key];
	    FORCED = isForced_1(GLOBAL ? key : TARGET + (STATIC ? '.' : '#') + key, options.forced);
	    // contained in target
	    if (!FORCED && targetProperty !== undefined) {
	      if (typeof sourceProperty === typeof targetProperty) continue;
	      copyConstructorProperties(sourceProperty, targetProperty);
	    }
	    // add a flag to not completely full polyfills
	    if (options.sham || (targetProperty && targetProperty.sham)) {
	      createNonEnumerableProperty(sourceProperty, 'sham', true);
	    }
	    // extend global
	    redefine(target, key, sourceProperty, options);
	  }
	};

	var nativePromiseConstructor = global_1.Promise;

	var redefineAll = function (target, src, options) {
	  for (var key in src) redefine(target, key, src[key], options);
	  return target;
	};

	var defineProperty = objectDefineProperty.f;



	var TO_STRING_TAG$2 = wellKnownSymbol('toStringTag');

	var setToStringTag = function (it, TAG, STATIC) {
	  if (it && !has(it = STATIC ? it : it.prototype, TO_STRING_TAG$2)) {
	    defineProperty(it, TO_STRING_TAG$2, { configurable: true, value: TAG });
	  }
	};

	var SPECIES = wellKnownSymbol('species');

	var setSpecies = function (CONSTRUCTOR_NAME) {
	  var Constructor = getBuiltIn(CONSTRUCTOR_NAME);
	  var defineProperty = objectDefineProperty.f;

	  if (descriptors && Constructor && !Constructor[SPECIES]) {
	    defineProperty(Constructor, SPECIES, {
	      configurable: true,
	      get: function () { return this; }
	    });
	  }
	};

	var aFunction$1 = function (it) {
	  if (typeof it != 'function') {
	    throw TypeError(String(it) + ' is not a function');
	  } return it;
	};

	var anInstance = function (it, Constructor, name) {
	  if (!(it instanceof Constructor)) {
	    throw TypeError('Incorrect ' + (name ? name + ' ' : '') + 'invocation');
	  } return it;
	};

	var iterators = {};

	var ITERATOR = wellKnownSymbol('iterator');
	var ArrayPrototype = Array.prototype;

	// check on default Array iterator
	var isArrayIteratorMethod = function (it) {
	  return it !== undefined && (iterators.Array === it || ArrayPrototype[ITERATOR] === it);
	};

	// optional / simple context binding
	var functionBindContext = function (fn, that, length) {
	  aFunction$1(fn);
	  if (that === undefined) return fn;
	  switch (length) {
	    case 0: return function () {
	      return fn.call(that);
	    };
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

	var ITERATOR$1 = wellKnownSymbol('iterator');

	var getIteratorMethod = function (it) {
	  if (it != undefined) return it[ITERATOR$1]
	    || it['@@iterator']
	    || iterators[classof(it)];
	};

	// call something on iterator step with safe closing on error
	var callWithSafeIterationClosing = function (iterator, fn, value, ENTRIES) {
	  try {
	    return ENTRIES ? fn(anObject(value)[0], value[1]) : fn(value);
	  // 7.4.6 IteratorClose(iterator, completion)
	  } catch (error) {
	    var returnMethod = iterator['return'];
	    if (returnMethod !== undefined) anObject(returnMethod.call(iterator));
	    throw error;
	  }
	};

	var iterate_1 = createCommonjsModule(function (module) {
	var Result = function (stopped, result) {
	  this.stopped = stopped;
	  this.result = result;
	};

	var iterate = module.exports = function (iterable, fn, that, AS_ENTRIES, IS_ITERATOR) {
	  var boundFunction = functionBindContext(fn, that, AS_ENTRIES ? 2 : 1);
	  var iterator, iterFn, index, length, result, next, step;

	  if (IS_ITERATOR) {
	    iterator = iterable;
	  } else {
	    iterFn = getIteratorMethod(iterable);
	    if (typeof iterFn != 'function') throw TypeError('Target is not iterable');
	    // optimisation for array iterators
	    if (isArrayIteratorMethod(iterFn)) {
	      for (index = 0, length = toLength(iterable.length); length > index; index++) {
	        result = AS_ENTRIES
	          ? boundFunction(anObject(step = iterable[index])[0], step[1])
	          : boundFunction(iterable[index]);
	        if (result && result instanceof Result) return result;
	      } return new Result(false);
	    }
	    iterator = iterFn.call(iterable);
	  }

	  next = iterator.next;
	  while (!(step = next.call(iterator)).done) {
	    result = callWithSafeIterationClosing(iterator, boundFunction, step.value, AS_ENTRIES);
	    if (typeof result == 'object' && result && result instanceof Result) return result;
	  } return new Result(false);
	};

	iterate.stop = function (result) {
	  return new Result(true, result);
	};
	});

	var ITERATOR$2 = wellKnownSymbol('iterator');
	var SAFE_CLOSING = false;

	try {
	  var called = 0;
	  var iteratorWithReturn = {
	    next: function () {
	      return { done: !!called++ };
	    },
	    'return': function () {
	      SAFE_CLOSING = true;
	    }
	  };
	  iteratorWithReturn[ITERATOR$2] = function () {
	    return this;
	  };
	} catch (error) { /* empty */ }

	var checkCorrectnessOfIteration = function (exec, SKIP_CLOSING) {
	  if (!SKIP_CLOSING && !SAFE_CLOSING) return false;
	  var ITERATION_SUPPORT = false;
	  try {
	    var object = {};
	    object[ITERATOR$2] = function () {
	      return {
	        next: function () {
	          return { done: ITERATION_SUPPORT = true };
	        }
	      };
	    };
	    exec(object);
	  } catch (error) { /* empty */ }
	  return ITERATION_SUPPORT;
	};

	var SPECIES$1 = wellKnownSymbol('species');

	// `SpeciesConstructor` abstract operation
	// https://tc39.github.io/ecma262/#sec-speciesconstructor
	var speciesConstructor = function (O, defaultConstructor) {
	  var C = anObject(O).constructor;
	  var S;
	  return C === undefined || (S = anObject(C)[SPECIES$1]) == undefined ? defaultConstructor : aFunction$1(S);
	};

	var html = getBuiltIn('document', 'documentElement');

	var engineUserAgent = getBuiltIn('navigator', 'userAgent') || '';

	var engineIsIos = /(iphone|ipod|ipad).*applewebkit/i.test(engineUserAgent);

	var location = global_1.location;
	var set$1 = global_1.setImmediate;
	var clear = global_1.clearImmediate;
	var process = global_1.process;
	var MessageChannel = global_1.MessageChannel;
	var Dispatch = global_1.Dispatch;
	var counter = 0;
	var queue = {};
	var ONREADYSTATECHANGE = 'onreadystatechange';
	var defer, channel, port;

	var run = function (id) {
	  // eslint-disable-next-line no-prototype-builtins
	  if (queue.hasOwnProperty(id)) {
	    var fn = queue[id];
	    delete queue[id];
	    fn();
	  }
	};

	var runner = function (id) {
	  return function () {
	    run(id);
	  };
	};

	var listener = function (event) {
	  run(event.data);
	};

	var post = function (id) {
	  // old engines have not location.origin
	  global_1.postMessage(id + '', location.protocol + '//' + location.host);
	};

	// Node.js 0.9+ & IE10+ has setImmediate, otherwise:
	if (!set$1 || !clear) {
	  set$1 = function setImmediate(fn) {
	    var args = [];
	    var i = 1;
	    while (arguments.length > i) args.push(arguments[i++]);
	    queue[++counter] = function () {
	      // eslint-disable-next-line no-new-func
	      (typeof fn == 'function' ? fn : Function(fn)).apply(undefined, args);
	    };
	    defer(counter);
	    return counter;
	  };
	  clear = function clearImmediate(id) {
	    delete queue[id];
	  };
	  // Node.js 0.8-
	  if (classofRaw(process) == 'process') {
	    defer = function (id) {
	      process.nextTick(runner(id));
	    };
	  // Sphere (JS game engine) Dispatch API
	  } else if (Dispatch && Dispatch.now) {
	    defer = function (id) {
	      Dispatch.now(runner(id));
	    };
	  // Browsers with MessageChannel, includes WebWorkers
	  // except iOS - https://github.com/zloirock/core-js/issues/624
	  } else if (MessageChannel && !engineIsIos) {
	    channel = new MessageChannel();
	    port = channel.port2;
	    channel.port1.onmessage = listener;
	    defer = functionBindContext(port.postMessage, port, 1);
	  // Browsers with postMessage, skip WebWorkers
	  // IE8 has postMessage, but it's sync & typeof its postMessage is 'object'
	  } else if (
	    global_1.addEventListener &&
	    typeof postMessage == 'function' &&
	    !global_1.importScripts &&
	    !fails(post) &&
	    location.protocol !== 'file:'
	  ) {
	    defer = post;
	    global_1.addEventListener('message', listener, false);
	  // IE8-
	  } else if (ONREADYSTATECHANGE in documentCreateElement('script')) {
	    defer = function (id) {
	      html.appendChild(documentCreateElement('script'))[ONREADYSTATECHANGE] = function () {
	        html.removeChild(this);
	        run(id);
	      };
	    };
	  // Rest old browsers
	  } else {
	    defer = function (id) {
	      setTimeout(runner(id), 0);
	    };
	  }
	}

	var task = {
	  set: set$1,
	  clear: clear
	};

	var getOwnPropertyDescriptor$2 = objectGetOwnPropertyDescriptor.f;

	var macrotask = task.set;


	var MutationObserver = global_1.MutationObserver || global_1.WebKitMutationObserver;
	var process$1 = global_1.process;
	var Promise$1 = global_1.Promise;
	var IS_NODE = classofRaw(process$1) == 'process';
	// Node.js 11 shows ExperimentalWarning on getting `queueMicrotask`
	var queueMicrotaskDescriptor = getOwnPropertyDescriptor$2(global_1, 'queueMicrotask');
	var queueMicrotask = queueMicrotaskDescriptor && queueMicrotaskDescriptor.value;

	var flush, head, last, notify, toggle, node, promise, then;

	// modern engines have queueMicrotask method
	if (!queueMicrotask) {
	  flush = function () {
	    var parent, fn;
	    if (IS_NODE && (parent = process$1.domain)) parent.exit();
	    while (head) {
	      fn = head.fn;
	      head = head.next;
	      try {
	        fn();
	      } catch (error) {
	        if (head) notify();
	        else last = undefined;
	        throw error;
	      }
	    } last = undefined;
	    if (parent) parent.enter();
	  };

	  // Node.js
	  if (IS_NODE) {
	    notify = function () {
	      process$1.nextTick(flush);
	    };
	  // browsers with MutationObserver, except iOS - https://github.com/zloirock/core-js/issues/339
	  } else if (MutationObserver && !engineIsIos) {
	    toggle = true;
	    node = document.createTextNode('');
	    new MutationObserver(flush).observe(node, { characterData: true });
	    notify = function () {
	      node.data = toggle = !toggle;
	    };
	  // environments with maybe non-completely correct, but existent Promise
	  } else if (Promise$1 && Promise$1.resolve) {
	    // Promise.resolve without an argument throws an error in LG WebOS 2
	    promise = Promise$1.resolve(undefined);
	    then = promise.then;
	    notify = function () {
	      then.call(promise, flush);
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
	      macrotask.call(global_1, flush);
	    };
	  }
	}

	var microtask = queueMicrotask || function (fn) {
	  var task$$1 = { fn: fn, next: undefined };
	  if (last) last.next = task$$1;
	  if (!head) {
	    head = task$$1;
	    notify();
	  } last = task$$1;
	};

	var PromiseCapability = function (C) {
	  var resolve, reject;
	  this.promise = new C(function ($$resolve, $$reject) {
	    if (resolve !== undefined || reject !== undefined) throw TypeError('Bad Promise constructor');
	    resolve = $$resolve;
	    reject = $$reject;
	  });
	  this.resolve = aFunction$1(resolve);
	  this.reject = aFunction$1(reject);
	};

	// 25.4.1.5 NewPromiseCapability(C)
	var f$5 = function (C) {
	  return new PromiseCapability(C);
	};

	var newPromiseCapability = {
		f: f$5
	};

	var promiseResolve = function (C, x) {
	  anObject(C);
	  if (isObject(x) && x.constructor === C) return x;
	  var promiseCapability = newPromiseCapability.f(C);
	  var resolve = promiseCapability.resolve;
	  resolve(x);
	  return promiseCapability.promise;
	};

	var hostReportErrors = function (a, b) {
	  var console = global_1.console;
	  if (console && console.error) {
	    arguments.length === 1 ? console.error(a) : console.error(a, b);
	  }
	};

	var perform = function (exec) {
	  try {
	    return { error: false, value: exec() };
	  } catch (error) {
	    return { error: true, value: error };
	  }
	};

	var process$2 = global_1.process;
	var versions = process$2 && process$2.versions;
	var v8 = versions && versions.v8;
	var match, version;

	if (v8) {
	  match = v8.split('.');
	  version = match[0] + match[1];
	} else if (engineUserAgent) {
	  match = engineUserAgent.match(/Edge\/(\d+)/);
	  if (!match || match[1] >= 74) {
	    match = engineUserAgent.match(/Chrome\/(\d+)/);
	    if (match) version = match[1];
	  }
	}

	var engineV8Version = version && +version;

	var task$1 = task.set;










	var SPECIES$2 = wellKnownSymbol('species');
	var PROMISE = 'Promise';
	var getInternalState = internalState.get;
	var setInternalState = internalState.set;
	var getInternalPromiseState = internalState.getterFor(PROMISE);
	var PromiseConstructor = nativePromiseConstructor;
	var TypeError$1 = global_1.TypeError;
	var document$2 = global_1.document;
	var process$3 = global_1.process;
	var $fetch = getBuiltIn('fetch');
	var newPromiseCapability$1 = newPromiseCapability.f;
	var newGenericPromiseCapability = newPromiseCapability$1;
	var IS_NODE$1 = classofRaw(process$3) == 'process';
	var DISPATCH_EVENT = !!(document$2 && document$2.createEvent && global_1.dispatchEvent);
	var UNHANDLED_REJECTION = 'unhandledrejection';
	var REJECTION_HANDLED = 'rejectionhandled';
	var PENDING = 0;
	var FULFILLED = 1;
	var REJECTED = 2;
	var HANDLED = 1;
	var UNHANDLED = 2;
	var Internal, OwnPromiseCapability, PromiseWrapper, nativeThen;

	var FORCED = isForced_1(PROMISE, function () {
	  var GLOBAL_CORE_JS_PROMISE = inspectSource(PromiseConstructor) !== String(PromiseConstructor);
	  if (!GLOBAL_CORE_JS_PROMISE) {
	    // V8 6.6 (Node 10 and Chrome 66) have a bug with resolving custom thenables
	    // https://bugs.chromium.org/p/chromium/issues/detail?id=830565
	    // We can't detect it synchronously, so just check versions
	    if (engineV8Version === 66) return true;
	    // Unhandled rejections tracking support, NodeJS Promise without it fails @@species test
	    if (!IS_NODE$1 && typeof PromiseRejectionEvent != 'function') return true;
	  }
	  // We need Promise#finally in the pure version for preventing prototype pollution
	  if (isPure && !PromiseConstructor.prototype['finally']) return true;
	  // We can't use @@species feature detection in V8 since it causes
	  // deoptimization and performance degradation
	  // https://github.com/zloirock/core-js/issues/679
	  if (engineV8Version >= 51 && /native code/.test(PromiseConstructor)) return false;
	  // Detect correctness of subclassing with @@species support
	  var promise = PromiseConstructor.resolve(1);
	  var FakePromise = function (exec) {
	    exec(function () { /* empty */ }, function () { /* empty */ });
	  };
	  var constructor = promise.constructor = {};
	  constructor[SPECIES$2] = FakePromise;
	  return !(promise.then(function () { /* empty */ }) instanceof FakePromise);
	});

	var INCORRECT_ITERATION = FORCED || !checkCorrectnessOfIteration(function (iterable) {
	  PromiseConstructor.all(iterable)['catch'](function () { /* empty */ });
	});

	// helpers
	var isThenable = function (it) {
	  var then;
	  return isObject(it) && typeof (then = it.then) == 'function' ? then : false;
	};

	var notify$1 = function (promise, state, isReject) {
	  if (state.notified) return;
	  state.notified = true;
	  var chain = state.reactions;
	  microtask(function () {
	    var value = state.value;
	    var ok = state.state == FULFILLED;
	    var index = 0;
	    // variable length - can't use forEach
	    while (chain.length > index) {
	      var reaction = chain[index++];
	      var handler = ok ? reaction.ok : reaction.fail;
	      var resolve = reaction.resolve;
	      var reject = reaction.reject;
	      var domain = reaction.domain;
	      var result, then, exited;
	      try {
	        if (handler) {
	          if (!ok) {
	            if (state.rejection === UNHANDLED) onHandleUnhandled(promise, state);
	            state.rejection = HANDLED;
	          }
	          if (handler === true) result = value;
	          else {
	            if (domain) domain.enter();
	            result = handler(value); // can throw
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
	      } catch (error) {
	        if (domain && !exited) domain.exit();
	        reject(error);
	      }
	    }
	    state.reactions = [];
	    state.notified = false;
	    if (isReject && !state.rejection) onUnhandled(promise, state);
	  });
	};

	var dispatchEvent = function (name, promise, reason) {
	  var event, handler;
	  if (DISPATCH_EVENT) {
	    event = document$2.createEvent('Event');
	    event.promise = promise;
	    event.reason = reason;
	    event.initEvent(name, false, true);
	    global_1.dispatchEvent(event);
	  } else event = { promise: promise, reason: reason };
	  if (handler = global_1['on' + name]) handler(event);
	  else if (name === UNHANDLED_REJECTION) hostReportErrors('Unhandled promise rejection', reason);
	};

	var onUnhandled = function (promise, state) {
	  task$1.call(global_1, function () {
	    var value = state.value;
	    var IS_UNHANDLED = isUnhandled(state);
	    var result;
	    if (IS_UNHANDLED) {
	      result = perform(function () {
	        if (IS_NODE$1) {
	          process$3.emit('unhandledRejection', value, promise);
	        } else dispatchEvent(UNHANDLED_REJECTION, promise, value);
	      });
	      // Browsers should not trigger `rejectionHandled` event if it was handled here, NodeJS - should
	      state.rejection = IS_NODE$1 || isUnhandled(state) ? UNHANDLED : HANDLED;
	      if (result.error) throw result.value;
	    }
	  });
	};

	var isUnhandled = function (state) {
	  return state.rejection !== HANDLED && !state.parent;
	};

	var onHandleUnhandled = function (promise, state) {
	  task$1.call(global_1, function () {
	    if (IS_NODE$1) {
	      process$3.emit('rejectionHandled', promise);
	    } else dispatchEvent(REJECTION_HANDLED, promise, state.value);
	  });
	};

	var bind = function (fn, promise, state, unwrap) {
	  return function (value) {
	    fn(promise, state, value, unwrap);
	  };
	};

	var internalReject = function (promise, state, value, unwrap) {
	  if (state.done) return;
	  state.done = true;
	  if (unwrap) state = unwrap;
	  state.value = value;
	  state.state = REJECTED;
	  notify$1(promise, state, true);
	};

	var internalResolve = function (promise, state, value, unwrap) {
	  if (state.done) return;
	  state.done = true;
	  if (unwrap) state = unwrap;
	  try {
	    if (promise === value) throw TypeError$1("Promise can't be resolved itself");
	    var then = isThenable(value);
	    if (then) {
	      microtask(function () {
	        var wrapper = { done: false };
	        try {
	          then.call(value,
	            bind(internalResolve, promise, wrapper, state),
	            bind(internalReject, promise, wrapper, state)
	          );
	        } catch (error) {
	          internalReject(promise, wrapper, error, state);
	        }
	      });
	    } else {
	      state.value = value;
	      state.state = FULFILLED;
	      notify$1(promise, state, false);
	    }
	  } catch (error) {
	    internalReject(promise, { done: false }, error, state);
	  }
	};

	// constructor polyfill
	if (FORCED) {
	  // 25.4.3.1 Promise(executor)
	  PromiseConstructor = function Promise(executor) {
	    anInstance(this, PromiseConstructor, PROMISE);
	    aFunction$1(executor);
	    Internal.call(this);
	    var state = getInternalState(this);
	    try {
	      executor(bind(internalResolve, this, state), bind(internalReject, this, state));
	    } catch (error) {
	      internalReject(this, state, error);
	    }
	  };
	  // eslint-disable-next-line no-unused-vars
	  Internal = function Promise(executor) {
	    setInternalState(this, {
	      type: PROMISE,
	      done: false,
	      notified: false,
	      parent: false,
	      reactions: [],
	      rejection: false,
	      state: PENDING,
	      value: undefined
	    });
	  };
	  Internal.prototype = redefineAll(PromiseConstructor.prototype, {
	    // `Promise.prototype.then` method
	    // https://tc39.github.io/ecma262/#sec-promise.prototype.then
	    then: function then(onFulfilled, onRejected) {
	      var state = getInternalPromiseState(this);
	      var reaction = newPromiseCapability$1(speciesConstructor(this, PromiseConstructor));
	      reaction.ok = typeof onFulfilled == 'function' ? onFulfilled : true;
	      reaction.fail = typeof onRejected == 'function' && onRejected;
	      reaction.domain = IS_NODE$1 ? process$3.domain : undefined;
	      state.parent = true;
	      state.reactions.push(reaction);
	      if (state.state != PENDING) notify$1(this, state, false);
	      return reaction.promise;
	    },
	    // `Promise.prototype.catch` method
	    // https://tc39.github.io/ecma262/#sec-promise.prototype.catch
	    'catch': function (onRejected) {
	      return this.then(undefined, onRejected);
	    }
	  });
	  OwnPromiseCapability = function () {
	    var promise = new Internal();
	    var state = getInternalState(promise);
	    this.promise = promise;
	    this.resolve = bind(internalResolve, promise, state);
	    this.reject = bind(internalReject, promise, state);
	  };
	  newPromiseCapability.f = newPromiseCapability$1 = function (C) {
	    return C === PromiseConstructor || C === PromiseWrapper
	      ? new OwnPromiseCapability(C)
	      : newGenericPromiseCapability(C);
	  };

	  if (!isPure && typeof nativePromiseConstructor == 'function') {
	    nativeThen = nativePromiseConstructor.prototype.then;

	    // wrap native Promise#then for native async functions
	    redefine(nativePromiseConstructor.prototype, 'then', function then(onFulfilled, onRejected) {
	      var that = this;
	      return new PromiseConstructor(function (resolve, reject) {
	        nativeThen.call(that, resolve, reject);
	      }).then(onFulfilled, onRejected);
	    // https://github.com/zloirock/core-js/issues/640
	    }, { unsafe: true });

	    // wrap fetch result
	    if (typeof $fetch == 'function') _export({ global: true, enumerable: true, forced: true }, {
	      // eslint-disable-next-line no-unused-vars
	      fetch: function fetch(input /* , init */) {
	        return promiseResolve(PromiseConstructor, $fetch.apply(global_1, arguments));
	      }
	    });
	  }
	}

	_export({ global: true, wrap: true, forced: FORCED }, {
	  Promise: PromiseConstructor
	});

	setToStringTag(PromiseConstructor, PROMISE, false, true);
	setSpecies(PROMISE);

	PromiseWrapper = getBuiltIn(PROMISE);

	// statics
	_export({ target: PROMISE, stat: true, forced: FORCED }, {
	  // `Promise.reject` method
	  // https://tc39.github.io/ecma262/#sec-promise.reject
	  reject: function reject(r) {
	    var capability = newPromiseCapability$1(this);
	    capability.reject.call(undefined, r);
	    return capability.promise;
	  }
	});

	_export({ target: PROMISE, stat: true, forced: isPure || FORCED }, {
	  // `Promise.resolve` method
	  // https://tc39.github.io/ecma262/#sec-promise.resolve
	  resolve: function resolve(x) {
	    return promiseResolve(isPure && this === PromiseWrapper ? PromiseConstructor : this, x);
	  }
	});

	_export({ target: PROMISE, stat: true, forced: INCORRECT_ITERATION }, {
	  // `Promise.all` method
	  // https://tc39.github.io/ecma262/#sec-promise.all
	  all: function all(iterable) {
	    var C = this;
	    var capability = newPromiseCapability$1(C);
	    var resolve = capability.resolve;
	    var reject = capability.reject;
	    var result = perform(function () {
	      var $promiseResolve = aFunction$1(C.resolve);
	      var values = [];
	      var counter = 0;
	      var remaining = 1;
	      iterate_1(iterable, function (promise) {
	        var index = counter++;
	        var alreadyCalled = false;
	        values.push(undefined);
	        remaining++;
	        $promiseResolve.call(C, promise).then(function (value) {
	          if (alreadyCalled) return;
	          alreadyCalled = true;
	          values[index] = value;
	          --remaining || resolve(values);
	        }, reject);
	      });
	      --remaining || resolve(values);
	    });
	    if (result.error) reject(result.value);
	    return capability.promise;
	  },
	  // `Promise.race` method
	  // https://tc39.github.io/ecma262/#sec-promise.race
	  race: function race(iterable) {
	    var C = this;
	    var capability = newPromiseCapability$1(C);
	    var reject = capability.reject;
	    var result = perform(function () {
	      var $promiseResolve = aFunction$1(C.resolve);
	      iterate_1(iterable, function (promise) {
	        $promiseResolve.call(C, promise).then(capability.resolve, reject);
	      });
	    });
	    if (result.error) reject(result.value);
	    return capability.promise;
	  }
	});

	var slice = [].slice;
	var MSIE = /MSIE .\./.test(engineUserAgent); // <- dirty ie9- check

	var wrap = function (scheduler) {
	  return function (handler, timeout /* , ...arguments */) {
	    var boundArgs = arguments.length > 2;
	    var args = boundArgs ? slice.call(arguments, 2) : undefined;
	    return scheduler(boundArgs ? function () {
	      // eslint-disable-next-line no-new-func
	      (typeof handler == 'function' ? handler : Function(handler)).apply(this, args);
	    } : handler, timeout);
	  };
	};

	// ie9- setTimeout & setInterval additional parameters fix
	// https://html.spec.whatwg.org/multipage/timers-and-user-prompts.html#timers
	_export({ global: true, bind: true, forced: MSIE }, {
	  // `setTimeout` method
	  // https://html.spec.whatwg.org/multipage/timers-and-user-prompts.html#dom-settimeout
	  setTimeout: wrap(global_1.setTimeout),
	  // `setInterval` method
	  // https://html.spec.whatwg.org/multipage/timers-and-user-prompts.html#dom-setinterval
	  setInterval: wrap(global_1.setInterval)
	});

	// `IsArray` abstract operation
	// https://tc39.github.io/ecma262/#sec-isarray
	var isArray = Array.isArray || function isArray(arg) {
	  return classofRaw(arg) == 'Array';
	};

	// `ToObject` abstract operation
	// https://tc39.github.io/ecma262/#sec-toobject
	var toObject = function (argument) {
	  return Object(requireObjectCoercible(argument));
	};

	// `Object.keys` method
	// https://tc39.github.io/ecma262/#sec-object.keys
	var objectKeys = Object.keys || function keys(O) {
	  return objectKeysInternal(O, enumBugKeys);
	};

	// `Object.defineProperties` method
	// https://tc39.github.io/ecma262/#sec-object.defineproperties
	var objectDefineProperties = descriptors ? Object.defineProperties : function defineProperties(O, Properties) {
	  anObject(O);
	  var keys = objectKeys(Properties);
	  var length = keys.length;
	  var index = 0;
	  var key;
	  while (length > index) objectDefineProperty.f(O, key = keys[index++], Properties[key]);
	  return O;
	};

	var GT = '>';
	var LT = '<';
	var PROTOTYPE = 'prototype';
	var SCRIPT = 'script';
	var IE_PROTO = sharedKey('IE_PROTO');

	var EmptyConstructor = function () { /* empty */ };

	var scriptTag = function (content) {
	  return LT + SCRIPT + GT + content + LT + '/' + SCRIPT + GT;
	};

	// Create object with fake `null` prototype: use ActiveX Object with cleared prototype
	var NullProtoObjectViaActiveX = function (activeXDocument) {
	  activeXDocument.write(scriptTag(''));
	  activeXDocument.close();
	  var temp = activeXDocument.parentWindow.Object;
	  activeXDocument = null; // avoid memory leak
	  return temp;
	};

	// Create object with fake `null` prototype: use iframe Object with cleared prototype
	var NullProtoObjectViaIFrame = function () {
	  // Thrash, waste and sodomy: IE GC bug
	  var iframe = documentCreateElement('iframe');
	  var JS = 'java' + SCRIPT + ':';
	  var iframeDocument;
	  iframe.style.display = 'none';
	  html.appendChild(iframe);
	  // https://github.com/zloirock/core-js/issues/475
	  iframe.src = String(JS);
	  iframeDocument = iframe.contentWindow.document;
	  iframeDocument.open();
	  iframeDocument.write(scriptTag('document.F=Object'));
	  iframeDocument.close();
	  return iframeDocument.F;
	};

	// Check for document.domain and active x support
	// No need to use active x approach when document.domain is not set
	// see https://github.com/es-shims/es5-shim/issues/150
	// variation of https://github.com/kitcambridge/es5-shim/commit/4f738ac066346
	// avoid IE GC bug
	var activeXDocument;
	var NullProtoObject = function () {
	  try {
	    /* global ActiveXObject */
	    activeXDocument = document.domain && new ActiveXObject('htmlfile');
	  } catch (error) { /* ignore */ }
	  NullProtoObject = activeXDocument ? NullProtoObjectViaActiveX(activeXDocument) : NullProtoObjectViaIFrame();
	  var length = enumBugKeys.length;
	  while (length--) delete NullProtoObject[PROTOTYPE][enumBugKeys[length]];
	  return NullProtoObject();
	};

	hiddenKeys[IE_PROTO] = true;

	// `Object.create` method
	// https://tc39.github.io/ecma262/#sec-object.create
	var objectCreate = Object.create || function create(O, Properties) {
	  var result;
	  if (O !== null) {
	    EmptyConstructor[PROTOTYPE] = anObject(O);
	    result = new EmptyConstructor();
	    EmptyConstructor[PROTOTYPE] = null;
	    // add "__proto__" for Object.getPrototypeOf polyfill
	    result[IE_PROTO] = O;
	  } else result = NullProtoObject();
	  return Properties === undefined ? result : objectDefineProperties(result, Properties);
	};

	var nativeGetOwnPropertyNames = objectGetOwnPropertyNames.f;

	var toString$1 = {}.toString;

	var windowNames = typeof window == 'object' && window && Object.getOwnPropertyNames
	  ? Object.getOwnPropertyNames(window) : [];

	var getWindowNames = function (it) {
	  try {
	    return nativeGetOwnPropertyNames(it);
	  } catch (error) {
	    return windowNames.slice();
	  }
	};

	// fallback for IE11 buggy Object.getOwnPropertyNames with iframe and window
	var f$6 = function getOwnPropertyNames(it) {
	  return windowNames && toString$1.call(it) == '[object Window]'
	    ? getWindowNames(it)
	    : nativeGetOwnPropertyNames(toIndexedObject(it));
	};

	var objectGetOwnPropertyNamesExternal = {
		f: f$6
	};

	var f$7 = wellKnownSymbol;

	var wellKnownSymbolWrapped = {
		f: f$7
	};

	var defineProperty$1 = objectDefineProperty.f;

	var defineWellKnownSymbol = function (NAME) {
	  var Symbol = path.Symbol || (path.Symbol = {});
	  if (!has(Symbol, NAME)) defineProperty$1(Symbol, NAME, {
	    value: wellKnownSymbolWrapped.f(NAME)
	  });
	};

	var SPECIES$3 = wellKnownSymbol('species');

	// `ArraySpeciesCreate` abstract operation
	// https://tc39.github.io/ecma262/#sec-arrayspeciescreate
	var arraySpeciesCreate = function (originalArray, length) {
	  var C;
	  if (isArray(originalArray)) {
	    C = originalArray.constructor;
	    // cross-realm fallback
	    if (typeof C == 'function' && (C === Array || isArray(C.prototype))) C = undefined;
	    else if (isObject(C)) {
	      C = C[SPECIES$3];
	      if (C === null) C = undefined;
	    }
	  } return new (C === undefined ? Array : C)(length === 0 ? 0 : length);
	};

	var push = [].push;

	// `Array.prototype.{ forEach, map, filter, some, every, find, findIndex }` methods implementation
	var createMethod$1 = function (TYPE) {
	  var IS_MAP = TYPE == 1;
	  var IS_FILTER = TYPE == 2;
	  var IS_SOME = TYPE == 3;
	  var IS_EVERY = TYPE == 4;
	  var IS_FIND_INDEX = TYPE == 6;
	  var NO_HOLES = TYPE == 5 || IS_FIND_INDEX;
	  return function ($this, callbackfn, that, specificCreate) {
	    var O = toObject($this);
	    var self = indexedObject(O);
	    var boundFunction = functionBindContext(callbackfn, that, 3);
	    var length = toLength(self.length);
	    var index = 0;
	    var create = specificCreate || arraySpeciesCreate;
	    var target = IS_MAP ? create($this, length) : IS_FILTER ? create($this, 0) : undefined;
	    var value, result;
	    for (;length > index; index++) if (NO_HOLES || index in self) {
	      value = self[index];
	      result = boundFunction(value, index, O);
	      if (TYPE) {
	        if (IS_MAP) target[index] = result; // map
	        else if (result) switch (TYPE) {
	          case 3: return true;              // some
	          case 5: return value;             // find
	          case 6: return index;             // findIndex
	          case 2: push.call(target, value); // filter
	        } else if (IS_EVERY) return false;  // every
	      }
	    }
	    return IS_FIND_INDEX ? -1 : IS_SOME || IS_EVERY ? IS_EVERY : target;
	  };
	};

	var arrayIteration = {
	  // `Array.prototype.forEach` method
	  // https://tc39.github.io/ecma262/#sec-array.prototype.foreach
	  forEach: createMethod$1(0),
	  // `Array.prototype.map` method
	  // https://tc39.github.io/ecma262/#sec-array.prototype.map
	  map: createMethod$1(1),
	  // `Array.prototype.filter` method
	  // https://tc39.github.io/ecma262/#sec-array.prototype.filter
	  filter: createMethod$1(2),
	  // `Array.prototype.some` method
	  // https://tc39.github.io/ecma262/#sec-array.prototype.some
	  some: createMethod$1(3),
	  // `Array.prototype.every` method
	  // https://tc39.github.io/ecma262/#sec-array.prototype.every
	  every: createMethod$1(4),
	  // `Array.prototype.find` method
	  // https://tc39.github.io/ecma262/#sec-array.prototype.find
	  find: createMethod$1(5),
	  // `Array.prototype.findIndex` method
	  // https://tc39.github.io/ecma262/#sec-array.prototype.findIndex
	  findIndex: createMethod$1(6)
	};

	var $forEach = arrayIteration.forEach;

	var HIDDEN = sharedKey('hidden');
	var SYMBOL = 'Symbol';
	var PROTOTYPE$1 = 'prototype';
	var TO_PRIMITIVE = wellKnownSymbol('toPrimitive');
	var setInternalState$1 = internalState.set;
	var getInternalState$1 = internalState.getterFor(SYMBOL);
	var ObjectPrototype = Object[PROTOTYPE$1];
	var $Symbol = global_1.Symbol;
	var $stringify = getBuiltIn('JSON', 'stringify');
	var nativeGetOwnPropertyDescriptor$1 = objectGetOwnPropertyDescriptor.f;
	var nativeDefineProperty$1 = objectDefineProperty.f;
	var nativeGetOwnPropertyNames$1 = objectGetOwnPropertyNamesExternal.f;
	var nativePropertyIsEnumerable$1 = objectPropertyIsEnumerable.f;
	var AllSymbols = shared('symbols');
	var ObjectPrototypeSymbols = shared('op-symbols');
	var StringToSymbolRegistry = shared('string-to-symbol-registry');
	var SymbolToStringRegistry = shared('symbol-to-string-registry');
	var WellKnownSymbolsStore$1 = shared('wks');
	var QObject = global_1.QObject;
	// Don't use setters in Qt Script, https://github.com/zloirock/core-js/issues/173
	var USE_SETTER = !QObject || !QObject[PROTOTYPE$1] || !QObject[PROTOTYPE$1].findChild;

	// fallback for old Android, https://code.google.com/p/v8/issues/detail?id=687
	var setSymbolDescriptor = descriptors && fails(function () {
	  return objectCreate(nativeDefineProperty$1({}, 'a', {
	    get: function () { return nativeDefineProperty$1(this, 'a', { value: 7 }).a; }
	  })).a != 7;
	}) ? function (O, P, Attributes) {
	  var ObjectPrototypeDescriptor = nativeGetOwnPropertyDescriptor$1(ObjectPrototype, P);
	  if (ObjectPrototypeDescriptor) delete ObjectPrototype[P];
	  nativeDefineProperty$1(O, P, Attributes);
	  if (ObjectPrototypeDescriptor && O !== ObjectPrototype) {
	    nativeDefineProperty$1(ObjectPrototype, P, ObjectPrototypeDescriptor);
	  }
	} : nativeDefineProperty$1;

	var wrap$1 = function (tag, description) {
	  var symbol = AllSymbols[tag] = objectCreate($Symbol[PROTOTYPE$1]);
	  setInternalState$1(symbol, {
	    type: SYMBOL,
	    tag: tag,
	    description: description
	  });
	  if (!descriptors) symbol.description = description;
	  return symbol;
	};

	var isSymbol = useSymbolAsUid ? function (it) {
	  return typeof it == 'symbol';
	} : function (it) {
	  return Object(it) instanceof $Symbol;
	};

	var $defineProperty = function defineProperty(O, P, Attributes) {
	  if (O === ObjectPrototype) $defineProperty(ObjectPrototypeSymbols, P, Attributes);
	  anObject(O);
	  var key = toPrimitive(P, true);
	  anObject(Attributes);
	  if (has(AllSymbols, key)) {
	    if (!Attributes.enumerable) {
	      if (!has(O, HIDDEN)) nativeDefineProperty$1(O, HIDDEN, createPropertyDescriptor(1, {}));
	      O[HIDDEN][key] = true;
	    } else {
	      if (has(O, HIDDEN) && O[HIDDEN][key]) O[HIDDEN][key] = false;
	      Attributes = objectCreate(Attributes, { enumerable: createPropertyDescriptor(0, false) });
	    } return setSymbolDescriptor(O, key, Attributes);
	  } return nativeDefineProperty$1(O, key, Attributes);
	};

	var $defineProperties = function defineProperties(O, Properties) {
	  anObject(O);
	  var properties = toIndexedObject(Properties);
	  var keys = objectKeys(properties).concat($getOwnPropertySymbols(properties));
	  $forEach(keys, function (key) {
	    if (!descriptors || $propertyIsEnumerable.call(properties, key)) $defineProperty(O, key, properties[key]);
	  });
	  return O;
	};

	var $create = function create(O, Properties) {
	  return Properties === undefined ? objectCreate(O) : $defineProperties(objectCreate(O), Properties);
	};

	var $propertyIsEnumerable = function propertyIsEnumerable(V) {
	  var P = toPrimitive(V, true);
	  var enumerable = nativePropertyIsEnumerable$1.call(this, P);
	  if (this === ObjectPrototype && has(AllSymbols, P) && !has(ObjectPrototypeSymbols, P)) return false;
	  return enumerable || !has(this, P) || !has(AllSymbols, P) || has(this, HIDDEN) && this[HIDDEN][P] ? enumerable : true;
	};

	var $getOwnPropertyDescriptor = function getOwnPropertyDescriptor(O, P) {
	  var it = toIndexedObject(O);
	  var key = toPrimitive(P, true);
	  if (it === ObjectPrototype && has(AllSymbols, key) && !has(ObjectPrototypeSymbols, key)) return;
	  var descriptor = nativeGetOwnPropertyDescriptor$1(it, key);
	  if (descriptor && has(AllSymbols, key) && !(has(it, HIDDEN) && it[HIDDEN][key])) {
	    descriptor.enumerable = true;
	  }
	  return descriptor;
	};

	var $getOwnPropertyNames = function getOwnPropertyNames(O) {
	  var names = nativeGetOwnPropertyNames$1(toIndexedObject(O));
	  var result = [];
	  $forEach(names, function (key) {
	    if (!has(AllSymbols, key) && !has(hiddenKeys, key)) result.push(key);
	  });
	  return result;
	};

	var $getOwnPropertySymbols = function getOwnPropertySymbols(O) {
	  var IS_OBJECT_PROTOTYPE = O === ObjectPrototype;
	  var names = nativeGetOwnPropertyNames$1(IS_OBJECT_PROTOTYPE ? ObjectPrototypeSymbols : toIndexedObject(O));
	  var result = [];
	  $forEach(names, function (key) {
	    if (has(AllSymbols, key) && (!IS_OBJECT_PROTOTYPE || has(ObjectPrototype, key))) {
	      result.push(AllSymbols[key]);
	    }
	  });
	  return result;
	};

	// `Symbol` constructor
	// https://tc39.github.io/ecma262/#sec-symbol-constructor
	if (!nativeSymbol) {
	  $Symbol = function Symbol() {
	    if (this instanceof $Symbol) throw TypeError('Symbol is not a constructor');
	    var description = !arguments.length || arguments[0] === undefined ? undefined : String(arguments[0]);
	    var tag = uid(description);
	    var setter = function (value) {
	      if (this === ObjectPrototype) setter.call(ObjectPrototypeSymbols, value);
	      if (has(this, HIDDEN) && has(this[HIDDEN], tag)) this[HIDDEN][tag] = false;
	      setSymbolDescriptor(this, tag, createPropertyDescriptor(1, value));
	    };
	    if (descriptors && USE_SETTER) setSymbolDescriptor(ObjectPrototype, tag, { configurable: true, set: setter });
	    return wrap$1(tag, description);
	  };

	  redefine($Symbol[PROTOTYPE$1], 'toString', function toString() {
	    return getInternalState$1(this).tag;
	  });

	  redefine($Symbol, 'withoutSetter', function (description) {
	    return wrap$1(uid(description), description);
	  });

	  objectPropertyIsEnumerable.f = $propertyIsEnumerable;
	  objectDefineProperty.f = $defineProperty;
	  objectGetOwnPropertyDescriptor.f = $getOwnPropertyDescriptor;
	  objectGetOwnPropertyNames.f = objectGetOwnPropertyNamesExternal.f = $getOwnPropertyNames;
	  objectGetOwnPropertySymbols.f = $getOwnPropertySymbols;

	  wellKnownSymbolWrapped.f = function (name) {
	    return wrap$1(wellKnownSymbol(name), name);
	  };

	  if (descriptors) {
	    // https://github.com/tc39/proposal-Symbol-description
	    nativeDefineProperty$1($Symbol[PROTOTYPE$1], 'description', {
	      configurable: true,
	      get: function description() {
	        return getInternalState$1(this).description;
	      }
	    });
	    {
	      redefine(ObjectPrototype, 'propertyIsEnumerable', $propertyIsEnumerable, { unsafe: true });
	    }
	  }
	}

	_export({ global: true, wrap: true, forced: !nativeSymbol, sham: !nativeSymbol }, {
	  Symbol: $Symbol
	});

	$forEach(objectKeys(WellKnownSymbolsStore$1), function (name) {
	  defineWellKnownSymbol(name);
	});

	_export({ target: SYMBOL, stat: true, forced: !nativeSymbol }, {
	  // `Symbol.for` method
	  // https://tc39.github.io/ecma262/#sec-symbol.for
	  'for': function (key) {
	    var string = String(key);
	    if (has(StringToSymbolRegistry, string)) return StringToSymbolRegistry[string];
	    var symbol = $Symbol(string);
	    StringToSymbolRegistry[string] = symbol;
	    SymbolToStringRegistry[symbol] = string;
	    return symbol;
	  },
	  // `Symbol.keyFor` method
	  // https://tc39.github.io/ecma262/#sec-symbol.keyfor
	  keyFor: function keyFor(sym) {
	    if (!isSymbol(sym)) throw TypeError(sym + ' is not a symbol');
	    if (has(SymbolToStringRegistry, sym)) return SymbolToStringRegistry[sym];
	  },
	  useSetter: function () { USE_SETTER = true; },
	  useSimple: function () { USE_SETTER = false; }
	});

	_export({ target: 'Object', stat: true, forced: !nativeSymbol, sham: !descriptors }, {
	  // `Object.create` method
	  // https://tc39.github.io/ecma262/#sec-object.create
	  create: $create,
	  // `Object.defineProperty` method
	  // https://tc39.github.io/ecma262/#sec-object.defineproperty
	  defineProperty: $defineProperty,
	  // `Object.defineProperties` method
	  // https://tc39.github.io/ecma262/#sec-object.defineproperties
	  defineProperties: $defineProperties,
	  // `Object.getOwnPropertyDescriptor` method
	  // https://tc39.github.io/ecma262/#sec-object.getownpropertydescriptors
	  getOwnPropertyDescriptor: $getOwnPropertyDescriptor
	});

	_export({ target: 'Object', stat: true, forced: !nativeSymbol }, {
	  // `Object.getOwnPropertyNames` method
	  // https://tc39.github.io/ecma262/#sec-object.getownpropertynames
	  getOwnPropertyNames: $getOwnPropertyNames,
	  // `Object.getOwnPropertySymbols` method
	  // https://tc39.github.io/ecma262/#sec-object.getownpropertysymbols
	  getOwnPropertySymbols: $getOwnPropertySymbols
	});

	// Chrome 38 and 39 `Object.getOwnPropertySymbols` fails on primitives
	// https://bugs.chromium.org/p/v8/issues/detail?id=3443
	_export({ target: 'Object', stat: true, forced: fails(function () { objectGetOwnPropertySymbols.f(1); }) }, {
	  getOwnPropertySymbols: function getOwnPropertySymbols(it) {
	    return objectGetOwnPropertySymbols.f(toObject(it));
	  }
	});

	// `JSON.stringify` method behavior with symbols
	// https://tc39.github.io/ecma262/#sec-json.stringify
	if ($stringify) {
	  var FORCED_JSON_STRINGIFY = !nativeSymbol || fails(function () {
	    var symbol = $Symbol();
	    // MS Edge converts symbol values to JSON as {}
	    return $stringify([symbol]) != '[null]'
	      // WebKit converts symbol values to JSON as null
	      || $stringify({ a: symbol }) != '{}'
	      // V8 throws on boxed symbols
	      || $stringify(Object(symbol)) != '{}';
	  });

	  _export({ target: 'JSON', stat: true, forced: FORCED_JSON_STRINGIFY }, {
	    // eslint-disable-next-line no-unused-vars
	    stringify: function stringify(it, replacer, space) {
	      var args = [it];
	      var index = 1;
	      var $replacer;
	      while (arguments.length > index) args.push(arguments[index++]);
	      $replacer = replacer;
	      if (!isObject(replacer) && it === undefined || isSymbol(it)) return; // IE8 returns string on undefined
	      if (!isArray(replacer)) replacer = function (key, value) {
	        if (typeof $replacer == 'function') value = $replacer.call(this, key, value);
	        if (!isSymbol(value)) return value;
	      };
	      args[1] = replacer;
	      return $stringify.apply(null, args);
	    }
	  });
	}

	// `Symbol.prototype[@@toPrimitive]` method
	// https://tc39.github.io/ecma262/#sec-symbol.prototype-@@toprimitive
	if (!$Symbol[PROTOTYPE$1][TO_PRIMITIVE]) {
	  createNonEnumerableProperty($Symbol[PROTOTYPE$1], TO_PRIMITIVE, $Symbol[PROTOTYPE$1].valueOf);
	}
	// `Symbol.prototype[@@toStringTag]` property
	// https://tc39.github.io/ecma262/#sec-symbol.prototype-@@tostringtag
	setToStringTag($Symbol, SYMBOL);

	hiddenKeys[HIDDEN] = true;

	var defineProperty$2 = objectDefineProperty.f;


	var NativeSymbol = global_1.Symbol;

	if (descriptors && typeof NativeSymbol == 'function' && (!('description' in NativeSymbol.prototype) ||
	  // Safari 12 bug
	  NativeSymbol().description !== undefined
	)) {
	  var EmptyStringDescriptionStore = {};
	  // wrap Symbol constructor for correct work with undefined description
	  var SymbolWrapper = function Symbol() {
	    var description = arguments.length < 1 || arguments[0] === undefined ? undefined : String(arguments[0]);
	    var result = this instanceof SymbolWrapper
	      ? new NativeSymbol(description)
	      // in Edge 13, String(Symbol(undefined)) === 'Symbol(undefined)'
	      : description === undefined ? NativeSymbol() : NativeSymbol(description);
	    if (description === '') EmptyStringDescriptionStore[result] = true;
	    return result;
	  };
	  copyConstructorProperties(SymbolWrapper, NativeSymbol);
	  var symbolPrototype = SymbolWrapper.prototype = NativeSymbol.prototype;
	  symbolPrototype.constructor = SymbolWrapper;

	  var symbolToString = symbolPrototype.toString;
	  var native = String(NativeSymbol('test')) == 'Symbol(test)';
	  var regexp = /^Symbol\((.*)\)[^)]+$/;
	  defineProperty$2(symbolPrototype, 'description', {
	    configurable: true,
	    get: function description() {
	      var symbol = isObject(this) ? this.valueOf() : this;
	      var string = symbolToString.call(symbol);
	      if (has(EmptyStringDescriptionStore, symbol)) return '';
	      var desc = native ? string.slice(7, -1) : string.replace(regexp, '$1');
	      return desc === '' ? undefined : desc;
	    }
	  });

	  _export({ global: true, forced: true }, {
	    Symbol: SymbolWrapper
	  });
	}

	// `Symbol.asyncIterator` well-known symbol
	// https://tc39.github.io/ecma262/#sec-symbol.asynciterator
	defineWellKnownSymbol('asyncIterator');

	// `Symbol.iterator` well-known symbol
	// https://tc39.github.io/ecma262/#sec-symbol.iterator
	defineWellKnownSymbol('iterator');

	// `Symbol.toStringTag` well-known symbol
	// https://tc39.github.io/ecma262/#sec-symbol.tostringtag
	defineWellKnownSymbol('toStringTag');

	var arrayMethodIsStrict = function (METHOD_NAME, argument) {
	  var method = [][METHOD_NAME];
	  return !!method && fails(function () {
	    // eslint-disable-next-line no-useless-call,no-throw-literal
	    method.call(null, argument || function () { throw 1; }, 1);
	  });
	};

	var defineProperty$3 = Object.defineProperty;
	var cache = {};

	var thrower = function (it) { throw it; };

	var arrayMethodUsesToLength = function (METHOD_NAME, options) {
	  if (has(cache, METHOD_NAME)) return cache[METHOD_NAME];
	  if (!options) options = {};
	  var method = [][METHOD_NAME];
	  var ACCESSORS = has(options, 'ACCESSORS') ? options.ACCESSORS : false;
	  var argument0 = has(options, 0) ? options[0] : thrower;
	  var argument1 = has(options, 1) ? options[1] : undefined;

	  return cache[METHOD_NAME] = !!method && !fails(function () {
	    if (ACCESSORS && !descriptors) return true;
	    var O = { length: -1 };

	    if (ACCESSORS) defineProperty$3(O, 1, { enumerable: true, get: thrower });
	    else O[1] = 1;

	    method.call(O, argument0, argument1);
	  });
	};

	var $forEach$1 = arrayIteration.forEach;



	var STRICT_METHOD = arrayMethodIsStrict('forEach');
	var USES_TO_LENGTH = arrayMethodUsesToLength('forEach');

	// `Array.prototype.forEach` method implementation
	// https://tc39.github.io/ecma262/#sec-array.prototype.foreach
	var arrayForEach = (!STRICT_METHOD || !USES_TO_LENGTH) ? function forEach(callbackfn /* , thisArg */) {
	  return $forEach$1(this, callbackfn, arguments.length > 1 ? arguments[1] : undefined);
	} : [].forEach;

	// `Array.prototype.forEach` method
	// https://tc39.github.io/ecma262/#sec-array.prototype.foreach
	_export({ target: 'Array', proto: true, forced: [].forEach != arrayForEach }, {
	  forEach: arrayForEach
	});

	var UNSCOPABLES = wellKnownSymbol('unscopables');
	var ArrayPrototype$1 = Array.prototype;

	// Array.prototype[@@unscopables]
	// https://tc39.github.io/ecma262/#sec-array.prototype-@@unscopables
	if (ArrayPrototype$1[UNSCOPABLES] == undefined) {
	  objectDefineProperty.f(ArrayPrototype$1, UNSCOPABLES, {
	    configurable: true,
	    value: objectCreate(null)
	  });
	}

	// add a key to Array.prototype[@@unscopables]
	var addToUnscopables = function (key) {
	  ArrayPrototype$1[UNSCOPABLES][key] = true;
	};

	var correctPrototypeGetter = !fails(function () {
	  function F() { /* empty */ }
	  F.prototype.constructor = null;
	  return Object.getPrototypeOf(new F()) !== F.prototype;
	});

	var IE_PROTO$1 = sharedKey('IE_PROTO');
	var ObjectPrototype$1 = Object.prototype;

	// `Object.getPrototypeOf` method
	// https://tc39.github.io/ecma262/#sec-object.getprototypeof
	var objectGetPrototypeOf = correctPrototypeGetter ? Object.getPrototypeOf : function (O) {
	  O = toObject(O);
	  if (has(O, IE_PROTO$1)) return O[IE_PROTO$1];
	  if (typeof O.constructor == 'function' && O instanceof O.constructor) {
	    return O.constructor.prototype;
	  } return O instanceof Object ? ObjectPrototype$1 : null;
	};

	var ITERATOR$3 = wellKnownSymbol('iterator');
	var BUGGY_SAFARI_ITERATORS = false;

	var returnThis = function () { return this; };

	// `%IteratorPrototype%` object
	// https://tc39.github.io/ecma262/#sec-%iteratorprototype%-object
	var IteratorPrototype, PrototypeOfArrayIteratorPrototype, arrayIterator;

	if ([].keys) {
	  arrayIterator = [].keys();
	  // Safari 8 has buggy iterators w/o `next`
	  if (!('next' in arrayIterator)) BUGGY_SAFARI_ITERATORS = true;
	  else {
	    PrototypeOfArrayIteratorPrototype = objectGetPrototypeOf(objectGetPrototypeOf(arrayIterator));
	    if (PrototypeOfArrayIteratorPrototype !== Object.prototype) IteratorPrototype = PrototypeOfArrayIteratorPrototype;
	  }
	}

	if (IteratorPrototype == undefined) IteratorPrototype = {};

	// 25.1.2.1.1 %IteratorPrototype%[@@iterator]()
	if (!has(IteratorPrototype, ITERATOR$3)) {
	  createNonEnumerableProperty(IteratorPrototype, ITERATOR$3, returnThis);
	}

	var iteratorsCore = {
	  IteratorPrototype: IteratorPrototype,
	  BUGGY_SAFARI_ITERATORS: BUGGY_SAFARI_ITERATORS
	};

	var IteratorPrototype$1 = iteratorsCore.IteratorPrototype;





	var returnThis$1 = function () { return this; };

	var createIteratorConstructor = function (IteratorConstructor, NAME, next) {
	  var TO_STRING_TAG = NAME + ' Iterator';
	  IteratorConstructor.prototype = objectCreate(IteratorPrototype$1, { next: createPropertyDescriptor(1, next) });
	  setToStringTag(IteratorConstructor, TO_STRING_TAG, false, true);
	  iterators[TO_STRING_TAG] = returnThis$1;
	  return IteratorConstructor;
	};

	var aPossiblePrototype = function (it) {
	  if (!isObject(it) && it !== null) {
	    throw TypeError("Can't set " + String(it) + ' as a prototype');
	  } return it;
	};

	// `Object.setPrototypeOf` method
	// https://tc39.github.io/ecma262/#sec-object.setprototypeof
	// Works with __proto__ only. Old v8 can't work with null proto objects.
	/* eslint-disable no-proto */
	var objectSetPrototypeOf = Object.setPrototypeOf || ('__proto__' in {} ? function () {
	  var CORRECT_SETTER = false;
	  var test = {};
	  var setter;
	  try {
	    setter = Object.getOwnPropertyDescriptor(Object.prototype, '__proto__').set;
	    setter.call(test, []);
	    CORRECT_SETTER = test instanceof Array;
	  } catch (error) { /* empty */ }
	  return function setPrototypeOf(O, proto) {
	    anObject(O);
	    aPossiblePrototype(proto);
	    if (CORRECT_SETTER) setter.call(O, proto);
	    else O.__proto__ = proto;
	    return O;
	  };
	}() : undefined);

	var IteratorPrototype$2 = iteratorsCore.IteratorPrototype;
	var BUGGY_SAFARI_ITERATORS$1 = iteratorsCore.BUGGY_SAFARI_ITERATORS;
	var ITERATOR$4 = wellKnownSymbol('iterator');
	var KEYS = 'keys';
	var VALUES = 'values';
	var ENTRIES = 'entries';

	var returnThis$2 = function () { return this; };

	var defineIterator = function (Iterable, NAME, IteratorConstructor, next, DEFAULT, IS_SET, FORCED) {
	  createIteratorConstructor(IteratorConstructor, NAME, next);

	  var getIterationMethod = function (KIND) {
	    if (KIND === DEFAULT && defaultIterator) return defaultIterator;
	    if (!BUGGY_SAFARI_ITERATORS$1 && KIND in IterablePrototype) return IterablePrototype[KIND];
	    switch (KIND) {
	      case KEYS: return function keys() { return new IteratorConstructor(this, KIND); };
	      case VALUES: return function values() { return new IteratorConstructor(this, KIND); };
	      case ENTRIES: return function entries() { return new IteratorConstructor(this, KIND); };
	    } return function () { return new IteratorConstructor(this); };
	  };

	  var TO_STRING_TAG = NAME + ' Iterator';
	  var INCORRECT_VALUES_NAME = false;
	  var IterablePrototype = Iterable.prototype;
	  var nativeIterator = IterablePrototype[ITERATOR$4]
	    || IterablePrototype['@@iterator']
	    || DEFAULT && IterablePrototype[DEFAULT];
	  var defaultIterator = !BUGGY_SAFARI_ITERATORS$1 && nativeIterator || getIterationMethod(DEFAULT);
	  var anyNativeIterator = NAME == 'Array' ? IterablePrototype.entries || nativeIterator : nativeIterator;
	  var CurrentIteratorPrototype, methods, KEY;

	  // fix native
	  if (anyNativeIterator) {
	    CurrentIteratorPrototype = objectGetPrototypeOf(anyNativeIterator.call(new Iterable()));
	    if (IteratorPrototype$2 !== Object.prototype && CurrentIteratorPrototype.next) {
	      if (objectGetPrototypeOf(CurrentIteratorPrototype) !== IteratorPrototype$2) {
	        if (objectSetPrototypeOf) {
	          objectSetPrototypeOf(CurrentIteratorPrototype, IteratorPrototype$2);
	        } else if (typeof CurrentIteratorPrototype[ITERATOR$4] != 'function') {
	          createNonEnumerableProperty(CurrentIteratorPrototype, ITERATOR$4, returnThis$2);
	        }
	      }
	      // Set @@toStringTag to native iterators
	      setToStringTag(CurrentIteratorPrototype, TO_STRING_TAG, true, true);
	    }
	  }

	  // fix Array#{values, @@iterator}.name in V8 / FF
	  if (DEFAULT == VALUES && nativeIterator && nativeIterator.name !== VALUES) {
	    INCORRECT_VALUES_NAME = true;
	    defaultIterator = function values() { return nativeIterator.call(this); };
	  }

	  // define iterator
	  if (IterablePrototype[ITERATOR$4] !== defaultIterator) {
	    createNonEnumerableProperty(IterablePrototype, ITERATOR$4, defaultIterator);
	  }
	  iterators[NAME] = defaultIterator;

	  // export additional methods
	  if (DEFAULT) {
	    methods = {
	      values: getIterationMethod(VALUES),
	      keys: IS_SET ? defaultIterator : getIterationMethod(KEYS),
	      entries: getIterationMethod(ENTRIES)
	    };
	    if (FORCED) for (KEY in methods) {
	      if (BUGGY_SAFARI_ITERATORS$1 || INCORRECT_VALUES_NAME || !(KEY in IterablePrototype)) {
	        redefine(IterablePrototype, KEY, methods[KEY]);
	      }
	    } else _export({ target: NAME, proto: true, forced: BUGGY_SAFARI_ITERATORS$1 || INCORRECT_VALUES_NAME }, methods);
	  }

	  return methods;
	};

	var ARRAY_ITERATOR = 'Array Iterator';
	var setInternalState$2 = internalState.set;
	var getInternalState$2 = internalState.getterFor(ARRAY_ITERATOR);

	// `Array.prototype.entries` method
	// https://tc39.github.io/ecma262/#sec-array.prototype.entries
	// `Array.prototype.keys` method
	// https://tc39.github.io/ecma262/#sec-array.prototype.keys
	// `Array.prototype.values` method
	// https://tc39.github.io/ecma262/#sec-array.prototype.values
	// `Array.prototype[@@iterator]` method
	// https://tc39.github.io/ecma262/#sec-array.prototype-@@iterator
	// `CreateArrayIterator` internal method
	// https://tc39.github.io/ecma262/#sec-createarrayiterator
	var es_array_iterator = defineIterator(Array, 'Array', function (iterated, kind) {
	  setInternalState$2(this, {
	    type: ARRAY_ITERATOR,
	    target: toIndexedObject(iterated), // target
	    index: 0,                          // next index
	    kind: kind                         // kind
	  });
	// `%ArrayIteratorPrototype%.next` method
	// https://tc39.github.io/ecma262/#sec-%arrayiteratorprototype%.next
	}, function () {
	  var state = getInternalState$2(this);
	  var target = state.target;
	  var kind = state.kind;
	  var index = state.index++;
	  if (!target || index >= target.length) {
	    state.target = undefined;
	    return { value: undefined, done: true };
	  }
	  if (kind == 'keys') return { value: index, done: false };
	  if (kind == 'values') return { value: target[index], done: false };
	  return { value: [index, target[index]], done: false };
	}, 'values');

	// argumentsList[@@iterator] is %ArrayProto_values%
	// https://tc39.github.io/ecma262/#sec-createunmappedargumentsobject
	// https://tc39.github.io/ecma262/#sec-createmappedargumentsobject
	iterators.Arguments = iterators.Array;

	// https://tc39.github.io/ecma262/#sec-array.prototype-@@unscopables
	addToUnscopables('keys');
	addToUnscopables('values');
	addToUnscopables('entries');

	var nativeReverse = [].reverse;
	var test$1 = [1, 2];

	// `Array.prototype.reverse` method
	// https://tc39.github.io/ecma262/#sec-array.prototype.reverse
	// fix for Safari 12.0 bug
	// https://bugs.webkit.org/show_bug.cgi?id=188794
	_export({ target: 'Array', proto: true, forced: String(test$1) === String(test$1.reverse()) }, {
	  reverse: function reverse() {
	    // eslint-disable-next-line no-self-assign
	    if (isArray(this)) this.length = this.length;
	    return nativeReverse.call(this);
	  }
	});

	var createProperty = function (object, key, value) {
	  var propertyKey = toPrimitive(key);
	  if (propertyKey in object) objectDefineProperty.f(object, propertyKey, createPropertyDescriptor(0, value));
	  else object[propertyKey] = value;
	};

	var SPECIES$4 = wellKnownSymbol('species');

	var arrayMethodHasSpeciesSupport = function (METHOD_NAME) {
	  // We can't use this feature detection in V8 since it causes
	  // deoptimization and serious performance degradation
	  // https://github.com/zloirock/core-js/issues/677
	  return engineV8Version >= 51 || !fails(function () {
	    var array = [];
	    var constructor = array.constructor = {};
	    constructor[SPECIES$4] = function () {
	      return { foo: 1 };
	    };
	    return array[METHOD_NAME](Boolean).foo !== 1;
	  });
	};

	var HAS_SPECIES_SUPPORT = arrayMethodHasSpeciesSupport('slice');
	var USES_TO_LENGTH$1 = arrayMethodUsesToLength('slice', { ACCESSORS: true, 0: 0, 1: 2 });

	var SPECIES$5 = wellKnownSymbol('species');
	var nativeSlice = [].slice;
	var max$1 = Math.max;

	// `Array.prototype.slice` method
	// https://tc39.github.io/ecma262/#sec-array.prototype.slice
	// fallback for not array-like ES3 strings and DOM objects
	_export({ target: 'Array', proto: true, forced: !HAS_SPECIES_SUPPORT || !USES_TO_LENGTH$1 }, {
	  slice: function slice(start, end) {
	    var O = toIndexedObject(this);
	    var length = toLength(O.length);
	    var k = toAbsoluteIndex(start, length);
	    var fin = toAbsoluteIndex(end === undefined ? length : end, length);
	    // inline `ArraySpeciesCreate` for usage native `Array#slice` where it's possible
	    var Constructor, result, n;
	    if (isArray(O)) {
	      Constructor = O.constructor;
	      // cross-realm fallback
	      if (typeof Constructor == 'function' && (Constructor === Array || isArray(Constructor.prototype))) {
	        Constructor = undefined;
	      } else if (isObject(Constructor)) {
	        Constructor = Constructor[SPECIES$5];
	        if (Constructor === null) Constructor = undefined;
	      }
	      if (Constructor === Array || Constructor === undefined) {
	        return nativeSlice.call(O, k, fin);
	      }
	    }
	    result = new (Constructor === undefined ? Array : Constructor)(max$1(fin - k, 0));
	    for (n = 0; k < fin; k++, n++) if (k in O) createProperty(result, n, O[k]);
	    result.length = n;
	    return result;
	  }
	});

	var DatePrototype = Date.prototype;
	var INVALID_DATE = 'Invalid Date';
	var TO_STRING = 'toString';
	var nativeDateToString = DatePrototype[TO_STRING];
	var getTime = DatePrototype.getTime;

	// `Date.prototype.toString` method
	// https://tc39.github.io/ecma262/#sec-date.prototype.tostring
	if (new Date(NaN) + '' != INVALID_DATE) {
	  redefine(DatePrototype, TO_STRING, function toString() {
	    var value = getTime.call(this);
	    // eslint-disable-next-line no-self-compare
	    return value === value ? nativeDateToString.call(this) : INVALID_DATE;
	  });
	}

	var defineProperty$4 = objectDefineProperty.f;

	var FunctionPrototype = Function.prototype;
	var FunctionPrototypeToString = FunctionPrototype.toString;
	var nameRE = /^\s*function ([^ (]*)/;
	var NAME = 'name';

	// Function instances `.name` property
	// https://tc39.github.io/ecma262/#sec-function-instances-name
	if (descriptors && !(NAME in FunctionPrototype)) {
	  defineProperty$4(FunctionPrototype, NAME, {
	    configurable: true,
	    get: function () {
	      try {
	        return FunctionPrototypeToString.call(this).match(nameRE)[1];
	      } catch (error) {
	        return '';
	      }
	    }
	  });
	}

	// JSON[@@toStringTag] property
	// https://tc39.github.io/ecma262/#sec-json-@@tostringtag
	setToStringTag(global_1.JSON, 'JSON', true);

	// Math[@@toStringTag] property
	// https://tc39.github.io/ecma262/#sec-math-@@tostringtag
	setToStringTag(Math, 'Math', true);

	// `Object.create` method
	// https://tc39.github.io/ecma262/#sec-object.create
	_export({ target: 'Object', stat: true, sham: !descriptors }, {
	  create: objectCreate
	});

	var FAILS_ON_PRIMITIVES = fails(function () { objectGetPrototypeOf(1); });

	// `Object.getPrototypeOf` method
	// https://tc39.github.io/ecma262/#sec-object.getprototypeof
	_export({ target: 'Object', stat: true, forced: FAILS_ON_PRIMITIVES, sham: !correctPrototypeGetter }, {
	  getPrototypeOf: function getPrototypeOf(it) {
	    return objectGetPrototypeOf(toObject(it));
	  }
	});

	// `Object.setPrototypeOf` method
	// https://tc39.github.io/ecma262/#sec-object.setprototypeof
	_export({ target: 'Object', stat: true }, {
	  setPrototypeOf: objectSetPrototypeOf
	});

	// `RegExp.prototype.flags` getter implementation
	// https://tc39.github.io/ecma262/#sec-get-regexp.prototype.flags
	var regexpFlags = function () {
	  var that = anObject(this);
	  var result = '';
	  if (that.global) result += 'g';
	  if (that.ignoreCase) result += 'i';
	  if (that.multiline) result += 'm';
	  if (that.dotAll) result += 's';
	  if (that.unicode) result += 'u';
	  if (that.sticky) result += 'y';
	  return result;
	};

	var TO_STRING$1 = 'toString';
	var RegExpPrototype = RegExp.prototype;
	var nativeToString = RegExpPrototype[TO_STRING$1];

	var NOT_GENERIC = fails(function () { return nativeToString.call({ source: 'a', flags: 'b' }) != '/a/b'; });
	// FF44- RegExp#toString has a wrong name
	var INCORRECT_NAME = nativeToString.name != TO_STRING$1;

	// `RegExp.prototype.toString` method
	// https://tc39.github.io/ecma262/#sec-regexp.prototype.tostring
	if (NOT_GENERIC || INCORRECT_NAME) {
	  redefine(RegExp.prototype, TO_STRING$1, function toString() {
	    var R = anObject(this);
	    var p = String(R.source);
	    var rf = R.flags;
	    var f = String(rf === undefined && R instanceof RegExp && !('flags' in RegExpPrototype) ? regexpFlags.call(R) : rf);
	    return '/' + p + '/' + f;
	  }, { unsafe: true });
	}

	// `String.prototype.{ codePointAt, at }` methods implementation
	var createMethod$2 = function (CONVERT_TO_STRING) {
	  return function ($this, pos) {
	    var S = String(requireObjectCoercible($this));
	    var position = toInteger(pos);
	    var size = S.length;
	    var first, second;
	    if (position < 0 || position >= size) return CONVERT_TO_STRING ? '' : undefined;
	    first = S.charCodeAt(position);
	    return first < 0xD800 || first > 0xDBFF || position + 1 === size
	      || (second = S.charCodeAt(position + 1)) < 0xDC00 || second > 0xDFFF
	        ? CONVERT_TO_STRING ? S.charAt(position) : first
	        : CONVERT_TO_STRING ? S.slice(position, position + 2) : (first - 0xD800 << 10) + (second - 0xDC00) + 0x10000;
	  };
	};

	var stringMultibyte = {
	  // `String.prototype.codePointAt` method
	  // https://tc39.github.io/ecma262/#sec-string.prototype.codepointat
	  codeAt: createMethod$2(false),
	  // `String.prototype.at` method
	  // https://github.com/mathiasbynens/String.prototype.at
	  charAt: createMethod$2(true)
	};

	var charAt = stringMultibyte.charAt;



	var STRING_ITERATOR = 'String Iterator';
	var setInternalState$3 = internalState.set;
	var getInternalState$3 = internalState.getterFor(STRING_ITERATOR);

	// `String.prototype[@@iterator]` method
	// https://tc39.github.io/ecma262/#sec-string.prototype-@@iterator
	defineIterator(String, 'String', function (iterated) {
	  setInternalState$3(this, {
	    type: STRING_ITERATOR,
	    string: String(iterated),
	    index: 0
	  });
	// `%StringIteratorPrototype%.next` method
	// https://tc39.github.io/ecma262/#sec-%stringiteratorprototype%.next
	}, function next() {
	  var state = getInternalState$3(this);
	  var string = state.string;
	  var index = state.index;
	  var point;
	  if (index >= string.length) return { value: undefined, done: true };
	  point = charAt(string, index);
	  state.index += point.length;
	  return { value: point, done: false };
	});

	// iterable DOM collections
	// flag - `iterable` interface - 'entries', 'keys', 'values', 'forEach' methods
	var domIterables = {
	  CSSRuleList: 0,
	  CSSStyleDeclaration: 0,
	  CSSValueList: 0,
	  ClientRectList: 0,
	  DOMRectList: 0,
	  DOMStringList: 0,
	  DOMTokenList: 1,
	  DataTransferItemList: 0,
	  FileList: 0,
	  HTMLAllCollection: 0,
	  HTMLCollection: 0,
	  HTMLFormElement: 0,
	  HTMLSelectElement: 0,
	  MediaList: 0,
	  MimeTypeArray: 0,
	  NamedNodeMap: 0,
	  NodeList: 1,
	  PaintRequestList: 0,
	  Plugin: 0,
	  PluginArray: 0,
	  SVGLengthList: 0,
	  SVGNumberList: 0,
	  SVGPathSegList: 0,
	  SVGPointList: 0,
	  SVGStringList: 0,
	  SVGTransformList: 0,
	  SourceBufferList: 0,
	  StyleSheetList: 0,
	  TextTrackCueList: 0,
	  TextTrackList: 0,
	  TouchList: 0
	};

	for (var COLLECTION_NAME in domIterables) {
	  var Collection = global_1[COLLECTION_NAME];
	  var CollectionPrototype = Collection && Collection.prototype;
	  // some Chrome versions have non-configurable methods on DOMTokenList
	  if (CollectionPrototype && CollectionPrototype.forEach !== arrayForEach) try {
	    createNonEnumerableProperty(CollectionPrototype, 'forEach', arrayForEach);
	  } catch (error) {
	    CollectionPrototype.forEach = arrayForEach;
	  }
	}

	var ITERATOR$5 = wellKnownSymbol('iterator');
	var TO_STRING_TAG$3 = wellKnownSymbol('toStringTag');
	var ArrayValues = es_array_iterator.values;

	for (var COLLECTION_NAME$1 in domIterables) {
	  var Collection$1 = global_1[COLLECTION_NAME$1];
	  var CollectionPrototype$1 = Collection$1 && Collection$1.prototype;
	  if (CollectionPrototype$1) {
	    // some Chrome versions have non-configurable methods on DOMTokenList
	    if (CollectionPrototype$1[ITERATOR$5] !== ArrayValues) try {
	      createNonEnumerableProperty(CollectionPrototype$1, ITERATOR$5, ArrayValues);
	    } catch (error) {
	      CollectionPrototype$1[ITERATOR$5] = ArrayValues;
	    }
	    if (!CollectionPrototype$1[TO_STRING_TAG$3]) {
	      createNonEnumerableProperty(CollectionPrototype$1, TO_STRING_TAG$3, COLLECTION_NAME$1);
	    }
	    if (domIterables[COLLECTION_NAME$1]) for (var METHOD_NAME in es_array_iterator) {
	      // some Chrome versions have non-configurable methods on DOMTokenList
	      if (CollectionPrototype$1[METHOD_NAME] !== es_array_iterator[METHOD_NAME]) try {
	        createNonEnumerableProperty(CollectionPrototype$1, METHOD_NAME, es_array_iterator[METHOD_NAME]);
	      } catch (error) {
	        CollectionPrototype$1[METHOD_NAME] = es_array_iterator[METHOD_NAME];
	      }
	    }
	  }
	}

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

	function isNativeReflectConstruct() {
	  if (typeof Reflect === "undefined" || !Reflect.construct) return false;
	  if (Reflect.construct.sham) return false;
	  if (typeof Proxy === "function") return true;

	  try {
	    Date.prototype.toString.call(Reflect.construct(Date, [], function () {}));
	    return true;
	  } catch (e) {
	    return false;
	  }
	}

	function _construct(Parent, args, Class) {
	  if (isNativeReflectConstruct()) {
	    _construct = Reflect.construct;
	  } else {
	    _construct = function _construct(Parent, args, Class) {
	      var a = [null];
	      a.push.apply(a, args);
	      var Constructor = Function.bind.apply(Parent, a);
	      var instance = new Constructor();
	      if (Class) _setPrototypeOf(instance, Class.prototype);
	      return instance;
	    };
	  }

	  return _construct.apply(null, arguments);
	}

	function _isNativeFunction(fn) {
	  return Function.toString.call(fn).indexOf("[native code]") !== -1;
	}

	function _wrapNativeSuper(Class) {
	  var _cache = typeof Map === "function" ? new Map() : undefined;

	  _wrapNativeSuper = function _wrapNativeSuper(Class) {
	    if (Class === null || !_isNativeFunction(Class)) return Class;

	    if (typeof Class !== "function") {
	      throw new TypeError("Super expression must either be null or a function");
	    }

	    if (typeof _cache !== "undefined") {
	      if (_cache.has(Class)) return _cache.get(Class);

	      _cache.set(Class, Wrapper);
	    }

	    function Wrapper() {
	      return _construct(Class, arguments, _getPrototypeOf(this).constructor);
	    }

	    Wrapper.prototype = Object.create(Class.prototype, {
	      constructor: {
	        value: Wrapper,
	        enumerable: false,
	        writable: true,
	        configurable: true
	      }
	    });
	    return _setPrototypeOf(Wrapper, Class);
	  };

	  return _wrapNativeSuper(Class);
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

	function _toConsumableArray(arr) {
	  return _arrayWithoutHoles(arr) || _iterableToArray(arr) || _nonIterableSpread();
	}

	function _arrayWithoutHoles(arr) {
	  if (Array.isArray(arr)) {
	    for (var i = 0, arr2 = new Array(arr.length); i < arr.length; i++) arr2[i] = arr[i];

	    return arr2;
	  }
	}

	function _arrayWithHoles(arr) {
	  if (Array.isArray(arr)) return arr;
	}

	function _iterableToArray(iter) {
	  if (Symbol.iterator in Object(iter) || Object.prototype.toString.call(iter) === "[object Arguments]") return Array.from(iter);
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

	function _nonIterableSpread() {
	  throw new TypeError("Invalid attempt to spread non-iterable instance");
	}

	function _nonIterableRest() {
	  throw new TypeError("Invalid attempt to destructure non-iterable instance");
	}

	var runtime_1 = createCommonjsModule(function (module) {
	  /**
	   * Copyright (c) 2014-present, Facebook, Inc.
	   *
	   * This source code is licensed under the MIT license found in the
	   * LICENSE file in the root directory of this source tree.
	   */
	  var runtime = function (exports) {

	    var Op = Object.prototype;
	    var hasOwn = Op.hasOwnProperty;
	    var undefined; // More compressible than void 0.

	    var $Symbol = typeof Symbol === "function" ? Symbol : {};
	    var iteratorSymbol = $Symbol.iterator || "@@iterator";
	    var asyncIteratorSymbol = $Symbol.asyncIterator || "@@asyncIterator";
	    var toStringTagSymbol = $Symbol.toStringTag || "@@toStringTag";

	    function wrap(innerFn, outerFn, self, tryLocsList) {
	      // If outerFn provided and outerFn.prototype is a Generator, then outerFn.prototype instanceof Generator.
	      var protoGenerator = outerFn && outerFn.prototype instanceof Generator ? outerFn : Generator;
	      var generator = Object.create(protoGenerator.prototype);
	      var context = new Context(tryLocsList || []); // The ._invoke method unifies the implementations of the .next,
	      // .throw, and .return methods.

	      generator._invoke = makeInvokeMethod(innerFn, self, context);
	      return generator;
	    }

	    exports.wrap = wrap; // Try/catch helper to minimize deoptimizations. Returns a completion
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

	    exports.isGeneratorFunction = function (genFun) {
	      var ctor = typeof genFun === "function" && genFun.constructor;
	      return ctor ? ctor === GeneratorFunction || // For the native GeneratorFunction constructor, the best we can
	      // do is to check its .name property.
	      (ctor.displayName || ctor.name) === "GeneratorFunction" : false;
	    };

	    exports.mark = function (genFun) {
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


	    exports.awrap = function (arg) {
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

	    exports.AsyncIterator = AsyncIterator; // Note that simple async functions are implemented on top of
	    // AsyncIterator objects; they just return a Promise for the value of
	    // the final result produced by the iterator.

	    exports.async = function (innerFn, outerFn, self, tryLocsList) {
	      var iter = new AsyncIterator(wrap(innerFn, outerFn, self, tryLocsList));
	      return exports.isGeneratorFunction(outerFn) ? iter // If outerFn is a generator, return the full iterator.
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
	          // Note: ["return"] must be used for ES3 parsing compatibility.
	          if (delegate.iterator["return"]) {
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

	    exports.keys = function (object) {
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

	    exports.values = values;

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
	    }; // Regardless of whether this script is executing as a CommonJS module
	    // or not, return the runtime object so that we can declare the variable
	    // regeneratorRuntime in the outer scope, which allows this module to be
	    // injected easily by `bin/regenerator --include-runtime script.js`.

	    return exports;
	  }( // If this script is executing as a CommonJS module, use module.exports
	  // as the regeneratorRuntime namespace. Otherwise create a new empty
	  // object. Either way, the resulting object will be used to initialize
	  // the regeneratorRuntime variable at the top of this file.
	  module.exports);

	  try {
	    regeneratorRuntime = runtime;
	  } catch (accidentalStrictMode) {
	    // This module should not be running in strict mode, so the above
	    // assignment should always work unless something is misconfigured. Just
	    // in case runtime.js accidentally runs in strict mode, we can escape
	    // strict mode using a global Function call. This could conceivably fail
	    // if a Content Security Policy forbids using Function, but in that case
	    // the proper solution is to fix the accidental strict mode problem. If
	    // you've misconfigured your bundler to force strict mode and applied a
	    // CSP to forbid Function, and you're not willing to fix either of those
	    // problems, please detail your unique predicament in a GitHub issue.
	    Function("r", "regeneratorRuntime = r")(runtime);
	  }
	});

	var arrayBufferNative = typeof ArrayBuffer !== 'undefined' && typeof DataView !== 'undefined';

	// `ToIndex` abstract operation
	// https://tc39.github.io/ecma262/#sec-toindex
	var toIndex = function (it) {
	  if (it === undefined) return 0;
	  var number = toInteger(it);
	  var length = toLength(number);
	  if (number !== length) throw RangeError('Wrong length or index');
	  return length;
	};

	// IEEE754 conversions based on https://github.com/feross/ieee754
	// eslint-disable-next-line no-shadow-restricted-names
	var Infinity$1 = 1 / 0;
	var abs = Math.abs;
	var pow = Math.pow;
	var floor$1 = Math.floor;
	var log = Math.log;
	var LN2 = Math.LN2;

	var pack = function (number, mantissaLength, bytes) {
	  var buffer = new Array(bytes);
	  var exponentLength = bytes * 8 - mantissaLength - 1;
	  var eMax = (1 << exponentLength) - 1;
	  var eBias = eMax >> 1;
	  var rt = mantissaLength === 23 ? pow(2, -24) - pow(2, -77) : 0;
	  var sign = number < 0 || number === 0 && 1 / number < 0 ? 1 : 0;
	  var index = 0;
	  var exponent, mantissa, c;
	  number = abs(number);
	  // eslint-disable-next-line no-self-compare
	  if (number != number || number === Infinity$1) {
	    // eslint-disable-next-line no-self-compare
	    mantissa = number != number ? 1 : 0;
	    exponent = eMax;
	  } else {
	    exponent = floor$1(log(number) / LN2);
	    if (number * (c = pow(2, -exponent)) < 1) {
	      exponent--;
	      c *= 2;
	    }
	    if (exponent + eBias >= 1) {
	      number += rt / c;
	    } else {
	      number += rt * pow(2, 1 - eBias);
	    }
	    if (number * c >= 2) {
	      exponent++;
	      c /= 2;
	    }
	    if (exponent + eBias >= eMax) {
	      mantissa = 0;
	      exponent = eMax;
	    } else if (exponent + eBias >= 1) {
	      mantissa = (number * c - 1) * pow(2, mantissaLength);
	      exponent = exponent + eBias;
	    } else {
	      mantissa = number * pow(2, eBias - 1) * pow(2, mantissaLength);
	      exponent = 0;
	    }
	  }
	  for (; mantissaLength >= 8; buffer[index++] = mantissa & 255, mantissa /= 256, mantissaLength -= 8);
	  exponent = exponent << mantissaLength | mantissa;
	  exponentLength += mantissaLength;
	  for (; exponentLength > 0; buffer[index++] = exponent & 255, exponent /= 256, exponentLength -= 8);
	  buffer[--index] |= sign * 128;
	  return buffer;
	};

	var unpack = function (buffer, mantissaLength) {
	  var bytes = buffer.length;
	  var exponentLength = bytes * 8 - mantissaLength - 1;
	  var eMax = (1 << exponentLength) - 1;
	  var eBias = eMax >> 1;
	  var nBits = exponentLength - 7;
	  var index = bytes - 1;
	  var sign = buffer[index--];
	  var exponent = sign & 127;
	  var mantissa;
	  sign >>= 7;
	  for (; nBits > 0; exponent = exponent * 256 + buffer[index], index--, nBits -= 8);
	  mantissa = exponent & (1 << -nBits) - 1;
	  exponent >>= -nBits;
	  nBits += mantissaLength;
	  for (; nBits > 0; mantissa = mantissa * 256 + buffer[index], index--, nBits -= 8);
	  if (exponent === 0) {
	    exponent = 1 - eBias;
	  } else if (exponent === eMax) {
	    return mantissa ? NaN : sign ? -Infinity$1 : Infinity$1;
	  } else {
	    mantissa = mantissa + pow(2, mantissaLength);
	    exponent = exponent - eBias;
	  } return (sign ? -1 : 1) * mantissa * pow(2, exponent - mantissaLength);
	};

	var ieee754 = {
	  pack: pack,
	  unpack: unpack
	};

	// `Array.prototype.fill` method implementation
	// https://tc39.github.io/ecma262/#sec-array.prototype.fill
	var arrayFill = function fill(value /* , start = 0, end = @length */) {
	  var O = toObject(this);
	  var length = toLength(O.length);
	  var argumentsLength = arguments.length;
	  var index = toAbsoluteIndex(argumentsLength > 1 ? arguments[1] : undefined, length);
	  var end = argumentsLength > 2 ? arguments[2] : undefined;
	  var endPos = end === undefined ? length : toAbsoluteIndex(end, length);
	  while (endPos > index) O[index++] = value;
	  return O;
	};

	var getOwnPropertyNames = objectGetOwnPropertyNames.f;
	var defineProperty$5 = objectDefineProperty.f;




	var getInternalState$4 = internalState.get;
	var setInternalState$4 = internalState.set;
	var ARRAY_BUFFER = 'ArrayBuffer';
	var DATA_VIEW = 'DataView';
	var PROTOTYPE$2 = 'prototype';
	var WRONG_LENGTH = 'Wrong length';
	var WRONG_INDEX = 'Wrong index';
	var NativeArrayBuffer = global_1[ARRAY_BUFFER];
	var $ArrayBuffer = NativeArrayBuffer;
	var $DataView = global_1[DATA_VIEW];
	var $DataViewPrototype = $DataView && $DataView[PROTOTYPE$2];
	var ObjectPrototype$2 = Object.prototype;
	var RangeError$1 = global_1.RangeError;

	var packIEEE754 = ieee754.pack;
	var unpackIEEE754 = ieee754.unpack;

	var packInt8 = function (number) {
	  return [number & 0xFF];
	};

	var packInt16 = function (number) {
	  return [number & 0xFF, number >> 8 & 0xFF];
	};

	var packInt32 = function (number) {
	  return [number & 0xFF, number >> 8 & 0xFF, number >> 16 & 0xFF, number >> 24 & 0xFF];
	};

	var unpackInt32 = function (buffer) {
	  return buffer[3] << 24 | buffer[2] << 16 | buffer[1] << 8 | buffer[0];
	};

	var packFloat32 = function (number) {
	  return packIEEE754(number, 23, 4);
	};

	var packFloat64 = function (number) {
	  return packIEEE754(number, 52, 8);
	};

	var addGetter = function (Constructor, key) {
	  defineProperty$5(Constructor[PROTOTYPE$2], key, { get: function () { return getInternalState$4(this)[key]; } });
	};

	var get$1 = function (view, count, index, isLittleEndian) {
	  var intIndex = toIndex(index);
	  var store = getInternalState$4(view);
	  if (intIndex + count > store.byteLength) throw RangeError$1(WRONG_INDEX);
	  var bytes = getInternalState$4(store.buffer).bytes;
	  var start = intIndex + store.byteOffset;
	  var pack = bytes.slice(start, start + count);
	  return isLittleEndian ? pack : pack.reverse();
	};

	var set$3 = function (view, count, index, conversion, value, isLittleEndian) {
	  var intIndex = toIndex(index);
	  var store = getInternalState$4(view);
	  if (intIndex + count > store.byteLength) throw RangeError$1(WRONG_INDEX);
	  var bytes = getInternalState$4(store.buffer).bytes;
	  var start = intIndex + store.byteOffset;
	  var pack = conversion(+value);
	  for (var i = 0; i < count; i++) bytes[start + i] = pack[isLittleEndian ? i : count - i - 1];
	};

	if (!arrayBufferNative) {
	  $ArrayBuffer = function ArrayBuffer(length) {
	    anInstance(this, $ArrayBuffer, ARRAY_BUFFER);
	    var byteLength = toIndex(length);
	    setInternalState$4(this, {
	      bytes: arrayFill.call(new Array(byteLength), 0),
	      byteLength: byteLength
	    });
	    if (!descriptors) this.byteLength = byteLength;
	  };

	  $DataView = function DataView(buffer, byteOffset, byteLength) {
	    anInstance(this, $DataView, DATA_VIEW);
	    anInstance(buffer, $ArrayBuffer, DATA_VIEW);
	    var bufferLength = getInternalState$4(buffer).byteLength;
	    var offset = toInteger(byteOffset);
	    if (offset < 0 || offset > bufferLength) throw RangeError$1('Wrong offset');
	    byteLength = byteLength === undefined ? bufferLength - offset : toLength(byteLength);
	    if (offset + byteLength > bufferLength) throw RangeError$1(WRONG_LENGTH);
	    setInternalState$4(this, {
	      buffer: buffer,
	      byteLength: byteLength,
	      byteOffset: offset
	    });
	    if (!descriptors) {
	      this.buffer = buffer;
	      this.byteLength = byteLength;
	      this.byteOffset = offset;
	    }
	  };

	  if (descriptors) {
	    addGetter($ArrayBuffer, 'byteLength');
	    addGetter($DataView, 'buffer');
	    addGetter($DataView, 'byteLength');
	    addGetter($DataView, 'byteOffset');
	  }

	  redefineAll($DataView[PROTOTYPE$2], {
	    getInt8: function getInt8(byteOffset) {
	      return get$1(this, 1, byteOffset)[0] << 24 >> 24;
	    },
	    getUint8: function getUint8(byteOffset) {
	      return get$1(this, 1, byteOffset)[0];
	    },
	    getInt16: function getInt16(byteOffset /* , littleEndian */) {
	      var bytes = get$1(this, 2, byteOffset, arguments.length > 1 ? arguments[1] : undefined);
	      return (bytes[1] << 8 | bytes[0]) << 16 >> 16;
	    },
	    getUint16: function getUint16(byteOffset /* , littleEndian */) {
	      var bytes = get$1(this, 2, byteOffset, arguments.length > 1 ? arguments[1] : undefined);
	      return bytes[1] << 8 | bytes[0];
	    },
	    getInt32: function getInt32(byteOffset /* , littleEndian */) {
	      return unpackInt32(get$1(this, 4, byteOffset, arguments.length > 1 ? arguments[1] : undefined));
	    },
	    getUint32: function getUint32(byteOffset /* , littleEndian */) {
	      return unpackInt32(get$1(this, 4, byteOffset, arguments.length > 1 ? arguments[1] : undefined)) >>> 0;
	    },
	    getFloat32: function getFloat32(byteOffset /* , littleEndian */) {
	      return unpackIEEE754(get$1(this, 4, byteOffset, arguments.length > 1 ? arguments[1] : undefined), 23);
	    },
	    getFloat64: function getFloat64(byteOffset /* , littleEndian */) {
	      return unpackIEEE754(get$1(this, 8, byteOffset, arguments.length > 1 ? arguments[1] : undefined), 52);
	    },
	    setInt8: function setInt8(byteOffset, value) {
	      set$3(this, 1, byteOffset, packInt8, value);
	    },
	    setUint8: function setUint8(byteOffset, value) {
	      set$3(this, 1, byteOffset, packInt8, value);
	    },
	    setInt16: function setInt16(byteOffset, value /* , littleEndian */) {
	      set$3(this, 2, byteOffset, packInt16, value, arguments.length > 2 ? arguments[2] : undefined);
	    },
	    setUint16: function setUint16(byteOffset, value /* , littleEndian */) {
	      set$3(this, 2, byteOffset, packInt16, value, arguments.length > 2 ? arguments[2] : undefined);
	    },
	    setInt32: function setInt32(byteOffset, value /* , littleEndian */) {
	      set$3(this, 4, byteOffset, packInt32, value, arguments.length > 2 ? arguments[2] : undefined);
	    },
	    setUint32: function setUint32(byteOffset, value /* , littleEndian */) {
	      set$3(this, 4, byteOffset, packInt32, value, arguments.length > 2 ? arguments[2] : undefined);
	    },
	    setFloat32: function setFloat32(byteOffset, value /* , littleEndian */) {
	      set$3(this, 4, byteOffset, packFloat32, value, arguments.length > 2 ? arguments[2] : undefined);
	    },
	    setFloat64: function setFloat64(byteOffset, value /* , littleEndian */) {
	      set$3(this, 8, byteOffset, packFloat64, value, arguments.length > 2 ? arguments[2] : undefined);
	    }
	  });
	} else {
	  if (!fails(function () {
	    NativeArrayBuffer(1);
	  }) || !fails(function () {
	    new NativeArrayBuffer(-1); // eslint-disable-line no-new
	  }) || fails(function () {
	    new NativeArrayBuffer(); // eslint-disable-line no-new
	    new NativeArrayBuffer(1.5); // eslint-disable-line no-new
	    new NativeArrayBuffer(NaN); // eslint-disable-line no-new
	    return NativeArrayBuffer.name != ARRAY_BUFFER;
	  })) {
	    $ArrayBuffer = function ArrayBuffer(length) {
	      anInstance(this, $ArrayBuffer);
	      return new NativeArrayBuffer(toIndex(length));
	    };
	    var ArrayBufferPrototype = $ArrayBuffer[PROTOTYPE$2] = NativeArrayBuffer[PROTOTYPE$2];
	    for (var keys$1 = getOwnPropertyNames(NativeArrayBuffer), j = 0, key; keys$1.length > j;) {
	      if (!((key = keys$1[j++]) in $ArrayBuffer)) {
	        createNonEnumerableProperty($ArrayBuffer, key, NativeArrayBuffer[key]);
	      }
	    }
	    ArrayBufferPrototype.constructor = $ArrayBuffer;
	  }

	  // WebKit bug - the same parent prototype for typed arrays and data view
	  if (objectSetPrototypeOf && objectGetPrototypeOf($DataViewPrototype) !== ObjectPrototype$2) {
	    objectSetPrototypeOf($DataViewPrototype, ObjectPrototype$2);
	  }

	  // iOS Safari 7.x bug
	  var testView = new $DataView(new $ArrayBuffer(2));
	  var nativeSetInt8 = $DataViewPrototype.setInt8;
	  testView.setInt8(0, 2147483648);
	  testView.setInt8(1, 2147483649);
	  if (testView.getInt8(0) || !testView.getInt8(1)) redefineAll($DataViewPrototype, {
	    setInt8: function setInt8(byteOffset, value) {
	      nativeSetInt8.call(this, byteOffset, value << 24 >> 24);
	    },
	    setUint8: function setUint8(byteOffset, value) {
	      nativeSetInt8.call(this, byteOffset, value << 24 >> 24);
	    }
	  }, { unsafe: true });
	}

	setToStringTag($ArrayBuffer, ARRAY_BUFFER);
	setToStringTag($DataView, DATA_VIEW);

	var arrayBuffer = {
	  ArrayBuffer: $ArrayBuffer,
	  DataView: $DataView
	};

	var ArrayBuffer$1 = arrayBuffer.ArrayBuffer;
	var DataView$1 = arrayBuffer.DataView;
	var nativeArrayBufferSlice = ArrayBuffer$1.prototype.slice;

	var INCORRECT_SLICE = fails(function () {
	  return !new ArrayBuffer$1(2).slice(1, undefined).byteLength;
	});

	// `ArrayBuffer.prototype.slice` method
	// https://tc39.github.io/ecma262/#sec-arraybuffer.prototype.slice
	_export({ target: 'ArrayBuffer', proto: true, unsafe: true, forced: INCORRECT_SLICE }, {
	  slice: function slice(start, end) {
	    if (nativeArrayBufferSlice !== undefined && end === undefined) {
	      return nativeArrayBufferSlice.call(anObject(this), start); // FF fix
	    }
	    var length = anObject(this).byteLength;
	    var first = toAbsoluteIndex(start, length);
	    var fin = toAbsoluteIndex(end === undefined ? length : end, length);
	    var result = new (speciesConstructor(this, ArrayBuffer$1))(toLength(fin - first));
	    var viewSource = new DataView$1(this);
	    var viewTarget = new DataView$1(result);
	    var index = 0;
	    while (first < fin) {
	      viewTarget.setUint8(index++, viewSource.getUint8(first++));
	    } return result;
	  }
	});

	var slice$1 = [].slice;
	var factories = {};

	var construct = function (C, argsLength, args) {
	  if (!(argsLength in factories)) {
	    for (var list = [], i = 0; i < argsLength; i++) list[i] = 'a[' + i + ']';
	    // eslint-disable-next-line no-new-func
	    factories[argsLength] = Function('C,a', 'return new C(' + list.join(',') + ')');
	  } return factories[argsLength](C, args);
	};

	// `Function.prototype.bind` method implementation
	// https://tc39.github.io/ecma262/#sec-function.prototype.bind
	var functionBind = Function.bind || function bind(that /* , ...args */) {
	  var fn = aFunction$1(this);
	  var partArgs = slice$1.call(arguments, 1);
	  var boundFunction = function bound(/* args... */) {
	    var args = partArgs.concat(slice$1.call(arguments));
	    return this instanceof boundFunction ? construct(fn, args.length, args) : fn.apply(that, args);
	  };
	  if (isObject(fn.prototype)) boundFunction.prototype = fn.prototype;
	  return boundFunction;
	};

	// `Function.prototype.bind` method
	// https://tc39.github.io/ecma262/#sec-function.prototype.bind
	_export({ target: 'Function', proto: true }, {
	  bind: functionBind
	});

	var defineProperty$6 = objectDefineProperty.f;





	var Int8Array$1 = global_1.Int8Array;
	var Int8ArrayPrototype = Int8Array$1 && Int8Array$1.prototype;
	var Uint8ClampedArray = global_1.Uint8ClampedArray;
	var Uint8ClampedArrayPrototype = Uint8ClampedArray && Uint8ClampedArray.prototype;
	var TypedArray = Int8Array$1 && objectGetPrototypeOf(Int8Array$1);
	var TypedArrayPrototype = Int8ArrayPrototype && objectGetPrototypeOf(Int8ArrayPrototype);
	var ObjectPrototype$3 = Object.prototype;
	var isPrototypeOf = ObjectPrototype$3.isPrototypeOf;

	var TO_STRING_TAG$4 = wellKnownSymbol('toStringTag');
	var TYPED_ARRAY_TAG = uid('TYPED_ARRAY_TAG');
	// Fixing native typed arrays in Opera Presto crashes the browser, see #595
	var NATIVE_ARRAY_BUFFER_VIEWS = arrayBufferNative && !!objectSetPrototypeOf && classof(global_1.opera) !== 'Opera';
	var TYPED_ARRAY_TAG_REQIRED = false;
	var NAME$1;

	var TypedArrayConstructorsList = {
	  Int8Array: 1,
	  Uint8Array: 1,
	  Uint8ClampedArray: 1,
	  Int16Array: 2,
	  Uint16Array: 2,
	  Int32Array: 4,
	  Uint32Array: 4,
	  Float32Array: 4,
	  Float64Array: 8
	};

	var isView = function isView(it) {
	  var klass = classof(it);
	  return klass === 'DataView' || has(TypedArrayConstructorsList, klass);
	};

	var isTypedArray = function (it) {
	  return isObject(it) && has(TypedArrayConstructorsList, classof(it));
	};

	var aTypedArray = function (it) {
	  if (isTypedArray(it)) return it;
	  throw TypeError('Target is not a typed array');
	};

	var aTypedArrayConstructor = function (C) {
	  if (objectSetPrototypeOf) {
	    if (isPrototypeOf.call(TypedArray, C)) return C;
	  } else for (var ARRAY in TypedArrayConstructorsList) if (has(TypedArrayConstructorsList, NAME$1)) {
	    var TypedArrayConstructor = global_1[ARRAY];
	    if (TypedArrayConstructor && (C === TypedArrayConstructor || isPrototypeOf.call(TypedArrayConstructor, C))) {
	      return C;
	    }
	  } throw TypeError('Target is not a typed array constructor');
	};

	var exportTypedArrayMethod = function (KEY, property, forced) {
	  if (!descriptors) return;
	  if (forced) for (var ARRAY in TypedArrayConstructorsList) {
	    var TypedArrayConstructor = global_1[ARRAY];
	    if (TypedArrayConstructor && has(TypedArrayConstructor.prototype, KEY)) {
	      delete TypedArrayConstructor.prototype[KEY];
	    }
	  }
	  if (!TypedArrayPrototype[KEY] || forced) {
	    redefine(TypedArrayPrototype, KEY, forced ? property
	      : NATIVE_ARRAY_BUFFER_VIEWS && Int8ArrayPrototype[KEY] || property);
	  }
	};

	var exportTypedArrayStaticMethod = function (KEY, property, forced) {
	  var ARRAY, TypedArrayConstructor;
	  if (!descriptors) return;
	  if (objectSetPrototypeOf) {
	    if (forced) for (ARRAY in TypedArrayConstructorsList) {
	      TypedArrayConstructor = global_1[ARRAY];
	      if (TypedArrayConstructor && has(TypedArrayConstructor, KEY)) {
	        delete TypedArrayConstructor[KEY];
	      }
	    }
	    if (!TypedArray[KEY] || forced) {
	      // V8 ~ Chrome 49-50 `%TypedArray%` methods are non-writable non-configurable
	      try {
	        return redefine(TypedArray, KEY, forced ? property : NATIVE_ARRAY_BUFFER_VIEWS && Int8Array$1[KEY] || property);
	      } catch (error) { /* empty */ }
	    } else return;
	  }
	  for (ARRAY in TypedArrayConstructorsList) {
	    TypedArrayConstructor = global_1[ARRAY];
	    if (TypedArrayConstructor && (!TypedArrayConstructor[KEY] || forced)) {
	      redefine(TypedArrayConstructor, KEY, property);
	    }
	  }
	};

	for (NAME$1 in TypedArrayConstructorsList) {
	  if (!global_1[NAME$1]) NATIVE_ARRAY_BUFFER_VIEWS = false;
	}

	// WebKit bug - typed arrays constructors prototype is Object.prototype
	if (!NATIVE_ARRAY_BUFFER_VIEWS || typeof TypedArray != 'function' || TypedArray === Function.prototype) {
	  // eslint-disable-next-line no-shadow
	  TypedArray = function TypedArray() {
	    throw TypeError('Incorrect invocation');
	  };
	  if (NATIVE_ARRAY_BUFFER_VIEWS) for (NAME$1 in TypedArrayConstructorsList) {
	    if (global_1[NAME$1]) objectSetPrototypeOf(global_1[NAME$1], TypedArray);
	  }
	}

	if (!NATIVE_ARRAY_BUFFER_VIEWS || !TypedArrayPrototype || TypedArrayPrototype === ObjectPrototype$3) {
	  TypedArrayPrototype = TypedArray.prototype;
	  if (NATIVE_ARRAY_BUFFER_VIEWS) for (NAME$1 in TypedArrayConstructorsList) {
	    if (global_1[NAME$1]) objectSetPrototypeOf(global_1[NAME$1].prototype, TypedArrayPrototype);
	  }
	}

	// WebKit bug - one more object in Uint8ClampedArray prototype chain
	if (NATIVE_ARRAY_BUFFER_VIEWS && objectGetPrototypeOf(Uint8ClampedArrayPrototype) !== TypedArrayPrototype) {
	  objectSetPrototypeOf(Uint8ClampedArrayPrototype, TypedArrayPrototype);
	}

	if (descriptors && !has(TypedArrayPrototype, TO_STRING_TAG$4)) {
	  TYPED_ARRAY_TAG_REQIRED = true;
	  defineProperty$6(TypedArrayPrototype, TO_STRING_TAG$4, { get: function () {
	    return isObject(this) ? this[TYPED_ARRAY_TAG] : undefined;
	  } });
	  for (NAME$1 in TypedArrayConstructorsList) if (global_1[NAME$1]) {
	    createNonEnumerableProperty(global_1[NAME$1], TYPED_ARRAY_TAG, NAME$1);
	  }
	}

	var arrayBufferViewCore = {
	  NATIVE_ARRAY_BUFFER_VIEWS: NATIVE_ARRAY_BUFFER_VIEWS,
	  TYPED_ARRAY_TAG: TYPED_ARRAY_TAG_REQIRED && TYPED_ARRAY_TAG,
	  aTypedArray: aTypedArray,
	  aTypedArrayConstructor: aTypedArrayConstructor,
	  exportTypedArrayMethod: exportTypedArrayMethod,
	  exportTypedArrayStaticMethod: exportTypedArrayStaticMethod,
	  isView: isView,
	  isTypedArray: isTypedArray,
	  TypedArray: TypedArray,
	  TypedArrayPrototype: TypedArrayPrototype
	};

	/* eslint-disable no-new */



	var NATIVE_ARRAY_BUFFER_VIEWS$1 = arrayBufferViewCore.NATIVE_ARRAY_BUFFER_VIEWS;

	var ArrayBuffer$2 = global_1.ArrayBuffer;
	var Int8Array$2 = global_1.Int8Array;

	var typedArrayConstructorsRequireWrappers = !NATIVE_ARRAY_BUFFER_VIEWS$1 || !fails(function () {
	  Int8Array$2(1);
	}) || !fails(function () {
	  new Int8Array$2(-1);
	}) || !checkCorrectnessOfIteration(function (iterable) {
	  new Int8Array$2();
	  new Int8Array$2(null);
	  new Int8Array$2(1.5);
	  new Int8Array$2(iterable);
	}, true) || fails(function () {
	  // Safari (11+) bug - a reason why even Safari 13 should load a typed array polyfill
	  return new Int8Array$2(new ArrayBuffer$2(2), 1, undefined).length !== 1;
	});

	var toPositiveInteger = function (it) {
	  var result = toInteger(it);
	  if (result < 0) throw RangeError("The argument can't be less than 0");
	  return result;
	};

	var toOffset = function (it, BYTES) {
	  var offset = toPositiveInteger(it);
	  if (offset % BYTES) throw RangeError('Wrong offset');
	  return offset;
	};

	var aTypedArrayConstructor$1 = arrayBufferViewCore.aTypedArrayConstructor;

	var typedArrayFrom = function from(source /* , mapfn, thisArg */) {
	  var O = toObject(source);
	  var argumentsLength = arguments.length;
	  var mapfn = argumentsLength > 1 ? arguments[1] : undefined;
	  var mapping = mapfn !== undefined;
	  var iteratorMethod = getIteratorMethod(O);
	  var i, length, result, step, iterator, next;
	  if (iteratorMethod != undefined && !isArrayIteratorMethod(iteratorMethod)) {
	    iterator = iteratorMethod.call(O);
	    next = iterator.next;
	    O = [];
	    while (!(step = next.call(iterator)).done) {
	      O.push(step.value);
	    }
	  }
	  if (mapping && argumentsLength > 2) {
	    mapfn = functionBindContext(mapfn, arguments[2], 2);
	  }
	  length = toLength(O.length);
	  result = new (aTypedArrayConstructor$1(this))(length);
	  for (i = 0; length > i; i++) {
	    result[i] = mapping ? mapfn(O[i], i) : O[i];
	  }
	  return result;
	};

	// makes subclassing work correct for wrapped built-ins
	var inheritIfRequired = function ($this, dummy, Wrapper) {
	  var NewTarget, NewTargetPrototype;
	  if (
	    // it can work only with native `setPrototypeOf`
	    objectSetPrototypeOf &&
	    // we haven't completely correct pre-ES6 way for getting `new.target`, so use this
	    typeof (NewTarget = dummy.constructor) == 'function' &&
	    NewTarget !== Wrapper &&
	    isObject(NewTargetPrototype = NewTarget.prototype) &&
	    NewTargetPrototype !== Wrapper.prototype
	  ) objectSetPrototypeOf($this, NewTargetPrototype);
	  return $this;
	};

	var typedArrayConstructor = createCommonjsModule(function (module) {


















	var getOwnPropertyNames = objectGetOwnPropertyNames.f;

	var forEach = arrayIteration.forEach;






	var getInternalState = internalState.get;
	var setInternalState = internalState.set;
	var nativeDefineProperty = objectDefineProperty.f;
	var nativeGetOwnPropertyDescriptor = objectGetOwnPropertyDescriptor.f;
	var round = Math.round;
	var RangeError = global_1.RangeError;
	var ArrayBuffer = arrayBuffer.ArrayBuffer;
	var DataView = arrayBuffer.DataView;
	var NATIVE_ARRAY_BUFFER_VIEWS = arrayBufferViewCore.NATIVE_ARRAY_BUFFER_VIEWS;
	var TYPED_ARRAY_TAG = arrayBufferViewCore.TYPED_ARRAY_TAG;
	var TypedArray = arrayBufferViewCore.TypedArray;
	var TypedArrayPrototype = arrayBufferViewCore.TypedArrayPrototype;
	var aTypedArrayConstructor = arrayBufferViewCore.aTypedArrayConstructor;
	var isTypedArray = arrayBufferViewCore.isTypedArray;
	var BYTES_PER_ELEMENT = 'BYTES_PER_ELEMENT';
	var WRONG_LENGTH = 'Wrong length';

	var fromList = function (C, list) {
	  var index = 0;
	  var length = list.length;
	  var result = new (aTypedArrayConstructor(C))(length);
	  while (length > index) result[index] = list[index++];
	  return result;
	};

	var addGetter = function (it, key) {
	  nativeDefineProperty(it, key, { get: function () {
	    return getInternalState(this)[key];
	  } });
	};

	var isArrayBuffer = function (it) {
	  var klass;
	  return it instanceof ArrayBuffer || (klass = classof(it)) == 'ArrayBuffer' || klass == 'SharedArrayBuffer';
	};

	var isTypedArrayIndex = function (target, key) {
	  return isTypedArray(target)
	    && typeof key != 'symbol'
	    && key in target
	    && String(+key) == String(key);
	};

	var wrappedGetOwnPropertyDescriptor = function getOwnPropertyDescriptor(target, key) {
	  return isTypedArrayIndex(target, key = toPrimitive(key, true))
	    ? createPropertyDescriptor(2, target[key])
	    : nativeGetOwnPropertyDescriptor(target, key);
	};

	var wrappedDefineProperty = function defineProperty(target, key, descriptor) {
	  if (isTypedArrayIndex(target, key = toPrimitive(key, true))
	    && isObject(descriptor)
	    && has(descriptor, 'value')
	    && !has(descriptor, 'get')
	    && !has(descriptor, 'set')
	    // TODO: add validation descriptor w/o calling accessors
	    && !descriptor.configurable
	    && (!has(descriptor, 'writable') || descriptor.writable)
	    && (!has(descriptor, 'enumerable') || descriptor.enumerable)
	  ) {
	    target[key] = descriptor.value;
	    return target;
	  } return nativeDefineProperty(target, key, descriptor);
	};

	if (descriptors) {
	  if (!NATIVE_ARRAY_BUFFER_VIEWS) {
	    objectGetOwnPropertyDescriptor.f = wrappedGetOwnPropertyDescriptor;
	    objectDefineProperty.f = wrappedDefineProperty;
	    addGetter(TypedArrayPrototype, 'buffer');
	    addGetter(TypedArrayPrototype, 'byteOffset');
	    addGetter(TypedArrayPrototype, 'byteLength');
	    addGetter(TypedArrayPrototype, 'length');
	  }

	  _export({ target: 'Object', stat: true, forced: !NATIVE_ARRAY_BUFFER_VIEWS }, {
	    getOwnPropertyDescriptor: wrappedGetOwnPropertyDescriptor,
	    defineProperty: wrappedDefineProperty
	  });

	  module.exports = function (TYPE, wrapper, CLAMPED) {
	    var BYTES = TYPE.match(/\d+$/)[0] / 8;
	    var CONSTRUCTOR_NAME = TYPE + (CLAMPED ? 'Clamped' : '') + 'Array';
	    var GETTER = 'get' + TYPE;
	    var SETTER = 'set' + TYPE;
	    var NativeTypedArrayConstructor = global_1[CONSTRUCTOR_NAME];
	    var TypedArrayConstructor = NativeTypedArrayConstructor;
	    var TypedArrayConstructorPrototype = TypedArrayConstructor && TypedArrayConstructor.prototype;
	    var exported = {};

	    var getter = function (that, index) {
	      var data = getInternalState(that);
	      return data.view[GETTER](index * BYTES + data.byteOffset, true);
	    };

	    var setter = function (that, index, value) {
	      var data = getInternalState(that);
	      if (CLAMPED) value = (value = round(value)) < 0 ? 0 : value > 0xFF ? 0xFF : value & 0xFF;
	      data.view[SETTER](index * BYTES + data.byteOffset, value, true);
	    };

	    var addElement = function (that, index) {
	      nativeDefineProperty(that, index, {
	        get: function () {
	          return getter(this, index);
	        },
	        set: function (value) {
	          return setter(this, index, value);
	        },
	        enumerable: true
	      });
	    };

	    if (!NATIVE_ARRAY_BUFFER_VIEWS) {
	      TypedArrayConstructor = wrapper(function (that, data, offset, $length) {
	        anInstance(that, TypedArrayConstructor, CONSTRUCTOR_NAME);
	        var index = 0;
	        var byteOffset = 0;
	        var buffer, byteLength, length;
	        if (!isObject(data)) {
	          length = toIndex(data);
	          byteLength = length * BYTES;
	          buffer = new ArrayBuffer(byteLength);
	        } else if (isArrayBuffer(data)) {
	          buffer = data;
	          byteOffset = toOffset(offset, BYTES);
	          var $len = data.byteLength;
	          if ($length === undefined) {
	            if ($len % BYTES) throw RangeError(WRONG_LENGTH);
	            byteLength = $len - byteOffset;
	            if (byteLength < 0) throw RangeError(WRONG_LENGTH);
	          } else {
	            byteLength = toLength($length) * BYTES;
	            if (byteLength + byteOffset > $len) throw RangeError(WRONG_LENGTH);
	          }
	          length = byteLength / BYTES;
	        } else if (isTypedArray(data)) {
	          return fromList(TypedArrayConstructor, data);
	        } else {
	          return typedArrayFrom.call(TypedArrayConstructor, data);
	        }
	        setInternalState(that, {
	          buffer: buffer,
	          byteOffset: byteOffset,
	          byteLength: byteLength,
	          length: length,
	          view: new DataView(buffer)
	        });
	        while (index < length) addElement(that, index++);
	      });

	      if (objectSetPrototypeOf) objectSetPrototypeOf(TypedArrayConstructor, TypedArray);
	      TypedArrayConstructorPrototype = TypedArrayConstructor.prototype = objectCreate(TypedArrayPrototype);
	    } else if (typedArrayConstructorsRequireWrappers) {
	      TypedArrayConstructor = wrapper(function (dummy, data, typedArrayOffset, $length) {
	        anInstance(dummy, TypedArrayConstructor, CONSTRUCTOR_NAME);
	        return inheritIfRequired(function () {
	          if (!isObject(data)) return new NativeTypedArrayConstructor(toIndex(data));
	          if (isArrayBuffer(data)) return $length !== undefined
	            ? new NativeTypedArrayConstructor(data, toOffset(typedArrayOffset, BYTES), $length)
	            : typedArrayOffset !== undefined
	              ? new NativeTypedArrayConstructor(data, toOffset(typedArrayOffset, BYTES))
	              : new NativeTypedArrayConstructor(data);
	          if (isTypedArray(data)) return fromList(TypedArrayConstructor, data);
	          return typedArrayFrom.call(TypedArrayConstructor, data);
	        }(), dummy, TypedArrayConstructor);
	      });

	      if (objectSetPrototypeOf) objectSetPrototypeOf(TypedArrayConstructor, TypedArray);
	      forEach(getOwnPropertyNames(NativeTypedArrayConstructor), function (key) {
	        if (!(key in TypedArrayConstructor)) {
	          createNonEnumerableProperty(TypedArrayConstructor, key, NativeTypedArrayConstructor[key]);
	        }
	      });
	      TypedArrayConstructor.prototype = TypedArrayConstructorPrototype;
	    }

	    if (TypedArrayConstructorPrototype.constructor !== TypedArrayConstructor) {
	      createNonEnumerableProperty(TypedArrayConstructorPrototype, 'constructor', TypedArrayConstructor);
	    }

	    if (TYPED_ARRAY_TAG) {
	      createNonEnumerableProperty(TypedArrayConstructorPrototype, TYPED_ARRAY_TAG, CONSTRUCTOR_NAME);
	    }

	    exported[CONSTRUCTOR_NAME] = TypedArrayConstructor;

	    _export({
	      global: true, forced: TypedArrayConstructor != NativeTypedArrayConstructor, sham: !NATIVE_ARRAY_BUFFER_VIEWS
	    }, exported);

	    if (!(BYTES_PER_ELEMENT in TypedArrayConstructor)) {
	      createNonEnumerableProperty(TypedArrayConstructor, BYTES_PER_ELEMENT, BYTES);
	    }

	    if (!(BYTES_PER_ELEMENT in TypedArrayConstructorPrototype)) {
	      createNonEnumerableProperty(TypedArrayConstructorPrototype, BYTES_PER_ELEMENT, BYTES);
	    }

	    setSpecies(CONSTRUCTOR_NAME);
	  };
	} else module.exports = function () { /* empty */ };
	});

	// `Uint8Array` constructor
	// https://tc39.github.io/ecma262/#sec-typedarray-objects
	typedArrayConstructor('Uint8', function (init) {
	  return function Uint8Array(data, byteOffset, length) {
	    return init(this, data, byteOffset, length);
	  };
	});

	var min$2 = Math.min;

	// `Array.prototype.copyWithin` method implementation
	// https://tc39.github.io/ecma262/#sec-array.prototype.copywithin
	var arrayCopyWithin = [].copyWithin || function copyWithin(target /* = 0 */, start /* = 0, end = @length */) {
	  var O = toObject(this);
	  var len = toLength(O.length);
	  var to = toAbsoluteIndex(target, len);
	  var from = toAbsoluteIndex(start, len);
	  var end = arguments.length > 2 ? arguments[2] : undefined;
	  var count = min$2((end === undefined ? len : toAbsoluteIndex(end, len)) - from, len - to);
	  var inc = 1;
	  if (from < to && to < from + count) {
	    inc = -1;
	    from += count - 1;
	    to += count - 1;
	  }
	  while (count-- > 0) {
	    if (from in O) O[to] = O[from];
	    else delete O[to];
	    to += inc;
	    from += inc;
	  } return O;
	};

	var aTypedArray$1 = arrayBufferViewCore.aTypedArray;
	var exportTypedArrayMethod$1 = arrayBufferViewCore.exportTypedArrayMethod;

	// `%TypedArray%.prototype.copyWithin` method
	// https://tc39.github.io/ecma262/#sec-%typedarray%.prototype.copywithin
	exportTypedArrayMethod$1('copyWithin', function copyWithin(target, start /* , end */) {
	  return arrayCopyWithin.call(aTypedArray$1(this), target, start, arguments.length > 2 ? arguments[2] : undefined);
	});

	var $every = arrayIteration.every;

	var aTypedArray$2 = arrayBufferViewCore.aTypedArray;
	var exportTypedArrayMethod$2 = arrayBufferViewCore.exportTypedArrayMethod;

	// `%TypedArray%.prototype.every` method
	// https://tc39.github.io/ecma262/#sec-%typedarray%.prototype.every
	exportTypedArrayMethod$2('every', function every(callbackfn /* , thisArg */) {
	  return $every(aTypedArray$2(this), callbackfn, arguments.length > 1 ? arguments[1] : undefined);
	});

	var aTypedArray$3 = arrayBufferViewCore.aTypedArray;
	var exportTypedArrayMethod$3 = arrayBufferViewCore.exportTypedArrayMethod;

	// `%TypedArray%.prototype.fill` method
	// https://tc39.github.io/ecma262/#sec-%typedarray%.prototype.fill
	// eslint-disable-next-line no-unused-vars
	exportTypedArrayMethod$3('fill', function fill(value /* , start, end */) {
	  return arrayFill.apply(aTypedArray$3(this), arguments);
	});

	var $filter = arrayIteration.filter;


	var aTypedArray$4 = arrayBufferViewCore.aTypedArray;
	var aTypedArrayConstructor$2 = arrayBufferViewCore.aTypedArrayConstructor;
	var exportTypedArrayMethod$4 = arrayBufferViewCore.exportTypedArrayMethod;

	// `%TypedArray%.prototype.filter` method
	// https://tc39.github.io/ecma262/#sec-%typedarray%.prototype.filter
	exportTypedArrayMethod$4('filter', function filter(callbackfn /* , thisArg */) {
	  var list = $filter(aTypedArray$4(this), callbackfn, arguments.length > 1 ? arguments[1] : undefined);
	  var C = speciesConstructor(this, this.constructor);
	  var index = 0;
	  var length = list.length;
	  var result = new (aTypedArrayConstructor$2(C))(length);
	  while (length > index) result[index] = list[index++];
	  return result;
	});

	var $find = arrayIteration.find;

	var aTypedArray$5 = arrayBufferViewCore.aTypedArray;
	var exportTypedArrayMethod$5 = arrayBufferViewCore.exportTypedArrayMethod;

	// `%TypedArray%.prototype.find` method
	// https://tc39.github.io/ecma262/#sec-%typedarray%.prototype.find
	exportTypedArrayMethod$5('find', function find(predicate /* , thisArg */) {
	  return $find(aTypedArray$5(this), predicate, arguments.length > 1 ? arguments[1] : undefined);
	});

	var $findIndex = arrayIteration.findIndex;

	var aTypedArray$6 = arrayBufferViewCore.aTypedArray;
	var exportTypedArrayMethod$6 = arrayBufferViewCore.exportTypedArrayMethod;

	// `%TypedArray%.prototype.findIndex` method
	// https://tc39.github.io/ecma262/#sec-%typedarray%.prototype.findindex
	exportTypedArrayMethod$6('findIndex', function findIndex(predicate /* , thisArg */) {
	  return $findIndex(aTypedArray$6(this), predicate, arguments.length > 1 ? arguments[1] : undefined);
	});

	var $forEach$2 = arrayIteration.forEach;

	var aTypedArray$7 = arrayBufferViewCore.aTypedArray;
	var exportTypedArrayMethod$7 = arrayBufferViewCore.exportTypedArrayMethod;

	// `%TypedArray%.prototype.forEach` method
	// https://tc39.github.io/ecma262/#sec-%typedarray%.prototype.foreach
	exportTypedArrayMethod$7('forEach', function forEach(callbackfn /* , thisArg */) {
	  $forEach$2(aTypedArray$7(this), callbackfn, arguments.length > 1 ? arguments[1] : undefined);
	});

	var $includes = arrayIncludes.includes;

	var aTypedArray$8 = arrayBufferViewCore.aTypedArray;
	var exportTypedArrayMethod$8 = arrayBufferViewCore.exportTypedArrayMethod;

	// `%TypedArray%.prototype.includes` method
	// https://tc39.github.io/ecma262/#sec-%typedarray%.prototype.includes
	exportTypedArrayMethod$8('includes', function includes(searchElement /* , fromIndex */) {
	  return $includes(aTypedArray$8(this), searchElement, arguments.length > 1 ? arguments[1] : undefined);
	});

	var $indexOf = arrayIncludes.indexOf;

	var aTypedArray$9 = arrayBufferViewCore.aTypedArray;
	var exportTypedArrayMethod$9 = arrayBufferViewCore.exportTypedArrayMethod;

	// `%TypedArray%.prototype.indexOf` method
	// https://tc39.github.io/ecma262/#sec-%typedarray%.prototype.indexof
	exportTypedArrayMethod$9('indexOf', function indexOf(searchElement /* , fromIndex */) {
	  return $indexOf(aTypedArray$9(this), searchElement, arguments.length > 1 ? arguments[1] : undefined);
	});

	var ITERATOR$6 = wellKnownSymbol('iterator');
	var Uint8Array$1 = global_1.Uint8Array;
	var arrayValues = es_array_iterator.values;
	var arrayKeys = es_array_iterator.keys;
	var arrayEntries = es_array_iterator.entries;
	var aTypedArray$a = arrayBufferViewCore.aTypedArray;
	var exportTypedArrayMethod$a = arrayBufferViewCore.exportTypedArrayMethod;
	var nativeTypedArrayIterator = Uint8Array$1 && Uint8Array$1.prototype[ITERATOR$6];

	var CORRECT_ITER_NAME = !!nativeTypedArrayIterator
	  && (nativeTypedArrayIterator.name == 'values' || nativeTypedArrayIterator.name == undefined);

	var typedArrayValues = function values() {
	  return arrayValues.call(aTypedArray$a(this));
	};

	// `%TypedArray%.prototype.entries` method
	// https://tc39.github.io/ecma262/#sec-%typedarray%.prototype.entries
	exportTypedArrayMethod$a('entries', function entries() {
	  return arrayEntries.call(aTypedArray$a(this));
	});
	// `%TypedArray%.prototype.keys` method
	// https://tc39.github.io/ecma262/#sec-%typedarray%.prototype.keys
	exportTypedArrayMethod$a('keys', function keys() {
	  return arrayKeys.call(aTypedArray$a(this));
	});
	// `%TypedArray%.prototype.values` method
	// https://tc39.github.io/ecma262/#sec-%typedarray%.prototype.values
	exportTypedArrayMethod$a('values', typedArrayValues, !CORRECT_ITER_NAME);
	// `%TypedArray%.prototype[@@iterator]` method
	// https://tc39.github.io/ecma262/#sec-%typedarray%.prototype-@@iterator
	exportTypedArrayMethod$a(ITERATOR$6, typedArrayValues, !CORRECT_ITER_NAME);

	var aTypedArray$b = arrayBufferViewCore.aTypedArray;
	var exportTypedArrayMethod$b = arrayBufferViewCore.exportTypedArrayMethod;
	var $join = [].join;

	// `%TypedArray%.prototype.join` method
	// https://tc39.github.io/ecma262/#sec-%typedarray%.prototype.join
	// eslint-disable-next-line no-unused-vars
	exportTypedArrayMethod$b('join', function join(separator) {
	  return $join.apply(aTypedArray$b(this), arguments);
	});

	var min$3 = Math.min;
	var nativeLastIndexOf = [].lastIndexOf;
	var NEGATIVE_ZERO = !!nativeLastIndexOf && 1 / [1].lastIndexOf(1, -0) < 0;
	var STRICT_METHOD$1 = arrayMethodIsStrict('lastIndexOf');
	// For preventing possible almost infinite loop in non-standard implementations, test the forward version of the method
	var USES_TO_LENGTH$2 = arrayMethodUsesToLength('indexOf', { ACCESSORS: true, 1: 0 });
	var FORCED$1 = NEGATIVE_ZERO || !STRICT_METHOD$1 || !USES_TO_LENGTH$2;

	// `Array.prototype.lastIndexOf` method implementation
	// https://tc39.github.io/ecma262/#sec-array.prototype.lastindexof
	var arrayLastIndexOf = FORCED$1 ? function lastIndexOf(searchElement /* , fromIndex = @[*-1] */) {
	  // convert -0 to +0
	  if (NEGATIVE_ZERO) return nativeLastIndexOf.apply(this, arguments) || 0;
	  var O = toIndexedObject(this);
	  var length = toLength(O.length);
	  var index = length - 1;
	  if (arguments.length > 1) index = min$3(index, toInteger(arguments[1]));
	  if (index < 0) index = length + index;
	  for (;index >= 0; index--) if (index in O && O[index] === searchElement) return index || 0;
	  return -1;
	} : nativeLastIndexOf;

	var aTypedArray$c = arrayBufferViewCore.aTypedArray;
	var exportTypedArrayMethod$c = arrayBufferViewCore.exportTypedArrayMethod;

	// `%TypedArray%.prototype.lastIndexOf` method
	// https://tc39.github.io/ecma262/#sec-%typedarray%.prototype.lastindexof
	// eslint-disable-next-line no-unused-vars
	exportTypedArrayMethod$c('lastIndexOf', function lastIndexOf(searchElement /* , fromIndex */) {
	  return arrayLastIndexOf.apply(aTypedArray$c(this), arguments);
	});

	var $map = arrayIteration.map;


	var aTypedArray$d = arrayBufferViewCore.aTypedArray;
	var aTypedArrayConstructor$3 = arrayBufferViewCore.aTypedArrayConstructor;
	var exportTypedArrayMethod$d = arrayBufferViewCore.exportTypedArrayMethod;

	// `%TypedArray%.prototype.map` method
	// https://tc39.github.io/ecma262/#sec-%typedarray%.prototype.map
	exportTypedArrayMethod$d('map', function map(mapfn /* , thisArg */) {
	  return $map(aTypedArray$d(this), mapfn, arguments.length > 1 ? arguments[1] : undefined, function (O, length) {
	    return new (aTypedArrayConstructor$3(speciesConstructor(O, O.constructor)))(length);
	  });
	});

	// `Array.prototype.{ reduce, reduceRight }` methods implementation
	var createMethod$3 = function (IS_RIGHT) {
	  return function (that, callbackfn, argumentsLength, memo) {
	    aFunction$1(callbackfn);
	    var O = toObject(that);
	    var self = indexedObject(O);
	    var length = toLength(O.length);
	    var index = IS_RIGHT ? length - 1 : 0;
	    var i = IS_RIGHT ? -1 : 1;
	    if (argumentsLength < 2) while (true) {
	      if (index in self) {
	        memo = self[index];
	        index += i;
	        break;
	      }
	      index += i;
	      if (IS_RIGHT ? index < 0 : length <= index) {
	        throw TypeError('Reduce of empty array with no initial value');
	      }
	    }
	    for (;IS_RIGHT ? index >= 0 : length > index; index += i) if (index in self) {
	      memo = callbackfn(memo, self[index], index, O);
	    }
	    return memo;
	  };
	};

	var arrayReduce = {
	  // `Array.prototype.reduce` method
	  // https://tc39.github.io/ecma262/#sec-array.prototype.reduce
	  left: createMethod$3(false),
	  // `Array.prototype.reduceRight` method
	  // https://tc39.github.io/ecma262/#sec-array.prototype.reduceright
	  right: createMethod$3(true)
	};

	var $reduce = arrayReduce.left;

	var aTypedArray$e = arrayBufferViewCore.aTypedArray;
	var exportTypedArrayMethod$e = arrayBufferViewCore.exportTypedArrayMethod;

	// `%TypedArray%.prototype.reduce` method
	// https://tc39.github.io/ecma262/#sec-%typedarray%.prototype.reduce
	exportTypedArrayMethod$e('reduce', function reduce(callbackfn /* , initialValue */) {
	  return $reduce(aTypedArray$e(this), callbackfn, arguments.length, arguments.length > 1 ? arguments[1] : undefined);
	});

	var $reduceRight = arrayReduce.right;

	var aTypedArray$f = arrayBufferViewCore.aTypedArray;
	var exportTypedArrayMethod$f = arrayBufferViewCore.exportTypedArrayMethod;

	// `%TypedArray%.prototype.reduceRicht` method
	// https://tc39.github.io/ecma262/#sec-%typedarray%.prototype.reduceright
	exportTypedArrayMethod$f('reduceRight', function reduceRight(callbackfn /* , initialValue */) {
	  return $reduceRight(aTypedArray$f(this), callbackfn, arguments.length, arguments.length > 1 ? arguments[1] : undefined);
	});

	var aTypedArray$g = arrayBufferViewCore.aTypedArray;
	var exportTypedArrayMethod$g = arrayBufferViewCore.exportTypedArrayMethod;
	var floor$2 = Math.floor;

	// `%TypedArray%.prototype.reverse` method
	// https://tc39.github.io/ecma262/#sec-%typedarray%.prototype.reverse
	exportTypedArrayMethod$g('reverse', function reverse() {
	  var that = this;
	  var length = aTypedArray$g(that).length;
	  var middle = floor$2(length / 2);
	  var index = 0;
	  var value;
	  while (index < middle) {
	    value = that[index];
	    that[index++] = that[--length];
	    that[length] = value;
	  } return that;
	});

	var aTypedArray$h = arrayBufferViewCore.aTypedArray;
	var exportTypedArrayMethod$h = arrayBufferViewCore.exportTypedArrayMethod;

	var FORCED$2 = fails(function () {
	  // eslint-disable-next-line no-undef
	  new Int8Array(1).set({});
	});

	// `%TypedArray%.prototype.set` method
	// https://tc39.github.io/ecma262/#sec-%typedarray%.prototype.set
	exportTypedArrayMethod$h('set', function set(arrayLike /* , offset */) {
	  aTypedArray$h(this);
	  var offset = toOffset(arguments.length > 1 ? arguments[1] : undefined, 1);
	  var length = this.length;
	  var src = toObject(arrayLike);
	  var len = toLength(src.length);
	  var index = 0;
	  if (len + offset > length) throw RangeError('Wrong length');
	  while (index < len) this[offset + index] = src[index++];
	}, FORCED$2);

	var aTypedArray$i = arrayBufferViewCore.aTypedArray;
	var aTypedArrayConstructor$4 = arrayBufferViewCore.aTypedArrayConstructor;
	var exportTypedArrayMethod$i = arrayBufferViewCore.exportTypedArrayMethod;
	var $slice = [].slice;

	var FORCED$3 = fails(function () {
	  // eslint-disable-next-line no-undef
	  new Int8Array(1).slice();
	});

	// `%TypedArray%.prototype.slice` method
	// https://tc39.github.io/ecma262/#sec-%typedarray%.prototype.slice
	exportTypedArrayMethod$i('slice', function slice(start, end) {
	  var list = $slice.call(aTypedArray$i(this), start, end);
	  var C = speciesConstructor(this, this.constructor);
	  var index = 0;
	  var length = list.length;
	  var result = new (aTypedArrayConstructor$4(C))(length);
	  while (length > index) result[index] = list[index++];
	  return result;
	}, FORCED$3);

	var $some = arrayIteration.some;

	var aTypedArray$j = arrayBufferViewCore.aTypedArray;
	var exportTypedArrayMethod$j = arrayBufferViewCore.exportTypedArrayMethod;

	// `%TypedArray%.prototype.some` method
	// https://tc39.github.io/ecma262/#sec-%typedarray%.prototype.some
	exportTypedArrayMethod$j('some', function some(callbackfn /* , thisArg */) {
	  return $some(aTypedArray$j(this), callbackfn, arguments.length > 1 ? arguments[1] : undefined);
	});

	var aTypedArray$k = arrayBufferViewCore.aTypedArray;
	var exportTypedArrayMethod$k = arrayBufferViewCore.exportTypedArrayMethod;
	var $sort = [].sort;

	// `%TypedArray%.prototype.sort` method
	// https://tc39.github.io/ecma262/#sec-%typedarray%.prototype.sort
	exportTypedArrayMethod$k('sort', function sort(comparefn) {
	  return $sort.call(aTypedArray$k(this), comparefn);
	});

	var aTypedArray$l = arrayBufferViewCore.aTypedArray;
	var exportTypedArrayMethod$l = arrayBufferViewCore.exportTypedArrayMethod;

	// `%TypedArray%.prototype.subarray` method
	// https://tc39.github.io/ecma262/#sec-%typedarray%.prototype.subarray
	exportTypedArrayMethod$l('subarray', function subarray(begin, end) {
	  var O = aTypedArray$l(this);
	  var length = O.length;
	  var beginIndex = toAbsoluteIndex(begin, length);
	  return new (speciesConstructor(O, O.constructor))(
	    O.buffer,
	    O.byteOffset + beginIndex * O.BYTES_PER_ELEMENT,
	    toLength((end === undefined ? length : toAbsoluteIndex(end, length)) - beginIndex)
	  );
	});

	var Int8Array$3 = global_1.Int8Array;
	var aTypedArray$m = arrayBufferViewCore.aTypedArray;
	var exportTypedArrayMethod$m = arrayBufferViewCore.exportTypedArrayMethod;
	var $toLocaleString = [].toLocaleString;
	var $slice$1 = [].slice;

	// iOS Safari 6.x fails here
	var TO_LOCALE_STRING_BUG = !!Int8Array$3 && fails(function () {
	  $toLocaleString.call(new Int8Array$3(1));
	});

	var FORCED$4 = fails(function () {
	  return [1, 2].toLocaleString() != new Int8Array$3([1, 2]).toLocaleString();
	}) || !fails(function () {
	  Int8Array$3.prototype.toLocaleString.call([1, 2]);
	});

	// `%TypedArray%.prototype.toLocaleString` method
	// https://tc39.github.io/ecma262/#sec-%typedarray%.prototype.tolocalestring
	exportTypedArrayMethod$m('toLocaleString', function toLocaleString() {
	  return $toLocaleString.apply(TO_LOCALE_STRING_BUG ? $slice$1.call(aTypedArray$m(this)) : aTypedArray$m(this), arguments);
	}, FORCED$4);

	var exportTypedArrayMethod$n = arrayBufferViewCore.exportTypedArrayMethod;



	var Uint8Array$2 = global_1.Uint8Array;
	var Uint8ArrayPrototype = Uint8Array$2 && Uint8Array$2.prototype || {};
	var arrayToString = [].toString;
	var arrayJoin = [].join;

	if (fails(function () { arrayToString.call({}); })) {
	  arrayToString = function toString() {
	    return arrayJoin.call(this);
	  };
	}

	var IS_NOT_ARRAY_METHOD = Uint8ArrayPrototype.toString != arrayToString;

	// `%TypedArray%.prototype.toString` method
	// https://tc39.github.io/ecma262/#sec-%typedarray%.prototype.tostring
	exportTypedArrayMethod$n('toString', arrayToString, IS_NOT_ARRAY_METHOD);

	var rngBrowser = createCommonjsModule(function (module) {
	  // Unique ID creation requires a high quality random # generator.  In the
	  // browser this is a little complicated due to unknown quality of Math.random()
	  // and inconsistent support for the `crypto` API.  We do the best we can via
	  // feature-detection
	  // getRandomValues needs to be invoked in a context where "this" is a Crypto
	  // implementation. Also, find the complete implementation of crypto on IE11.
	  var getRandomValues = typeof crypto != 'undefined' && crypto.getRandomValues && crypto.getRandomValues.bind(crypto) || typeof msCrypto != 'undefined' && typeof window.msCrypto.getRandomValues == 'function' && msCrypto.getRandomValues.bind(msCrypto);

	  if (getRandomValues) {
	    // WHATWG crypto RNG - http://wiki.whatwg.org/wiki/Crypto
	    var rnds8 = new Uint8Array(16); // eslint-disable-line no-undef

	    module.exports = function whatwgRNG() {
	      getRandomValues(rnds8);
	      return rnds8;
	    };
	  } else {
	    // Math.random()-based (RNG)
	    //
	    // If all else fails, use Math.random().  It's fast, but is of unspecified
	    // quality.
	    var rnds = new Array(16);

	    module.exports = function mathRNG() {
	      for (var i = 0, r; i < 16; i++) {
	        if ((i & 0x03) === 0) r = Math.random() * 0x100000000;
	        rnds[i] = r >>> ((i & 0x03) << 3) & 0xff;
	      }

	      return rnds;
	    };
	  }
	});

	var nativeJoin = [].join;

	var ES3_STRINGS = indexedObject != Object;
	var STRICT_METHOD$2 = arrayMethodIsStrict('join', ',');

	// `Array.prototype.join` method
	// https://tc39.github.io/ecma262/#sec-array.prototype.join
	_export({ target: 'Array', proto: true, forced: ES3_STRINGS || !STRICT_METHOD$2 }, {
	  join: function join(separator) {
	    return nativeJoin.call(toIndexedObject(this), separator === undefined ? ',' : separator);
	  }
	});

	/**
	 * Convert array of 16 byte values to UUID string format of the form:
	 * XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX
	 */
	var byteToHex = [];

	for (var i = 0; i < 256; ++i) {
	  byteToHex[i] = (i + 0x100).toString(16).substr(1);
	}

	function bytesToUuid(buf, offset) {
	  var i = offset || 0;
	  var bth = byteToHex; // join used to fix memory issue caused by concatenation: https://bugs.chromium.org/p/v8/issues/detail?id=3175#c4

	  return [bth[buf[i++]], bth[buf[i++]], bth[buf[i++]], bth[buf[i++]], '-', bth[buf[i++]], bth[buf[i++]], '-', bth[buf[i++]], bth[buf[i++]], '-', bth[buf[i++]], bth[buf[i++]], '-', bth[buf[i++]], bth[buf[i++]], bth[buf[i++]], bth[buf[i++]], bth[buf[i++]], bth[buf[i++]]].join('');
	}

	var bytesToUuid_1 = bytesToUuid;

	function v4(options, buf, offset) {
	  var i = buf && offset || 0;

	  if (typeof options == 'string') {
	    buf = options === 'binary' ? new Array(16) : null;
	    options = null;
	  }

	  options = options || {};
	  var rnds = options.random || (options.rng || rngBrowser)(); // Per 4.4, set bits for version and `clock_seq_hi_and_reserved`

	  rnds[6] = rnds[6] & 0x0f | 0x40;
	  rnds[8] = rnds[8] & 0x3f | 0x80; // Copy bytes to buffer, if provided

	  if (buf) {
	    for (var ii = 0; ii < 16; ++ii) {
	      buf[i + ii] = rnds[ii];
	    }
	  }

	  return buf || bytesToUuid_1(rnds);
	}

	var v4_1 = v4;

	var IS_CONCAT_SPREADABLE = wellKnownSymbol('isConcatSpreadable');
	var MAX_SAFE_INTEGER = 0x1FFFFFFFFFFFFF;
	var MAXIMUM_ALLOWED_INDEX_EXCEEDED = 'Maximum allowed index exceeded';

	// We can't use this feature detection in V8 since it causes
	// deoptimization and serious performance degradation
	// https://github.com/zloirock/core-js/issues/679
	var IS_CONCAT_SPREADABLE_SUPPORT = engineV8Version >= 51 || !fails(function () {
	  var array = [];
	  array[IS_CONCAT_SPREADABLE] = false;
	  return array.concat()[0] !== array;
	});

	var SPECIES_SUPPORT = arrayMethodHasSpeciesSupport('concat');

	var isConcatSpreadable = function (O) {
	  if (!isObject(O)) return false;
	  var spreadable = O[IS_CONCAT_SPREADABLE];
	  return spreadable !== undefined ? !!spreadable : isArray(O);
	};

	var FORCED$5 = !IS_CONCAT_SPREADABLE_SUPPORT || !SPECIES_SUPPORT;

	// `Array.prototype.concat` method
	// https://tc39.github.io/ecma262/#sec-array.prototype.concat
	// with adding support of @@isConcatSpreadable and @@species
	_export({ target: 'Array', proto: true, forced: FORCED$5 }, {
	  concat: function concat(arg) { // eslint-disable-line no-unused-vars
	    var O = toObject(this);
	    var A = arraySpeciesCreate(O, 0);
	    var n = 0;
	    var i, k, length, len, E;
	    for (i = -1, length = arguments.length; i < length; i++) {
	      E = i === -1 ? O : arguments[i];
	      if (isConcatSpreadable(E)) {
	        len = toLength(E.length);
	        if (n + len > MAX_SAFE_INTEGER) throw TypeError(MAXIMUM_ALLOWED_INDEX_EXCEEDED);
	        for (k = 0; k < len; k++, n++) if (k in E) createProperty(A, n, E[k]);
	      } else {
	        if (n >= MAX_SAFE_INTEGER) throw TypeError(MAXIMUM_ALLOWED_INDEX_EXCEEDED);
	        createProperty(A, n++, E);
	      }
	    }
	    A.length = n;
	    return A;
	  }
	});

	var $filter$1 = arrayIteration.filter;



	var HAS_SPECIES_SUPPORT$1 = arrayMethodHasSpeciesSupport('filter');
	// Edge 14- issue
	var USES_TO_LENGTH$3 = arrayMethodUsesToLength('filter');

	// `Array.prototype.filter` method
	// https://tc39.github.io/ecma262/#sec-array.prototype.filter
	// with adding support of @@species
	_export({ target: 'Array', proto: true, forced: !HAS_SPECIES_SUPPORT$1 || !USES_TO_LENGTH$3 }, {
	  filter: function filter(callbackfn /* , thisArg */) {
	    return $filter$1(this, callbackfn, arguments.length > 1 ? arguments[1] : undefined);
	  }
	});

	var $find$1 = arrayIteration.find;



	var FIND = 'find';
	var SKIPS_HOLES = true;

	var USES_TO_LENGTH$4 = arrayMethodUsesToLength(FIND);

	// Shouldn't skip holes
	if (FIND in []) Array(1)[FIND](function () { SKIPS_HOLES = false; });

	// `Array.prototype.find` method
	// https://tc39.github.io/ecma262/#sec-array.prototype.find
	_export({ target: 'Array', proto: true, forced: SKIPS_HOLES || !USES_TO_LENGTH$4 }, {
	  find: function find(callbackfn /* , that = undefined */) {
	    return $find$1(this, callbackfn, arguments.length > 1 ? arguments[1] : undefined);
	  }
	});

	// https://tc39.github.io/ecma262/#sec-array.prototype-@@unscopables
	addToUnscopables(FIND);

	// `Array.from` method implementation
	// https://tc39.github.io/ecma262/#sec-array.from
	var arrayFrom = function from(arrayLike /* , mapfn = undefined, thisArg = undefined */) {
	  var O = toObject(arrayLike);
	  var C = typeof this == 'function' ? this : Array;
	  var argumentsLength = arguments.length;
	  var mapfn = argumentsLength > 1 ? arguments[1] : undefined;
	  var mapping = mapfn !== undefined;
	  var iteratorMethod = getIteratorMethod(O);
	  var index = 0;
	  var length, result, step, iterator, next, value;
	  if (mapping) mapfn = functionBindContext(mapfn, argumentsLength > 2 ? arguments[2] : undefined, 2);
	  // if the target is not iterable or it's an array with the default iterator - use a simple case
	  if (iteratorMethod != undefined && !(C == Array && isArrayIteratorMethod(iteratorMethod))) {
	    iterator = iteratorMethod.call(O);
	    next = iterator.next;
	    result = new C();
	    for (;!(step = next.call(iterator)).done; index++) {
	      value = mapping ? callWithSafeIterationClosing(iterator, mapfn, [step.value, index], true) : step.value;
	      createProperty(result, index, value);
	    }
	  } else {
	    length = toLength(O.length);
	    result = new C(length);
	    for (;length > index; index++) {
	      value = mapping ? mapfn(O[index], index) : O[index];
	      createProperty(result, index, value);
	    }
	  }
	  result.length = index;
	  return result;
	};

	var INCORRECT_ITERATION$1 = !checkCorrectnessOfIteration(function (iterable) {
	});

	// `Array.from` method
	// https://tc39.github.io/ecma262/#sec-array.from
	_export({ target: 'Array', stat: true, forced: INCORRECT_ITERATION$1 }, {
	  from: arrayFrom
	});

	var $includes$1 = arrayIncludes.includes;



	var USES_TO_LENGTH$5 = arrayMethodUsesToLength('indexOf', { ACCESSORS: true, 1: 0 });

	// `Array.prototype.includes` method
	// https://tc39.github.io/ecma262/#sec-array.prototype.includes
	_export({ target: 'Array', proto: true, forced: !USES_TO_LENGTH$5 }, {
	  includes: function includes(el /* , fromIndex = 0 */) {
	    return $includes$1(this, el, arguments.length > 1 ? arguments[1] : undefined);
	  }
	});

	// https://tc39.github.io/ecma262/#sec-array.prototype-@@unscopables
	addToUnscopables('includes');

	var $map$1 = arrayIteration.map;



	var HAS_SPECIES_SUPPORT$2 = arrayMethodHasSpeciesSupport('map');
	// FF49- issue
	var USES_TO_LENGTH$6 = arrayMethodUsesToLength('map');

	// `Array.prototype.map` method
	// https://tc39.github.io/ecma262/#sec-array.prototype.map
	// with adding support of @@species
	_export({ target: 'Array', proto: true, forced: !HAS_SPECIES_SUPPORT$2 || !USES_TO_LENGTH$6 }, {
	  map: function map(callbackfn /* , thisArg */) {
	    return $map$1(this, callbackfn, arguments.length > 1 ? arguments[1] : undefined);
	  }
	});

	var $reduce$1 = arrayReduce.left;



	var STRICT_METHOD$3 = arrayMethodIsStrict('reduce');
	var USES_TO_LENGTH$7 = arrayMethodUsesToLength('reduce', { 1: 0 });

	// `Array.prototype.reduce` method
	// https://tc39.github.io/ecma262/#sec-array.prototype.reduce
	_export({ target: 'Array', proto: true, forced: !STRICT_METHOD$3 || !USES_TO_LENGTH$7 }, {
	  reduce: function reduce(callbackfn /* , initialValue */) {
	    return $reduce$1(this, callbackfn, arguments.length, arguments.length > 1 ? arguments[1] : undefined);
	  }
	});

	// a string of all valid unicode whitespaces
	// eslint-disable-next-line max-len
	var whitespaces = '\u0009\u000A\u000B\u000C\u000D\u0020\u00A0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200A\u202F\u205F\u3000\u2028\u2029\uFEFF';

	var whitespace = '[' + whitespaces + ']';
	var ltrim = RegExp('^' + whitespace + whitespace + '*');
	var rtrim = RegExp(whitespace + whitespace + '*$');

	// `String.prototype.{ trim, trimStart, trimEnd, trimLeft, trimRight }` methods implementation
	var createMethod$4 = function (TYPE) {
	  return function ($this) {
	    var string = String(requireObjectCoercible($this));
	    if (TYPE & 1) string = string.replace(ltrim, '');
	    if (TYPE & 2) string = string.replace(rtrim, '');
	    return string;
	  };
	};

	var stringTrim = {
	  // `String.prototype.{ trimLeft, trimStart }` methods
	  // https://tc39.github.io/ecma262/#sec-string.prototype.trimstart
	  start: createMethod$4(1),
	  // `String.prototype.{ trimRight, trimEnd }` methods
	  // https://tc39.github.io/ecma262/#sec-string.prototype.trimend
	  end: createMethod$4(2),
	  // `String.prototype.trim` method
	  // https://tc39.github.io/ecma262/#sec-string.prototype.trim
	  trim: createMethod$4(3)
	};

	var getOwnPropertyNames$1 = objectGetOwnPropertyNames.f;
	var getOwnPropertyDescriptor$3 = objectGetOwnPropertyDescriptor.f;
	var defineProperty$7 = objectDefineProperty.f;
	var trim = stringTrim.trim;

	var NUMBER = 'Number';
	var NativeNumber = global_1[NUMBER];
	var NumberPrototype = NativeNumber.prototype;

	// Opera ~12 has broken Object#toString
	var BROKEN_CLASSOF = classofRaw(objectCreate(NumberPrototype)) == NUMBER;

	// `ToNumber` abstract operation
	// https://tc39.github.io/ecma262/#sec-tonumber
	var toNumber = function (argument) {
	  var it = toPrimitive(argument, false);
	  var first, third, radix, maxCode, digits, length, index, code;
	  if (typeof it == 'string' && it.length > 2) {
	    it = trim(it);
	    first = it.charCodeAt(0);
	    if (first === 43 || first === 45) {
	      third = it.charCodeAt(2);
	      if (third === 88 || third === 120) return NaN; // Number('+0x1') should be NaN, old V8 fix
	    } else if (first === 48) {
	      switch (it.charCodeAt(1)) {
	        case 66: case 98: radix = 2; maxCode = 49; break; // fast equal of /^0b[01]+$/i
	        case 79: case 111: radix = 8; maxCode = 55; break; // fast equal of /^0o[0-7]+$/i
	        default: return +it;
	      }
	      digits = it.slice(2);
	      length = digits.length;
	      for (index = 0; index < length; index++) {
	        code = digits.charCodeAt(index);
	        // parseInt parses a string to a first unavailable symbol
	        // but ToNumber should return NaN if a string contains unavailable symbols
	        if (code < 48 || code > maxCode) return NaN;
	      } return parseInt(digits, radix);
	    }
	  } return +it;
	};

	// `Number` constructor
	// https://tc39.github.io/ecma262/#sec-number-constructor
	if (isForced_1(NUMBER, !NativeNumber(' 0o1') || !NativeNumber('0b1') || NativeNumber('+0x1'))) {
	  var NumberWrapper = function Number(value) {
	    var it = arguments.length < 1 ? 0 : value;
	    var dummy = this;
	    return dummy instanceof NumberWrapper
	      // check on 1..constructor(foo) case
	      && (BROKEN_CLASSOF ? fails(function () { NumberPrototype.valueOf.call(dummy); }) : classofRaw(dummy) != NUMBER)
	        ? inheritIfRequired(new NativeNumber(toNumber(it)), dummy, NumberWrapper) : toNumber(it);
	  };
	  for (var keys$2 = descriptors ? getOwnPropertyNames$1(NativeNumber) : (
	    // ES3:
	    'MAX_VALUE,MIN_VALUE,NaN,NEGATIVE_INFINITY,POSITIVE_INFINITY,' +
	    // ES2015 (in case, if modules with ES2015 Number statics required before):
	    'EPSILON,isFinite,isInteger,isNaN,isSafeInteger,MAX_SAFE_INTEGER,' +
	    'MIN_SAFE_INTEGER,parseFloat,parseInt,isInteger'
	  ).split(','), j$1 = 0, key$1; keys$2.length > j$1; j$1++) {
	    if (has(NativeNumber, key$1 = keys$2[j$1]) && !has(NumberWrapper, key$1)) {
	      defineProperty$7(NumberWrapper, key$1, getOwnPropertyDescriptor$3(NativeNumber, key$1));
	    }
	  }
	  NumberWrapper.prototype = NumberPrototype;
	  NumberPrototype.constructor = NumberWrapper;
	  redefine(global_1, NUMBER, NumberWrapper);
	}

	// `Number.isNaN` method
	// https://tc39.github.io/ecma262/#sec-number.isnan
	_export({ target: 'Number', stat: true }, {
	  isNaN: function isNaN(number) {
	    // eslint-disable-next-line no-self-compare
	    return number != number;
	  }
	});

	var trim$1 = stringTrim.trim;


	var $parseFloat = global_1.parseFloat;
	var FORCED$6 = 1 / $parseFloat(whitespaces + '-0') !== -Infinity;

	// `parseFloat` method
	// https://tc39.github.io/ecma262/#sec-parsefloat-string
	var numberParseFloat = FORCED$6 ? function parseFloat(string) {
	  var trimmedString = trim$1(String(string));
	  var result = $parseFloat(trimmedString);
	  return result === 0 && trimmedString.charAt(0) == '-' ? -0 : result;
	} : $parseFloat;

	// `Number.parseFloat` method
	// https://tc39.github.io/ecma262/#sec-number.parseFloat
	_export({ target: 'Number', stat: true, forced: Number.parseFloat != numberParseFloat }, {
	  parseFloat: numberParseFloat
	});

	var nativeAssign = Object.assign;
	var defineProperty$8 = Object.defineProperty;

	// `Object.assign` method
	// https://tc39.github.io/ecma262/#sec-object.assign
	var objectAssign = !nativeAssign || fails(function () {
	  // should have correct order of operations (Edge bug)
	  if (descriptors && nativeAssign({ b: 1 }, nativeAssign(defineProperty$8({}, 'a', {
	    enumerable: true,
	    get: function () {
	      defineProperty$8(this, 'b', {
	        value: 3,
	        enumerable: false
	      });
	    }
	  }), { b: 2 })).b !== 1) return true;
	  // should work with symbols and should have deterministic property order (V8 bug)
	  var A = {};
	  var B = {};
	  // eslint-disable-next-line no-undef
	  var symbol = Symbol();
	  var alphabet = 'abcdefghijklmnopqrst';
	  A[symbol] = 7;
	  alphabet.split('').forEach(function (chr) { B[chr] = chr; });
	  return nativeAssign({}, A)[symbol] != 7 || objectKeys(nativeAssign({}, B)).join('') != alphabet;
	}) ? function assign(target, source) { // eslint-disable-line no-unused-vars
	  var T = toObject(target);
	  var argumentsLength = arguments.length;
	  var index = 1;
	  var getOwnPropertySymbols = objectGetOwnPropertySymbols.f;
	  var propertyIsEnumerable = objectPropertyIsEnumerable.f;
	  while (argumentsLength > index) {
	    var S = indexedObject(arguments[index++]);
	    var keys = getOwnPropertySymbols ? objectKeys(S).concat(getOwnPropertySymbols(S)) : objectKeys(S);
	    var length = keys.length;
	    var j = 0;
	    var key;
	    while (length > j) {
	      key = keys[j++];
	      if (!descriptors || propertyIsEnumerable.call(S, key)) T[key] = S[key];
	    }
	  } return T;
	} : nativeAssign;

	// `Object.assign` method
	// https://tc39.github.io/ecma262/#sec-object.assign
	_export({ target: 'Object', stat: true, forced: Object.assign !== objectAssign }, {
	  assign: objectAssign
	});

	var FAILS_ON_PRIMITIVES$1 = fails(function () { objectKeys(1); });

	// `Object.keys` method
	// https://tc39.github.io/ecma262/#sec-object.keys
	_export({ target: 'Object', stat: true, forced: FAILS_ON_PRIMITIVES$1 }, {
	  keys: function keys(it) {
	    return objectKeys(toObject(it));
	  }
	});

	var propertyIsEnumerable = objectPropertyIsEnumerable.f;

	// `Object.{ entries, values }` methods implementation
	var createMethod$5 = function (TO_ENTRIES) {
	  return function (it) {
	    var O = toIndexedObject(it);
	    var keys = objectKeys(O);
	    var length = keys.length;
	    var i = 0;
	    var result = [];
	    var key;
	    while (length > i) {
	      key = keys[i++];
	      if (!descriptors || propertyIsEnumerable.call(O, key)) {
	        result.push(TO_ENTRIES ? [key, O[key]] : O[key]);
	      }
	    }
	    return result;
	  };
	};

	var objectToArray = {
	  // `Object.entries` method
	  // https://tc39.github.io/ecma262/#sec-object.entries
	  entries: createMethod$5(true),
	  // `Object.values` method
	  // https://tc39.github.io/ecma262/#sec-object.values
	  values: createMethod$5(false)
	};

	var $values = objectToArray.values;

	// `Object.values` method
	// https://tc39.github.io/ecma262/#sec-object.values
	_export({ target: 'Object', stat: true }, {
	  values: function values(O) {
	    return $values(O);
	  }
	});

	// babel-minify transpiles RegExp('a', 'y') -> /a/y and it causes SyntaxError,
	// so we use an intermediate function.
	function RE(s, f) {
	  return RegExp(s, f);
	}

	var UNSUPPORTED_Y = fails(function () {
	  // babel-minify transpiles RegExp('a', 'y') -> /a/y and it causes SyntaxError
	  var re = RE('a', 'y');
	  re.lastIndex = 2;
	  return re.exec('abcd') != null;
	});

	var BROKEN_CARET = fails(function () {
	  // https://bugzilla.mozilla.org/show_bug.cgi?id=773687
	  var re = RE('^r', 'gy');
	  re.lastIndex = 2;
	  return re.exec('str') != null;
	});

	var regexpStickyHelpers = {
		UNSUPPORTED_Y: UNSUPPORTED_Y,
		BROKEN_CARET: BROKEN_CARET
	};

	var nativeExec = RegExp.prototype.exec;
	// This always refers to the native implementation, because the
	// String#replace polyfill uses ./fix-regexp-well-known-symbol-logic.js,
	// which loads this file before patching the method.
	var nativeReplace = String.prototype.replace;

	var patchedExec = nativeExec;

	var UPDATES_LAST_INDEX_WRONG = (function () {
	  var re1 = /a/;
	  var re2 = /b*/g;
	  nativeExec.call(re1, 'a');
	  nativeExec.call(re2, 'a');
	  return re1.lastIndex !== 0 || re2.lastIndex !== 0;
	})();

	var UNSUPPORTED_Y$1 = regexpStickyHelpers.UNSUPPORTED_Y || regexpStickyHelpers.BROKEN_CARET;

	// nonparticipating capturing group, copied from es5-shim's String#split patch.
	var NPCG_INCLUDED = /()??/.exec('')[1] !== undefined;

	var PATCH = UPDATES_LAST_INDEX_WRONG || NPCG_INCLUDED || UNSUPPORTED_Y$1;

	if (PATCH) {
	  patchedExec = function exec(str) {
	    var re = this;
	    var lastIndex, reCopy, match, i;
	    var sticky = UNSUPPORTED_Y$1 && re.sticky;
	    var flags = regexpFlags.call(re);
	    var source = re.source;
	    var charsAdded = 0;
	    var strCopy = str;

	    if (sticky) {
	      flags = flags.replace('y', '');
	      if (flags.indexOf('g') === -1) {
	        flags += 'g';
	      }

	      strCopy = String(str).slice(re.lastIndex);
	      // Support anchored sticky behavior.
	      if (re.lastIndex > 0 && (!re.multiline || re.multiline && str[re.lastIndex - 1] !== '\n')) {
	        source = '(?: ' + source + ')';
	        strCopy = ' ' + strCopy;
	        charsAdded++;
	      }
	      // ^(? + rx + ) is needed, in combination with some str slicing, to
	      // simulate the 'y' flag.
	      reCopy = new RegExp('^(?:' + source + ')', flags);
	    }

	    if (NPCG_INCLUDED) {
	      reCopy = new RegExp('^' + source + '$(?!\\s)', flags);
	    }
	    if (UPDATES_LAST_INDEX_WRONG) lastIndex = re.lastIndex;

	    match = nativeExec.call(sticky ? reCopy : re, strCopy);

	    if (sticky) {
	      if (match) {
	        match.input = match.input.slice(charsAdded);
	        match[0] = match[0].slice(charsAdded);
	        match.index = re.lastIndex;
	        re.lastIndex += match[0].length;
	      } else re.lastIndex = 0;
	    } else if (UPDATES_LAST_INDEX_WRONG && match) {
	      re.lastIndex = re.global ? match.index + match[0].length : lastIndex;
	    }
	    if (NPCG_INCLUDED && match && match.length > 1) {
	      // Fix browsers whose `exec` methods don't consistently return `undefined`
	      // for NPCG, like IE8. NOTE: This doesn' work for /(.?)?/
	      nativeReplace.call(match[0], reCopy, function () {
	        for (i = 1; i < arguments.length - 2; i++) {
	          if (arguments[i] === undefined) match[i] = undefined;
	        }
	      });
	    }

	    return match;
	  };
	}

	var regexpExec = patchedExec;

	_export({ target: 'RegExp', proto: true, forced: /./.exec !== regexpExec }, {
	  exec: regexpExec
	});

	var MATCH = wellKnownSymbol('match');

	// `IsRegExp` abstract operation
	// https://tc39.github.io/ecma262/#sec-isregexp
	var isRegexp = function (it) {
	  var isRegExp;
	  return isObject(it) && ((isRegExp = it[MATCH]) !== undefined ? !!isRegExp : classofRaw(it) == 'RegExp');
	};

	var notARegexp = function (it) {
	  if (isRegexp(it)) {
	    throw TypeError("The method doesn't accept regular expressions");
	  } return it;
	};

	var MATCH$1 = wellKnownSymbol('match');

	var correctIsRegexpLogic = function (METHOD_NAME) {
	  var regexp = /./;
	  try {
	    '/./'[METHOD_NAME](regexp);
	  } catch (e) {
	    try {
	      regexp[MATCH$1] = false;
	      return '/./'[METHOD_NAME](regexp);
	    } catch (f) { /* empty */ }
	  } return false;
	};

	// `String.prototype.includes` method
	// https://tc39.github.io/ecma262/#sec-string.prototype.includes
	_export({ target: 'String', proto: true, forced: !correctIsRegexpLogic('includes') }, {
	  includes: function includes(searchString /* , position = 0 */) {
	    return !!~String(requireObjectCoercible(this))
	      .indexOf(notARegexp(searchString), arguments.length > 1 ? arguments[1] : undefined);
	  }
	});

	// TODO: Remove from `core-js@4` since it's moved to entry points







	var SPECIES$6 = wellKnownSymbol('species');

	var REPLACE_SUPPORTS_NAMED_GROUPS = !fails(function () {
	  // #replace needs built-in support for named groups.
	  // #match works fine because it just return the exec results, even if it has
	  // a "grops" property.
	  var re = /./;
	  re.exec = function () {
	    var result = [];
	    result.groups = { a: '7' };
	    return result;
	  };
	  return ''.replace(re, '$<a>') !== '7';
	});

	// IE <= 11 replaces $0 with the whole match, as if it was $&
	// https://stackoverflow.com/questions/6024666/getting-ie-to-replace-a-regex-with-the-literal-string-0
	var REPLACE_KEEPS_$0 = (function () {
	  return 'a'.replace(/./, '$0') === '$0';
	})();

	var REPLACE = wellKnownSymbol('replace');
	// Safari <= 13.0.3(?) substitutes nth capture where n>m with an empty string
	var REGEXP_REPLACE_SUBSTITUTES_UNDEFINED_CAPTURE = (function () {
	  if (/./[REPLACE]) {
	    return /./[REPLACE]('a', '$0') === '';
	  }
	  return false;
	})();

	// Chrome 51 has a buggy "split" implementation when RegExp#exec !== nativeExec
	// Weex JS has frozen built-in prototypes, so use try / catch wrapper
	var SPLIT_WORKS_WITH_OVERWRITTEN_EXEC = !fails(function () {
	  var re = /(?:)/;
	  var originalExec = re.exec;
	  re.exec = function () { return originalExec.apply(this, arguments); };
	  var result = 'ab'.split(re);
	  return result.length !== 2 || result[0] !== 'a' || result[1] !== 'b';
	});

	var fixRegexpWellKnownSymbolLogic = function (KEY, length, exec, sham) {
	  var SYMBOL = wellKnownSymbol(KEY);

	  var DELEGATES_TO_SYMBOL = !fails(function () {
	    // String methods call symbol-named RegEp methods
	    var O = {};
	    O[SYMBOL] = function () { return 7; };
	    return ''[KEY](O) != 7;
	  });

	  var DELEGATES_TO_EXEC = DELEGATES_TO_SYMBOL && !fails(function () {
	    // Symbol-named RegExp methods call .exec
	    var execCalled = false;
	    var re = /a/;

	    if (KEY === 'split') {
	      // We can't use real regex here since it causes deoptimization
	      // and serious performance degradation in V8
	      // https://github.com/zloirock/core-js/issues/306
	      re = {};
	      // RegExp[@@split] doesn't call the regex's exec method, but first creates
	      // a new one. We need to return the patched regex when creating the new one.
	      re.constructor = {};
	      re.constructor[SPECIES$6] = function () { return re; };
	      re.flags = '';
	      re[SYMBOL] = /./[SYMBOL];
	    }

	    re.exec = function () { execCalled = true; return null; };

	    re[SYMBOL]('');
	    return !execCalled;
	  });

	  if (
	    !DELEGATES_TO_SYMBOL ||
	    !DELEGATES_TO_EXEC ||
	    (KEY === 'replace' && !(
	      REPLACE_SUPPORTS_NAMED_GROUPS &&
	      REPLACE_KEEPS_$0 &&
	      !REGEXP_REPLACE_SUBSTITUTES_UNDEFINED_CAPTURE
	    )) ||
	    (KEY === 'split' && !SPLIT_WORKS_WITH_OVERWRITTEN_EXEC)
	  ) {
	    var nativeRegExpMethod = /./[SYMBOL];
	    var methods = exec(SYMBOL, ''[KEY], function (nativeMethod, regexp, str, arg2, forceStringMethod) {
	      if (regexp.exec === regexpExec) {
	        if (DELEGATES_TO_SYMBOL && !forceStringMethod) {
	          // The native String method already delegates to @@method (this
	          // polyfilled function), leasing to infinite recursion.
	          // We avoid it by directly calling the native @@method method.
	          return { done: true, value: nativeRegExpMethod.call(regexp, str, arg2) };
	        }
	        return { done: true, value: nativeMethod.call(str, regexp, arg2) };
	      }
	      return { done: false };
	    }, {
	      REPLACE_KEEPS_$0: REPLACE_KEEPS_$0,
	      REGEXP_REPLACE_SUBSTITUTES_UNDEFINED_CAPTURE: REGEXP_REPLACE_SUBSTITUTES_UNDEFINED_CAPTURE
	    });
	    var stringMethod = methods[0];
	    var regexMethod = methods[1];

	    redefine(String.prototype, KEY, stringMethod);
	    redefine(RegExp.prototype, SYMBOL, length == 2
	      // 21.2.5.8 RegExp.prototype[@@replace](string, replaceValue)
	      // 21.2.5.11 RegExp.prototype[@@split](string, limit)
	      ? function (string, arg) { return regexMethod.call(string, this, arg); }
	      // 21.2.5.6 RegExp.prototype[@@match](string)
	      // 21.2.5.9 RegExp.prototype[@@search](string)
	      : function (string) { return regexMethod.call(string, this); }
	    );
	  }

	  if (sham) createNonEnumerableProperty(RegExp.prototype[SYMBOL], 'sham', true);
	};

	var charAt$1 = stringMultibyte.charAt;

	// `AdvanceStringIndex` abstract operation
	// https://tc39.github.io/ecma262/#sec-advancestringindex
	var advanceStringIndex = function (S, index, unicode) {
	  return index + (unicode ? charAt$1(S, index).length : 1);
	};

	// `RegExpExec` abstract operation
	// https://tc39.github.io/ecma262/#sec-regexpexec
	var regexpExecAbstract = function (R, S) {
	  var exec = R.exec;
	  if (typeof exec === 'function') {
	    var result = exec.call(R, S);
	    if (typeof result !== 'object') {
	      throw TypeError('RegExp exec method returned something other than an Object or null');
	    }
	    return result;
	  }

	  if (classofRaw(R) !== 'RegExp') {
	    throw TypeError('RegExp#exec called on incompatible receiver');
	  }

	  return regexpExec.call(R, S);
	};

	// @@match logic
	fixRegexpWellKnownSymbolLogic('match', 1, function (MATCH, nativeMatch, maybeCallNative) {
	  return [
	    // `String.prototype.match` method
	    // https://tc39.github.io/ecma262/#sec-string.prototype.match
	    function match(regexp) {
	      var O = requireObjectCoercible(this);
	      var matcher = regexp == undefined ? undefined : regexp[MATCH];
	      return matcher !== undefined ? matcher.call(regexp, O) : new RegExp(regexp)[MATCH](String(O));
	    },
	    // `RegExp.prototype[@@match]` method
	    // https://tc39.github.io/ecma262/#sec-regexp.prototype-@@match
	    function (regexp) {
	      var res = maybeCallNative(nativeMatch, regexp, this);
	      if (res.done) return res.value;

	      var rx = anObject(regexp);
	      var S = String(this);

	      if (!rx.global) return regexpExecAbstract(rx, S);

	      var fullUnicode = rx.unicode;
	      rx.lastIndex = 0;
	      var A = [];
	      var n = 0;
	      var result;
	      while ((result = regexpExecAbstract(rx, S)) !== null) {
	        var matchStr = String(result[0]);
	        A[n] = matchStr;
	        if (matchStr === '') rx.lastIndex = advanceStringIndex(S, toLength(rx.lastIndex), fullUnicode);
	        n++;
	      }
	      return n === 0 ? null : A;
	    }
	  ];
	});

	var arrayPush = [].push;
	var min$4 = Math.min;
	var MAX_UINT32 = 0xFFFFFFFF;

	// babel-minify transpiles RegExp('x', 'y') -> /x/y and it causes SyntaxError
	var SUPPORTS_Y = !fails(function () { return !RegExp(MAX_UINT32, 'y'); });

	// @@split logic
	fixRegexpWellKnownSymbolLogic('split', 2, function (SPLIT, nativeSplit, maybeCallNative) {
	  var internalSplit;
	  if (
	    'abbc'.split(/(b)*/)[1] == 'c' ||
	    'test'.split(/(?:)/, -1).length != 4 ||
	    'ab'.split(/(?:ab)*/).length != 2 ||
	    '.'.split(/(.?)(.?)/).length != 4 ||
	    '.'.split(/()()/).length > 1 ||
	    ''.split(/.?/).length
	  ) {
	    // based on es5-shim implementation, need to rework it
	    internalSplit = function (separator, limit) {
	      var string = String(requireObjectCoercible(this));
	      var lim = limit === undefined ? MAX_UINT32 : limit >>> 0;
	      if (lim === 0) return [];
	      if (separator === undefined) return [string];
	      // If `separator` is not a regex, use native split
	      if (!isRegexp(separator)) {
	        return nativeSplit.call(string, separator, lim);
	      }
	      var output = [];
	      var flags = (separator.ignoreCase ? 'i' : '') +
	                  (separator.multiline ? 'm' : '') +
	                  (separator.unicode ? 'u' : '') +
	                  (separator.sticky ? 'y' : '');
	      var lastLastIndex = 0;
	      // Make `global` and avoid `lastIndex` issues by working with a copy
	      var separatorCopy = new RegExp(separator.source, flags + 'g');
	      var match, lastIndex, lastLength;
	      while (match = regexpExec.call(separatorCopy, string)) {
	        lastIndex = separatorCopy.lastIndex;
	        if (lastIndex > lastLastIndex) {
	          output.push(string.slice(lastLastIndex, match.index));
	          if (match.length > 1 && match.index < string.length) arrayPush.apply(output, match.slice(1));
	          lastLength = match[0].length;
	          lastLastIndex = lastIndex;
	          if (output.length >= lim) break;
	        }
	        if (separatorCopy.lastIndex === match.index) separatorCopy.lastIndex++; // Avoid an infinite loop
	      }
	      if (lastLastIndex === string.length) {
	        if (lastLength || !separatorCopy.test('')) output.push('');
	      } else output.push(string.slice(lastLastIndex));
	      return output.length > lim ? output.slice(0, lim) : output;
	    };
	  // Chakra, V8
	  } else if ('0'.split(undefined, 0).length) {
	    internalSplit = function (separator, limit) {
	      return separator === undefined && limit === 0 ? [] : nativeSplit.call(this, separator, limit);
	    };
	  } else internalSplit = nativeSplit;

	  return [
	    // `String.prototype.split` method
	    // https://tc39.github.io/ecma262/#sec-string.prototype.split
	    function split(separator, limit) {
	      var O = requireObjectCoercible(this);
	      var splitter = separator == undefined ? undefined : separator[SPLIT];
	      return splitter !== undefined
	        ? splitter.call(separator, O, limit)
	        : internalSplit.call(String(O), separator, limit);
	    },
	    // `RegExp.prototype[@@split]` method
	    // https://tc39.github.io/ecma262/#sec-regexp.prototype-@@split
	    //
	    // NOTE: This cannot be properly polyfilled in engines that don't support
	    // the 'y' flag.
	    function (regexp, limit) {
	      var res = maybeCallNative(internalSplit, regexp, this, limit, internalSplit !== nativeSplit);
	      if (res.done) return res.value;

	      var rx = anObject(regexp);
	      var S = String(this);
	      var C = speciesConstructor(rx, RegExp);

	      var unicodeMatching = rx.unicode;
	      var flags = (rx.ignoreCase ? 'i' : '') +
	                  (rx.multiline ? 'm' : '') +
	                  (rx.unicode ? 'u' : '') +
	                  (SUPPORTS_Y ? 'y' : 'g');

	      // ^(? + rx + ) is needed, in combination with some S slicing, to
	      // simulate the 'y' flag.
	      var splitter = new C(SUPPORTS_Y ? rx : '^(?:' + rx.source + ')', flags);
	      var lim = limit === undefined ? MAX_UINT32 : limit >>> 0;
	      if (lim === 0) return [];
	      if (S.length === 0) return regexpExecAbstract(splitter, S) === null ? [S] : [];
	      var p = 0;
	      var q = 0;
	      var A = [];
	      while (q < S.length) {
	        splitter.lastIndex = SUPPORTS_Y ? q : 0;
	        var z = regexpExecAbstract(splitter, SUPPORTS_Y ? S : S.slice(q));
	        var e;
	        if (
	          z === null ||
	          (e = min$4(toLength(splitter.lastIndex + (SUPPORTS_Y ? 0 : q)), S.length)) === p
	        ) {
	          q = advanceStringIndex(S, q, unicodeMatching);
	        } else {
	          A.push(S.slice(p, q));
	          if (A.length === lim) return A;
	          for (var i = 1; i <= z.length - 1; i++) {
	            A.push(z[i]);
	            if (A.length === lim) return A;
	          }
	          q = p = e;
	        }
	      }
	      A.push(S.slice(p));
	      return A;
	    }
	  ];
	}, !SUPPORTS_Y);

	var non = '\u200B\u0085\u180E';

	// check that a method works with the correct list
	// of whitespaces and has a correct name
	var stringTrimForced = function (METHOD_NAME) {
	  return fails(function () {
	    return !!whitespaces[METHOD_NAME]() || non[METHOD_NAME]() != non || whitespaces[METHOD_NAME].name !== METHOD_NAME;
	  });
	};

	var $trim = stringTrim.trim;


	// `String.prototype.trim` method
	// https://tc39.github.io/ecma262/#sec-string.prototype.trim
	_export({ target: 'String', proto: true, forced: stringTrimForced('trim') }, {
	  trim: function trim() {
	    return $trim(this);
	  }
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
	 * @see R.T
	 * @example
	 *
	 *      R.F(); //=> false
	 */

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
	 * @see R.F
	 * @example
	 *
	 *      R.T(); //=> true
	 */

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
	 * @name __
	 * @constant
	 * @memberOf R
	 * @since v0.6.0
	 * @category Function
	 * @example
	 *
	 *      const greet = R.replace('{name}', R.__, 'Hello, {name}!');
	 *      greet('Alice'); //=> 'Hello, Alice!'
	 */

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
	 *      const sumArgs = (...args) => R.sum(args);
	 *
	 *      const curriedAddFourNumbers = R.curryN(4, sumArgs);
	 *      const f = curriedAddFourNumbers(1, 2);
	 *      const g = f(3);
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

	// `Array.isArray` method
	// https://tc39.github.io/ecma262/#sec-array.isarray
	_export({ target: 'Array', stat: true }, {
	  isArray: isArray
	});

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
	var _isArray = Array.isArray || function _isArray(val) {
	  return val != null && val.length >= 0 && Object.prototype.toString.call(val) === '[object Array]';
	};

	function _isTransformer(obj) {
	  return obj != null && typeof obj['@@transducer/step'] === 'function';
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

	    if (!_isArray(obj)) {
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

	var max$2 =
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
	  if (_isArray(x)) {
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
	 *      const log = R.bind(console.log, console);
	 *      R.pipe(R.assoc('a', 2), R.tap(log), R.assoc('a', 3))({a: 1}); //=> {a: 3}
	 *      // logs {a: 2}
	 * @symb R.bind(f, o)(a, b) = f.call(o, a, b)
	 */

	var bind$1 =
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
	  return xf['@@transducer/result'](obj[methodName](bind$1(xf['@@transducer/step'], xf), acc));
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

	function _has(prop, obj) {
	  return Object.prototype.hasOwnProperty.call(obj, prop);
	}

	var toString$2 = Object.prototype.toString;

	var _isArguments =
	/*#__PURE__*/
	function () {
	  return toString$2.call(arguments) === '[object Arguments]' ? function _isArguments(x) {
	    return toString$2.call(x) === '[object Arguments]';
	  } : function _isArguments(x) {
	    return _has('callee', x);
	  };
	}();

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


	var keys$3 = typeof Object.keys === 'function' && !hasArgsEnumBug ?
	/*#__PURE__*/
	_curry1(function keys(obj) {
	  return Object(obj) !== obj ? [] : Object.keys(obj);
	}) :
	/*#__PURE__*/
	_curry1(function keys(obj) {
	  if (Object(obj) !== obj) {
	    return [];
	  }

	  var prop, nIdx;
	  var ks = [];

	  var checkArgsLength = hasArgsEnumBug && _isArguments(obj);

	  for (prop in obj) {
	    if (_has(prop, obj) && (!checkArgsLength || prop !== 'length')) {
	      ks[ks.length] = prop;
	    }
	  }

	  if (hasEnumBug) {
	    nIdx = nonEnumerableProps.length - 1;

	    while (nIdx >= 0) {
	      prop = nonEnumerableProps[nIdx];

	      if (_has(prop, obj) && !contains(ks, prop)) {
	        ks[ks.length] = prop;
	      }

	      nIdx -= 1;
	    }
	  }

	  return ks;
	});

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
	 *      const double = x => x * 2;
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

	var floor$3 = Math.floor;

	// `Number.isInteger` method implementation
	// https://tc39.github.io/ecma262/#sec-number.isinteger
	var isInteger = function isInteger(it) {
	  return !isObject(it) && isFinite(it) && floor$3(it) === it;
	};

	// `Number.isInteger` method
	// https://tc39.github.io/ecma262/#sec-number.isinteger
	_export({ target: 'Number', stat: true }, {
	  isInteger: isInteger
	});

	/**
	 * Determine if the passed argument is an integer.
	 *
	 * @private
	 * @param {*} n
	 * @category Type
	 * @return {Boolean}
	 */
	var _isInteger = Number.isInteger || function _isInteger(n) {
	  return n << 0 === n;
	};

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
	 *      const list = ['foo', 'bar', 'baz', 'quux'];
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
	 * Retrieves the values at given paths of an object.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.27.0
	 * @category Object
	 * @typedefn Idx = [String | Int]
	 * @sig [Idx] -> {a} -> [a | Undefined]
	 * @param {Array} pathsArray The array of paths to be fetched.
	 * @param {Object} obj The object to retrieve the nested properties from.
	 * @return {Array} A list consisting of values at paths specified by "pathsArray".
	 * @see R.path
	 * @example
	 *
	 *      R.paths([['a', 'b'], ['p', 0, 'q']], {a: {b: 2}, p: [{q: 3}]}); //=> [2, 3]
	 *      R.paths([['a', 'b'], ['p', 'r']], {a: {b: 2}, p: [{q: 3}]}); //=> [2, undefined]
	 */

	var paths =
	/*#__PURE__*/
	_curry2(function paths(pathsArray, obj) {
	  return pathsArray.map(function (paths) {
	    var val = obj;
	    var idx = 0;
	    var p;

	    while (idx < paths.length) {
	      if (val == null) {
	        return;
	      }

	      p = paths[idx];
	      val = _isInteger(p) ? nth(p, val) : val[p];
	      idx += 1;
	    }

	    return val;
	  });
	});

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
	 * @see R.prop, R.nth
	 * @example
	 *
	 *      R.path(['a', 'b'], {a: {b: 2}}); //=> 2
	 *      R.path(['a', 'b'], {c: {b: 2}}); //=> undefined
	 *      R.path(['a', 'b', 0], {a: {b: [1, 2, 3]}}); //=> 1
	 *      R.path(['a', 'b', -2], {a: {b: [1, 2, 3]}}); //=> 2
	 */

	var path$1 =
	/*#__PURE__*/
	_curry2(function path(pathAr, obj) {
	  return paths([pathAr], obj)[0];
	});

	/**
	 * Returns a function that when supplied an object returns the indicated
	 * property of that object, if it exists.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category Object
	 * @typedefn Idx = String | Int
	 * @sig Idx -> {s: a} -> a | Undefined
	 * @param {String|Number} p The property name or array index
	 * @param {Object} obj The object to query
	 * @return {*} The value at `obj.p`.
	 * @see R.path, R.nth
	 * @example
	 *
	 *      R.prop('x', {x: 100}); //=> 100
	 *      R.prop('x', {}); //=> undefined
	 *      R.prop(0, [100]); //=> 100
	 *      R.compose(R.inc, R.prop('x'))({ x: 3 }) //=> 4
	 */

	var prop =
	/*#__PURE__*/
	_curry2(function prop(p, obj) {
	  return path$1([p], obj);
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
	 *      var getAges = R.pluck('age');
	 *      getAges([{name: 'fred', age: 29}, {name: 'wilma', age: 27}]); //=> [29, 27]
	 *
	 *      R.pluck(0, [[1, 2], [3, 4]]);               //=> [1, 3]
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
	 * @sig (r -> a -> b) -> (r -> a) -> (r -> b)
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
	  } : _reduce(function (acc, f) {
	    return _concat(acc, map(f, applyX));
	  }, [], applyF);
	});

	function _isFunction(x) {
	  var type = Object.prototype.toString.call(x);
	  return type === '[object Function]' || type === '[object AsyncFunction]' || type === '[object GeneratorFunction]' || type === '[object AsyncGeneratorFunction]';
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
	 *      const madd3 = R.liftN(3, (...args) => R.sum(args));
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
	 *      const madd3 = R.lift((a, b, c) => a + b + c);
	 *
	 *      madd3([1,2,3], [1,2,3], [1]); //=> [3, 4, 5, 4, 5, 6, 5, 6, 7]
	 *
	 *      const madd5 = R.lift((a, b, c, d, e) => a + b + c + d + e);
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
	 * @see R.curryN, R.partial
	 * @example
	 *
	 *      const addFourNumbers = (a, b, c, d) => a + b + c + d;
	 *
	 *      const curriedAddFourNumbers = R.curry(addFourNumbers);
	 *      const f = curriedAddFourNumbers(1, 2);
	 *      const g = f(3);
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
	 *      const indentN = R.pipe(R.repeat(' '),
	 *                           R.join(''),
	 *                           R.replace(/^(?!$)/gm));
	 *
	 *      const format = R.converge(R.call, [
	 *                                  R.pipe(R.prop('indent'), indentN),
	 *                                  R.prop('value')
	 *                              ]);
	 *
	 *      format({indent: 2, value: 'foo\nbar\nbaz\n'}); //=> '  foo\n  bar\n  baz\n'
	 * @symb R.call(f, a, b) = f(a, b)
	 */

	var call =
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
	 * is also known as `flatMap` in some libraries.
	 *
	 * Dispatches to the `chain` method of the second argument, if present,
	 * according to the [FantasyLand Chain spec](https://github.com/fantasyland/fantasy-land#chain).
	 *
	 * If second argument is a function, `chain(f, g)(x)` is equivalent to `f(g(x), x)`.
	 *
	 * Acts as a transducer if a transformer is given in list position.
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
	 *      const duplicate = n => [n, n];
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

	var defineProperty$9 = objectDefineProperty.f;
	var getOwnPropertyNames$2 = objectGetOwnPropertyNames.f;





	var setInternalState$5 = internalState.set;



	var MATCH$2 = wellKnownSymbol('match');
	var NativeRegExp = global_1.RegExp;
	var RegExpPrototype$1 = NativeRegExp.prototype;
	var re1 = /a/g;
	var re2 = /a/g;

	// "new" should create a new object, old webkit bug
	var CORRECT_NEW = new NativeRegExp(re1) !== re1;

	var UNSUPPORTED_Y$2 = regexpStickyHelpers.UNSUPPORTED_Y;

	var FORCED$7 = descriptors && isForced_1('RegExp', (!CORRECT_NEW || UNSUPPORTED_Y$2 || fails(function () {
	  re2[MATCH$2] = false;
	  // RegExp constructor can alter flags and IsRegExp works correct with @@match
	  return NativeRegExp(re1) != re1 || NativeRegExp(re2) == re2 || NativeRegExp(re1, 'i') != '/a/i';
	})));

	// `RegExp` constructor
	// https://tc39.github.io/ecma262/#sec-regexp-constructor
	if (FORCED$7) {
	  var RegExpWrapper = function RegExp(pattern, flags) {
	    var thisIsRegExp = this instanceof RegExpWrapper;
	    var patternIsRegExp = isRegexp(pattern);
	    var flagsAreUndefined = flags === undefined;
	    var sticky;

	    if (!thisIsRegExp && patternIsRegExp && pattern.constructor === RegExpWrapper && flagsAreUndefined) {
	      return pattern;
	    }

	    if (CORRECT_NEW) {
	      if (patternIsRegExp && !flagsAreUndefined) pattern = pattern.source;
	    } else if (pattern instanceof RegExpWrapper) {
	      if (flagsAreUndefined) flags = regexpFlags.call(pattern);
	      pattern = pattern.source;
	    }

	    if (UNSUPPORTED_Y$2) {
	      sticky = !!flags && flags.indexOf('y') > -1;
	      if (sticky) flags = flags.replace(/y/g, '');
	    }

	    var result = inheritIfRequired(
	      CORRECT_NEW ? new NativeRegExp(pattern, flags) : NativeRegExp(pattern, flags),
	      thisIsRegExp ? this : RegExpPrototype$1,
	      RegExpWrapper
	    );

	    if (UNSUPPORTED_Y$2 && sticky) setInternalState$5(result, { sticky: sticky });

	    return result;
	  };
	  var proxy = function (key) {
	    key in RegExpWrapper || defineProperty$9(RegExpWrapper, key, {
	      configurable: true,
	      get: function () { return NativeRegExp[key]; },
	      set: function (it) { NativeRegExp[key] = it; }
	    });
	  };
	  var keys$4 = getOwnPropertyNames$2(NativeRegExp);
	  var index = 0;
	  while (keys$4.length > index) proxy(keys$4[index++]);
	  RegExpPrototype$1.constructor = RegExpWrapper;
	  RegExpWrapper.prototype = RegExpPrototype$1;
	  redefine(global_1, 'RegExp', RegExpWrapper);
	}

	// https://tc39.github.io/ecma262/#sec-get-regexp-@@species
	setSpecies('RegExp');

	function _cloneRegExp(pattern) {
	  return new RegExp(pattern.source, (pattern.global ? 'g' : '') + (pattern.ignoreCase ? 'i' : '') + (pattern.multiline ? 'm' : '') + (pattern.sticky ? 'y' : '') + (pattern.unicode ? 'u' : ''));
	}

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
	 * Copies an object.
	 *
	 * @private
	 * @param {*} value The value to be copied
	 * @param {Array} refFrom Array containing the source references
	 * @param {Array} refTo Array containing the copied source references
	 * @param {Boolean} deep Whether or not to perform deep cloning.
	 * @return {*} The copied value.
	 */

	function _clone(value, refFrom, refTo, deep) {
	  var copy = function copy(copiedValue) {
	    var len = refFrom.length;
	    var idx = 0;

	    while (idx < len) {
	      if (value === refFrom[idx]) {
	        return refTo[idx];
	      }

	      idx += 1;
	    }

	    refFrom[idx + 1] = value;
	    refTo[idx + 1] = copiedValue;

	    for (var key in value) {
	      copiedValue[key] = deep ? _clone(value[key], refFrom, refTo, true) : value[key];
	    }

	    return copiedValue;
	  };

	  switch (type(value)) {
	    case 'Object':
	      return copy({});

	    case 'Array':
	      return copy([]);

	    case 'Date':
	      return new Date(value.valueOf());

	    case 'RegExp':
	      return _cloneRegExp(value);

	    default:
	      return value;
	  }
	}

	/**
	 * Creates a deep copy of the value which may contain (nested) `Array`s and
	 * `Object`s, `Number`s, `String`s, `Boolean`s and `Date`s. `Function`s are
	 * assigned by reference rather than copied
	 *
	 * Dispatches to a `clone` method if present.
	 *
	 * @func
	 * @memberOf R
	 * @since v0.1.0
	 * @category Object
	 * @sig {*} -> {*}
	 * @param {*} value The object or array to clone
	 * @return {*} A deeply cloned copy of `val`
	 * @example
	 *
	 *      const objects = [{}, {}, {}];
	 *      const objectsClone = R.clone(objects);
	 *      objects === objectsClone; //=> false
	 *      objects[0] === objectsClone[0]; //=> false
	 */

	var clone =
	/*#__PURE__*/
	_curry1(function clone(value) {
	  return value != null && typeof value.clone === 'function' ? value.clone() : _clone(value, [], [], true);
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
	 *      const isNotNil = R.complement(R.isNil);
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
	    return _isArray(obj) || typeof obj[methodname] !== 'function' ? fn.apply(this, arguments) : obj[methodname].apply(obj, Array.prototype.slice.call(arguments, 0, length - 1));
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

	var slice$2 =
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
	slice$2(1, Infinity)));

	/**
	 * Performs left-to-right function composition. The first argument may have
	 * any arity; the remaining arguments must be unary.
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
	 *      const f = R.pipe(Math.pow, R.negate, R.inc);
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
	 * Performs right-to-left function composition. The last argument may have
	 * any arity; the remaining arguments must be unary.
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
	 *      const classyGreeting = (firstName, lastName) => "The name's " + lastName + ", " + firstName + " " + lastName
	 *      const yellGreeting = R.compose(R.toUpper, classyGreeting);
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

	var head$1 =
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
	 *      const obj = {};
	 *      R.identity(obj) === obj; //=> true
	 * @symb R.identity(a) = a
	 */

	var identity =
	/*#__PURE__*/
	_curry1(_identity);

	var test$2 = [];
	var nativeSort = test$2.sort;

	// IE8-
	var FAILS_ON_UNDEFINED = fails(function () {
	  test$2.sort(undefined);
	});
	// V8 bug
	var FAILS_ON_NULL = fails(function () {
	  test$2.sort(null);
	});
	// Old WebKit
	var STRICT_METHOD$4 = arrayMethodIsStrict('sort');

	var FORCED$8 = FAILS_ON_UNDEFINED || !FAILS_ON_NULL || !STRICT_METHOD$4;

	// `Array.prototype.sort` method
	// https://tc39.github.io/ecma262/#sec-array.prototype.sort
	_export({ target: 'Array', proto: true, forced: FORCED$8 }, {
	  sort: function sort(comparefn) {
	    return comparefn === undefined
	      ? nativeSort.call(toObject(this))
	      : nativeSort.call(toObject(this), aFunction$1(comparefn));
	  }
	});

	var $indexOf$1 = arrayIncludes.indexOf;



	var nativeIndexOf = [].indexOf;

	var NEGATIVE_ZERO$1 = !!nativeIndexOf && 1 / [1].indexOf(1, -0) < 0;
	var STRICT_METHOD$5 = arrayMethodIsStrict('indexOf');
	var USES_TO_LENGTH$8 = arrayMethodUsesToLength('indexOf', { ACCESSORS: true, 1: 0 });

	// `Array.prototype.indexOf` method
	// https://tc39.github.io/ecma262/#sec-array.prototype.indexof
	_export({ target: 'Array', proto: true, forced: NEGATIVE_ZERO$1 || !STRICT_METHOD$5 || !USES_TO_LENGTH$8 }, {
	  indexOf: function indexOf(searchElement /* , fromIndex = 0 */) {
	    return NEGATIVE_ZERO$1
	      // convert -0 to +0
	      ? nativeIndexOf.apply(this, arguments) || 0
	      : $indexOf$1(this, searchElement, arguments.length > 1 ? arguments[1] : undefined);
	  }
	});

	function _arrayFromIterator(iter) {
	  var list = [];
	  var next;

	  while (!(next = iter.next()).done) {
	    list.push(next.value);
	  }

	  return list;
	}

	function _includesWith(pred, x, list) {
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

	// `SameValue` abstract operation
	// https://tc39.github.io/ecma262/#sec-samevalue
	var sameValue = Object.is || function is(x, y) {
	  // eslint-disable-next-line no-self-compare
	  return x === y ? x !== 0 || 1 / x === 1 / y : x != x && y != y;
	};

	// `Object.is` method
	// https://tc39.github.io/ecma262/#sec-object.is
	_export({ target: 'Object', stat: true }, {
	  is: sameValue
	});

	// Based on https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object/is
	function _objectIs(a, b) {
	  // SameValue algorithm
	  if (a === b) {
	    // Steps 1-5, 7-10
	    // Steps 6.b-6.e: +0 != -0
	    return a !== 0 || 1 / a === 1 / b;
	  } else {
	    // Step 6.a: NaN == NaN
	    return a !== a && b !== b;
	  }
	}

	var _objectIs$1 = typeof Object.is === 'function' ? Object.is : _objectIs;

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


	  return !_includesWith(function (b, aItem) {
	    return !_includesWith(eq, aItem, b);
	  }, b, a);
	}

	function _equals(a, b, stackA, stackB) {
	  if (_objectIs$1(a, b)) {
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
	      if (!(_typeof(a) === _typeof(b) && _objectIs$1(a.valueOf(), b.valueOf()))) {
	        return false;
	      }

	      break;

	    case 'Date':
	      if (!_objectIs$1(a.valueOf(), b.valueOf())) {
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

	    if (!(_has(key, b) && _equals(b[key], a[key], extendedStackA, extendedStackB))) {
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
	 *      const a = {}; a.v = a;
	 *      const b = {}; b.v = b;
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

	function _includes(a, list) {
	  return _indexOf(list, a, 0) >= 0;
	}

	var max$3 = Math.max;
	var min$5 = Math.min;
	var floor$4 = Math.floor;
	var SUBSTITUTION_SYMBOLS = /\$([$&'`]|\d\d?|<[^>]*>)/g;
	var SUBSTITUTION_SYMBOLS_NO_NAMED = /\$([$&'`]|\d\d?)/g;

	var maybeToString = function (it) {
	  return it === undefined ? it : String(it);
	};

	// @@replace logic
	fixRegexpWellKnownSymbolLogic('replace', 2, function (REPLACE, nativeReplace, maybeCallNative, reason) {
	  var REGEXP_REPLACE_SUBSTITUTES_UNDEFINED_CAPTURE = reason.REGEXP_REPLACE_SUBSTITUTES_UNDEFINED_CAPTURE;
	  var REPLACE_KEEPS_$0 = reason.REPLACE_KEEPS_$0;
	  var UNSAFE_SUBSTITUTE = REGEXP_REPLACE_SUBSTITUTES_UNDEFINED_CAPTURE ? '$' : '$0';

	  return [
	    // `String.prototype.replace` method
	    // https://tc39.github.io/ecma262/#sec-string.prototype.replace
	    function replace(searchValue, replaceValue) {
	      var O = requireObjectCoercible(this);
	      var replacer = searchValue == undefined ? undefined : searchValue[REPLACE];
	      return replacer !== undefined
	        ? replacer.call(searchValue, O, replaceValue)
	        : nativeReplace.call(String(O), searchValue, replaceValue);
	    },
	    // `RegExp.prototype[@@replace]` method
	    // https://tc39.github.io/ecma262/#sec-regexp.prototype-@@replace
	    function (regexp, replaceValue) {
	      if (
	        (!REGEXP_REPLACE_SUBSTITUTES_UNDEFINED_CAPTURE && REPLACE_KEEPS_$0) ||
	        (typeof replaceValue === 'string' && replaceValue.indexOf(UNSAFE_SUBSTITUTE) === -1)
	      ) {
	        var res = maybeCallNative(nativeReplace, regexp, this, replaceValue);
	        if (res.done) return res.value;
	      }

	      var rx = anObject(regexp);
	      var S = String(this);

	      var functionalReplace = typeof replaceValue === 'function';
	      if (!functionalReplace) replaceValue = String(replaceValue);

	      var global = rx.global;
	      if (global) {
	        var fullUnicode = rx.unicode;
	        rx.lastIndex = 0;
	      }
	      var results = [];
	      while (true) {
	        var result = regexpExecAbstract(rx, S);
	        if (result === null) break;

	        results.push(result);
	        if (!global) break;

	        var matchStr = String(result[0]);
	        if (matchStr === '') rx.lastIndex = advanceStringIndex(S, toLength(rx.lastIndex), fullUnicode);
	      }

	      var accumulatedResult = '';
	      var nextSourcePosition = 0;
	      for (var i = 0; i < results.length; i++) {
	        result = results[i];

	        var matched = String(result[0]);
	        var position = max$3(min$5(toInteger(result.index), S.length), 0);
	        var captures = [];
	        // NOTE: This is equivalent to
	        //   captures = result.slice(1).map(maybeToString)
	        // but for some reason `nativeSlice.call(result, 1, result.length)` (called in
	        // the slice polyfill when slicing native arrays) "doesn't work" in safari 9 and
	        // causes a crash (https://pastebin.com/N21QzeQA) when trying to debug it.
	        for (var j = 1; j < result.length; j++) captures.push(maybeToString(result[j]));
	        var namedCaptures = result.groups;
	        if (functionalReplace) {
	          var replacerArgs = [matched].concat(captures, position, S);
	          if (namedCaptures !== undefined) replacerArgs.push(namedCaptures);
	          var replacement = String(replaceValue.apply(undefined, replacerArgs));
	        } else {
	          replacement = getSubstitution(matched, S, position, captures, namedCaptures, replaceValue);
	        }
	        if (position >= nextSourcePosition) {
	          accumulatedResult += S.slice(nextSourcePosition, position) + replacement;
	          nextSourcePosition = position + matched.length;
	        }
	      }
	      return accumulatedResult + S.slice(nextSourcePosition);
	    }
	  ];

	  // https://tc39.github.io/ecma262/#sec-getsubstitution
	  function getSubstitution(matched, str, position, captures, namedCaptures, replacement) {
	    var tailPos = position + matched.length;
	    var m = captures.length;
	    var symbols = SUBSTITUTION_SYMBOLS_NO_NAMED;
	    if (namedCaptures !== undefined) {
	      namedCaptures = toObject(namedCaptures);
	      symbols = SUBSTITUTION_SYMBOLS;
	    }
	    return nativeReplace.call(replacement, symbols, function (match, ch) {
	      var capture;
	      switch (ch.charAt(0)) {
	        case '$': return '$';
	        case '&': return matched;
	        case '`': return str.slice(0, position);
	        case "'": return str.slice(tailPos);
	        case '<':
	          capture = namedCaptures[ch.slice(1, -1)];
	          break;
	        default: // \d\d?
	          var n = +ch;
	          if (n === 0) return match;
	          if (n > m) {
	            var f = floor$4(n / 10);
	            if (f === 0) return match;
	            if (f <= m) return captures[f - 1] === undefined ? ch.charAt(1) : captures[f - 1] + ch.charAt(1);
	            return match;
	          }
	          capture = captures[n - 1];
	      }
	      return capture === undefined ? '' : capture;
	    });
	  }
	});

	function _quote(s) {
	  var escaped = s.replace(/\\/g, '\\\\').replace(/[\b]/g, '\\b') // \b matches word boundary; [\b] matches backspace
	  .replace(/\f/g, '\\f').replace(/\n/g, '\\n').replace(/\r/g, '\\r').replace(/\t/g, '\\t').replace(/\v/g, '\\v').replace(/\0/g, '\\0');
	  return '"' + escaped.replace(/"/g, '\\"') + '"';
	}

	// `String.prototype.repeat` method implementation
	// https://tc39.github.io/ecma262/#sec-string.prototype.repeat
	var stringRepeat = ''.repeat || function repeat(count) {
	  var str = String(requireObjectCoercible(this));
	  var result = '';
	  var n = toInteger(count);
	  if (n < 0 || n == Infinity) throw RangeError('Wrong number of repetitions');
	  for (;n > 0; (n >>>= 1) && (str += str)) if (n & 1) result += str;
	  return result;
	};

	// https://github.com/tc39/proposal-string-pad-start-end




	var ceil$1 = Math.ceil;

	// `String.prototype.{ padStart, padEnd }` methods implementation
	var createMethod$6 = function (IS_END) {
	  return function ($this, maxLength, fillString) {
	    var S = String(requireObjectCoercible($this));
	    var stringLength = S.length;
	    var fillStr = fillString === undefined ? ' ' : String(fillString);
	    var intMaxLength = toLength(maxLength);
	    var fillLen, stringFiller;
	    if (intMaxLength <= stringLength || fillStr == '') return S;
	    fillLen = intMaxLength - stringLength;
	    stringFiller = stringRepeat.call(fillStr, ceil$1(fillLen / fillStr.length));
	    if (stringFiller.length > fillLen) stringFiller = stringFiller.slice(0, fillLen);
	    return IS_END ? S + stringFiller : stringFiller + S;
	  };
	};

	var stringPad = {
	  // `String.prototype.padStart` method
	  // https://tc39.github.io/ecma262/#sec-string.prototype.padstart
	  start: createMethod$6(false),
	  // `String.prototype.padEnd` method
	  // https://tc39.github.io/ecma262/#sec-string.prototype.padend
	  end: createMethod$6(true)
	};

	var padStart = stringPad.start;

	var abs$1 = Math.abs;
	var DatePrototype$1 = Date.prototype;
	var getTime$1 = DatePrototype$1.getTime;
	var nativeDateToISOString = DatePrototype$1.toISOString;

	// `Date.prototype.toISOString` method implementation
	// https://tc39.github.io/ecma262/#sec-date.prototype.toisostring
	// PhantomJS / old WebKit fails here:
	var dateToIsoString = (fails(function () {
	  return nativeDateToISOString.call(new Date(-5e13 - 1)) != '0385-07-25T07:06:39.999Z';
	}) || !fails(function () {
	  nativeDateToISOString.call(new Date(NaN));
	})) ? function toISOString() {
	  if (!isFinite(getTime$1.call(this))) throw RangeError('Invalid time value');
	  var date = this;
	  var year = date.getUTCFullYear();
	  var milliseconds = date.getUTCMilliseconds();
	  var sign = year < 0 ? '-' : year > 9999 ? '+' : '';
	  return sign + padStart(abs$1(year), sign ? 6 : 4, 0) +
	    '-' + padStart(date.getUTCMonth() + 1, 2, 0) +
	    '-' + padStart(date.getUTCDate(), 2, 0) +
	    'T' + padStart(date.getUTCHours(), 2, 0) +
	    ':' + padStart(date.getUTCMinutes(), 2, 0) +
	    ':' + padStart(date.getUTCSeconds(), 2, 0) +
	    '.' + padStart(milliseconds, 3, 0) +
	    'Z';
	} : nativeDateToISOString;

	// `Date.prototype.toISOString` method
	// https://tc39.github.io/ecma262/#sec-date.prototype.toisostring
	// PhantomJS / old WebKit has a broken implementations
	_export({ target: 'Date', proto: true, forced: Date.prototype.toISOString !== dateToIsoString }, {
	  toISOString: dateToIsoString
	});

	// `thisNumberValue` abstract operation
	// https://tc39.github.io/ecma262/#sec-thisnumbervalue
	var thisNumberValue = function (value) {
	  if (typeof value != 'number' && classofRaw(value) != 'Number') {
	    throw TypeError('Incorrect invocation');
	  }
	  return +value;
	};

	var nativeToFixed = 1.0.toFixed;
	var floor$5 = Math.floor;

	var pow$1 = function (x, n, acc) {
	  return n === 0 ? acc : n % 2 === 1 ? pow$1(x, n - 1, acc * x) : pow$1(x * x, n / 2, acc);
	};

	var log$1 = function (x) {
	  var n = 0;
	  var x2 = x;
	  while (x2 >= 4096) {
	    n += 12;
	    x2 /= 4096;
	  }
	  while (x2 >= 2) {
	    n += 1;
	    x2 /= 2;
	  } return n;
	};

	var FORCED$9 = nativeToFixed && (
	  0.00008.toFixed(3) !== '0.000' ||
	  0.9.toFixed(0) !== '1' ||
	  1.255.toFixed(2) !== '1.25' ||
	  1000000000000000128.0.toFixed(0) !== '1000000000000000128'
	) || !fails(function () {
	  // V8 ~ Android 4.3-
	  nativeToFixed.call({});
	});

	// `Number.prototype.toFixed` method
	// https://tc39.github.io/ecma262/#sec-number.prototype.tofixed
	_export({ target: 'Number', proto: true, forced: FORCED$9 }, {
	  // eslint-disable-next-line max-statements
	  toFixed: function toFixed(fractionDigits) {
	    var number = thisNumberValue(this);
	    var fractDigits = toInteger(fractionDigits);
	    var data = [0, 0, 0, 0, 0, 0];
	    var sign = '';
	    var result = '0';
	    var e, z, j, k;

	    var multiply = function (n, c) {
	      var index = -1;
	      var c2 = c;
	      while (++index < 6) {
	        c2 += n * data[index];
	        data[index] = c2 % 1e7;
	        c2 = floor$5(c2 / 1e7);
	      }
	    };

	    var divide = function (n) {
	      var index = 6;
	      var c = 0;
	      while (--index >= 0) {
	        c += data[index];
	        data[index] = floor$5(c / n);
	        c = (c % n) * 1e7;
	      }
	    };

	    var dataToString = function () {
	      var index = 6;
	      var s = '';
	      while (--index >= 0) {
	        if (s !== '' || index === 0 || data[index] !== 0) {
	          var t = String(data[index]);
	          s = s === '' ? t : s + stringRepeat.call('0', 7 - t.length) + t;
	        }
	      } return s;
	    };

	    if (fractDigits < 0 || fractDigits > 20) throw RangeError('Incorrect fraction digits');
	    // eslint-disable-next-line no-self-compare
	    if (number != number) return 'NaN';
	    if (number <= -1e21 || number >= 1e21) return String(number);
	    if (number < 0) {
	      sign = '-';
	      number = -number;
	    }
	    if (number > 1e-21) {
	      e = log$1(number * pow$1(2, 69, 1)) - 69;
	      z = e < 0 ? number * pow$1(2, -e, 1) : number / pow$1(2, e, 1);
	      z *= 0x10000000000000;
	      e = 52 - e;
	      if (e > 0) {
	        multiply(0, z);
	        j = fractDigits;
	        while (j >= 7) {
	          multiply(1e7, 0);
	          j -= 7;
	        }
	        multiply(pow$1(10, j, 1), 0);
	        j = e - 1;
	        while (j >= 23) {
	          divide(1 << 23);
	          j -= 23;
	        }
	        divide(1 << j);
	        multiply(1, 1);
	        divide(2);
	        result = dataToString();
	      } else {
	        multiply(0, z);
	        multiply(1 << -e, 0);
	        result = dataToString() + stringRepeat.call('0', fractDigits);
	      }
	    }
	    if (fractDigits > 0) {
	      k = result.length;
	      result = sign + (k <= fractDigits
	        ? '0.' + stringRepeat.call('0', fractDigits - k) + result
	        : result.slice(0, k - fractDigits) + '.' + result.slice(k - fractDigits));
	    } else {
	      result = sign + result;
	    } return result;
	  }
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
	 *      const isEven = n => n % 2 === 0;
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
	 *      const isOdd = (n) => n % 2 === 1;
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
	    return _includes(y, xs) ? '<Circular>' : _toString(y, xs);
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

	var toString$3 =
	/*#__PURE__*/
	_curry1(function toString(val) {
	  return _toString(val, []);
	});

	/**
	 * Accepts a converging function and a list of branching functions and returns
	 * a new function. The arity of the new function is the same as the arity of
	 * the longest branching function. When invoked, this new function is applied
	 * to some arguments, and each branching function is applied to those same
	 * arguments. The results of each branching function are passed as arguments
	 * to the converging function to produce the return value.
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
	 *      const average = R.converge(R.divide, [R.sum, R.length])
	 *      average([1, 2, 3, 4, 5, 6, 7]) //=> 4
	 *
	 *      const strangeConcat = R.converge(R.concat, [R.toUpper, R.toLower])
	 *      strangeConcat("Yodel") //=> "YODELyodel"
	 *
	 * @symb R.converge(f, [g, h])(a, b) = f(g(a, b), h(a, b))
	 */

	var converge =
	/*#__PURE__*/
	_curry2(function converge(after, fns) {
	  return curryN(reduce(max$2, 0, pluck('length', fns)), function () {
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
	      if (_has(key, this.inputs)) {
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
	 *      const groupNames = (acc, {name}) => acc.concat(name)
	 *      const toGrade = ({score}) =>
	 *        score < 65 ? 'F' :
	 *        score < 70 ? 'D' :
	 *        score < 80 ? 'C' :
	 *        score < 90 ? 'B' : 'A'
	 *
	 *      var students = [
	 *        {name: 'Abby', score: 83},
	 *        {name: 'Bart', score: 62},
	 *        {name: 'Curt', score: 88},
	 *        {name: 'Dora', score: 92},
	 *      ]
	 *
	 *      reduceBy(groupNames, [], toGrade, students)
	 *      //=> {"A": ["Dora"], "B": ["Abby", "Curt"], "F": ["Bart"]}
	 */

	var reduceBy =
	/*#__PURE__*/
	_curryN(4, [],
	/*#__PURE__*/
	_dispatchable([], _xreduceBy, function reduceBy(valueFn, valueAcc, keyFn, list) {
	  return _reduce(function (acc, elt) {
	    var key = keyFn(elt);
	    acc[key] = valueFn(_has(key, acc) ? acc[key] : _clone(valueAcc, [], [], false), elt);
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
	 *      const numbers = [1.0, 1.1, 1.2, 2.0, 3.0, 2.2];
	 *      R.countBy(Math.floor)(numbers);    //=> {'1': 3, '2': 2, '3': 1}
	 *
	 *      const letters = ['a', 'b', 'A', 'a', 'B', 'c'];
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

	var freezing = !fails(function () {
	  return Object.isExtensible(Object.preventExtensions({}));
	});

	var internalMetadata = createCommonjsModule(function (module) {
	var defineProperty = objectDefineProperty.f;



	var METADATA = uid('meta');
	var id = 0;

	var isExtensible = Object.isExtensible || function () {
	  return true;
	};

	var setMetadata = function (it) {
	  defineProperty(it, METADATA, { value: {
	    objectID: 'O' + ++id, // object ID
	    weakData: {}          // weak collections IDs
	  } });
	};

	var fastKey = function (it, create) {
	  // return a primitive with prefix
	  if (!isObject(it)) return typeof it == 'symbol' ? it : (typeof it == 'string' ? 'S' : 'P') + it;
	  if (!has(it, METADATA)) {
	    // can't set metadata to uncaught frozen object
	    if (!isExtensible(it)) return 'F';
	    // not necessary to add metadata
	    if (!create) return 'E';
	    // add missing metadata
	    setMetadata(it);
	  // return object ID
	  } return it[METADATA].objectID;
	};

	var getWeakData = function (it, create) {
	  if (!has(it, METADATA)) {
	    // can't set metadata to uncaught frozen object
	    if (!isExtensible(it)) return true;
	    // not necessary to add metadata
	    if (!create) return false;
	    // add missing metadata
	    setMetadata(it);
	  // return the store of weak collections IDs
	  } return it[METADATA].weakData;
	};

	// add metadata on freeze-family methods calling
	var onFreeze = function (it) {
	  if (freezing && meta.REQUIRED && isExtensible(it) && !has(it, METADATA)) setMetadata(it);
	  return it;
	};

	var meta = module.exports = {
	  REQUIRED: false,
	  fastKey: fastKey,
	  getWeakData: getWeakData,
	  onFreeze: onFreeze
	};

	hiddenKeys[METADATA] = true;
	});
	var internalMetadata_1 = internalMetadata.REQUIRED;
	var internalMetadata_2 = internalMetadata.fastKey;
	var internalMetadata_3 = internalMetadata.getWeakData;
	var internalMetadata_4 = internalMetadata.onFreeze;

	var collection = function (CONSTRUCTOR_NAME, wrapper, common) {
	  var IS_MAP = CONSTRUCTOR_NAME.indexOf('Map') !== -1;
	  var IS_WEAK = CONSTRUCTOR_NAME.indexOf('Weak') !== -1;
	  var ADDER = IS_MAP ? 'set' : 'add';
	  var NativeConstructor = global_1[CONSTRUCTOR_NAME];
	  var NativePrototype = NativeConstructor && NativeConstructor.prototype;
	  var Constructor = NativeConstructor;
	  var exported = {};

	  var fixMethod = function (KEY) {
	    var nativeMethod = NativePrototype[KEY];
	    redefine(NativePrototype, KEY,
	      KEY == 'add' ? function add(value) {
	        nativeMethod.call(this, value === 0 ? 0 : value);
	        return this;
	      } : KEY == 'delete' ? function (key) {
	        return IS_WEAK && !isObject(key) ? false : nativeMethod.call(this, key === 0 ? 0 : key);
	      } : KEY == 'get' ? function get(key) {
	        return IS_WEAK && !isObject(key) ? undefined : nativeMethod.call(this, key === 0 ? 0 : key);
	      } : KEY == 'has' ? function has(key) {
	        return IS_WEAK && !isObject(key) ? false : nativeMethod.call(this, key === 0 ? 0 : key);
	      } : function set(key, value) {
	        nativeMethod.call(this, key === 0 ? 0 : key, value);
	        return this;
	      }
	    );
	  };

	  // eslint-disable-next-line max-len
	  if (isForced_1(CONSTRUCTOR_NAME, typeof NativeConstructor != 'function' || !(IS_WEAK || NativePrototype.forEach && !fails(function () {
	    new NativeConstructor().entries().next();
	  })))) {
	    // create collection constructor
	    Constructor = common.getConstructor(wrapper, CONSTRUCTOR_NAME, IS_MAP, ADDER);
	    internalMetadata.REQUIRED = true;
	  } else if (isForced_1(CONSTRUCTOR_NAME, true)) {
	    var instance = new Constructor();
	    // early implementations not supports chaining
	    var HASNT_CHAINING = instance[ADDER](IS_WEAK ? {} : -0, 1) != instance;
	    // V8 ~ Chromium 40- weak-collections throws on primitives, but should return false
	    var THROWS_ON_PRIMITIVES = fails(function () { instance.has(1); });
	    // most early implementations doesn't supports iterables, most modern - not close it correctly
	    // eslint-disable-next-line no-new
	    var ACCEPT_ITERABLES = checkCorrectnessOfIteration(function (iterable) { new NativeConstructor(iterable); });
	    // for early implementations -0 and +0 not the same
	    var BUGGY_ZERO = !IS_WEAK && fails(function () {
	      // V8 ~ Chromium 42- fails only with 5+ elements
	      var $instance = new NativeConstructor();
	      var index = 5;
	      while (index--) $instance[ADDER](index, index);
	      return !$instance.has(-0);
	    });

	    if (!ACCEPT_ITERABLES) {
	      Constructor = wrapper(function (dummy, iterable) {
	        anInstance(dummy, Constructor, CONSTRUCTOR_NAME);
	        var that = inheritIfRequired(new NativeConstructor(), dummy, Constructor);
	        if (iterable != undefined) iterate_1(iterable, that[ADDER], that, IS_MAP);
	        return that;
	      });
	      Constructor.prototype = NativePrototype;
	      NativePrototype.constructor = Constructor;
	    }

	    if (THROWS_ON_PRIMITIVES || BUGGY_ZERO) {
	      fixMethod('delete');
	      fixMethod('has');
	      IS_MAP && fixMethod('get');
	    }

	    if (BUGGY_ZERO || HASNT_CHAINING) fixMethod(ADDER);

	    // weak collections should not contains .clear method
	    if (IS_WEAK && NativePrototype.clear) delete NativePrototype.clear;
	  }

	  exported[CONSTRUCTOR_NAME] = Constructor;
	  _export({ global: true, forced: Constructor != NativeConstructor }, exported);

	  setToStringTag(Constructor, CONSTRUCTOR_NAME);

	  if (!IS_WEAK) common.setStrong(Constructor, CONSTRUCTOR_NAME, IS_MAP);

	  return Constructor;
	};

	var defineProperty$a = objectDefineProperty.f;








	var fastKey = internalMetadata.fastKey;


	var setInternalState$6 = internalState.set;
	var internalStateGetterFor = internalState.getterFor;

	var collectionStrong = {
	  getConstructor: function (wrapper, CONSTRUCTOR_NAME, IS_MAP, ADDER) {
	    var C = wrapper(function (that, iterable) {
	      anInstance(that, C, CONSTRUCTOR_NAME);
	      setInternalState$6(that, {
	        type: CONSTRUCTOR_NAME,
	        index: objectCreate(null),
	        first: undefined,
	        last: undefined,
	        size: 0
	      });
	      if (!descriptors) that.size = 0;
	      if (iterable != undefined) iterate_1(iterable, that[ADDER], that, IS_MAP);
	    });

	    var getInternalState = internalStateGetterFor(CONSTRUCTOR_NAME);

	    var define = function (that, key, value) {
	      var state = getInternalState(that);
	      var entry = getEntry(that, key);
	      var previous, index;
	      // change existing entry
	      if (entry) {
	        entry.value = value;
	      // create new entry
	      } else {
	        state.last = entry = {
	          index: index = fastKey(key, true),
	          key: key,
	          value: value,
	          previous: previous = state.last,
	          next: undefined,
	          removed: false
	        };
	        if (!state.first) state.first = entry;
	        if (previous) previous.next = entry;
	        if (descriptors) state.size++;
	        else that.size++;
	        // add to index
	        if (index !== 'F') state.index[index] = entry;
	      } return that;
	    };

	    var getEntry = function (that, key) {
	      var state = getInternalState(that);
	      // fast case
	      var index = fastKey(key);
	      var entry;
	      if (index !== 'F') return state.index[index];
	      // frozen object case
	      for (entry = state.first; entry; entry = entry.next) {
	        if (entry.key == key) return entry;
	      }
	    };

	    redefineAll(C.prototype, {
	      // 23.1.3.1 Map.prototype.clear()
	      // 23.2.3.2 Set.prototype.clear()
	      clear: function clear() {
	        var that = this;
	        var state = getInternalState(that);
	        var data = state.index;
	        var entry = state.first;
	        while (entry) {
	          entry.removed = true;
	          if (entry.previous) entry.previous = entry.previous.next = undefined;
	          delete data[entry.index];
	          entry = entry.next;
	        }
	        state.first = state.last = undefined;
	        if (descriptors) state.size = 0;
	        else that.size = 0;
	      },
	      // 23.1.3.3 Map.prototype.delete(key)
	      // 23.2.3.4 Set.prototype.delete(value)
	      'delete': function (key) {
	        var that = this;
	        var state = getInternalState(that);
	        var entry = getEntry(that, key);
	        if (entry) {
	          var next = entry.next;
	          var prev = entry.previous;
	          delete state.index[entry.index];
	          entry.removed = true;
	          if (prev) prev.next = next;
	          if (next) next.previous = prev;
	          if (state.first == entry) state.first = next;
	          if (state.last == entry) state.last = prev;
	          if (descriptors) state.size--;
	          else that.size--;
	        } return !!entry;
	      },
	      // 23.2.3.6 Set.prototype.forEach(callbackfn, thisArg = undefined)
	      // 23.1.3.5 Map.prototype.forEach(callbackfn, thisArg = undefined)
	      forEach: function forEach(callbackfn /* , that = undefined */) {
	        var state = getInternalState(this);
	        var boundFunction = functionBindContext(callbackfn, arguments.length > 1 ? arguments[1] : undefined, 3);
	        var entry;
	        while (entry = entry ? entry.next : state.first) {
	          boundFunction(entry.value, entry.key, this);
	          // revert to the last existing entry
	          while (entry && entry.removed) entry = entry.previous;
	        }
	      },
	      // 23.1.3.7 Map.prototype.has(key)
	      // 23.2.3.7 Set.prototype.has(value)
	      has: function has(key) {
	        return !!getEntry(this, key);
	      }
	    });

	    redefineAll(C.prototype, IS_MAP ? {
	      // 23.1.3.6 Map.prototype.get(key)
	      get: function get(key) {
	        var entry = getEntry(this, key);
	        return entry && entry.value;
	      },
	      // 23.1.3.9 Map.prototype.set(key, value)
	      set: function set(key, value) {
	        return define(this, key === 0 ? 0 : key, value);
	      }
	    } : {
	      // 23.2.3.1 Set.prototype.add(value)
	      add: function add(value) {
	        return define(this, value = value === 0 ? 0 : value, value);
	      }
	    });
	    if (descriptors) defineProperty$a(C.prototype, 'size', {
	      get: function () {
	        return getInternalState(this).size;
	      }
	    });
	    return C;
	  },
	  setStrong: function (C, CONSTRUCTOR_NAME, IS_MAP) {
	    var ITERATOR_NAME = CONSTRUCTOR_NAME + ' Iterator';
	    var getInternalCollectionState = internalStateGetterFor(CONSTRUCTOR_NAME);
	    var getInternalIteratorState = internalStateGetterFor(ITERATOR_NAME);
	    // add .keys, .values, .entries, [@@iterator]
	    // 23.1.3.4, 23.1.3.8, 23.1.3.11, 23.1.3.12, 23.2.3.5, 23.2.3.8, 23.2.3.10, 23.2.3.11
	    defineIterator(C, CONSTRUCTOR_NAME, function (iterated, kind) {
	      setInternalState$6(this, {
	        type: ITERATOR_NAME,
	        target: iterated,
	        state: getInternalCollectionState(iterated),
	        kind: kind,
	        last: undefined
	      });
	    }, function () {
	      var state = getInternalIteratorState(this);
	      var kind = state.kind;
	      var entry = state.last;
	      // revert to the last existing entry
	      while (entry && entry.removed) entry = entry.previous;
	      // get next entry
	      if (!state.target || !(state.last = entry = entry ? entry.next : state.state.first)) {
	        // or finish the iteration
	        state.target = undefined;
	        return { value: undefined, done: true };
	      }
	      // return step by kind
	      if (kind == 'keys') return { value: entry.key, done: false };
	      if (kind == 'values') return { value: entry.value, done: false };
	      return { value: [entry.key, entry.value], done: false };
	    }, IS_MAP ? 'entries' : 'values', !IS_MAP, true);

	    // add [@@species], 23.1.2.2, 23.2.2.2
	    setSpecies(CONSTRUCTOR_NAME);
	  }
	};

	// `Set` constructor
	// https://tc39.github.io/ecma262/#sec-set-objects
	var es_set = collection('Set', function (init) {
	  return function Set() { return init(this, arguments.length ? arguments[0] : undefined); };
	}, collectionStrong);

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

	        if (!_includes(item, set._items[type])) {
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


	      if (!_includes(item, set._items[type])) {
	        if (shouldAdd) {
	          set._items[type].push(item);
	        }

	        return false;
	      }

	      return true;
	  }
	} // A simple Set type that honours R.equals semantics

	var HAS_SPECIES_SUPPORT$3 = arrayMethodHasSpeciesSupport('splice');
	var USES_TO_LENGTH$9 = arrayMethodUsesToLength('splice', { ACCESSORS: true, 0: 0, 1: 2 });

	var max$4 = Math.max;
	var min$6 = Math.min;
	var MAX_SAFE_INTEGER$1 = 0x1FFFFFFFFFFFFF;
	var MAXIMUM_ALLOWED_LENGTH_EXCEEDED = 'Maximum allowed length exceeded';

	// `Array.prototype.splice` method
	// https://tc39.github.io/ecma262/#sec-array.prototype.splice
	// with adding support of @@species
	_export({ target: 'Array', proto: true, forced: !HAS_SPECIES_SUPPORT$3 || !USES_TO_LENGTH$9 }, {
	  splice: function splice(start, deleteCount /* , ...items */) {
	    var O = toObject(this);
	    var len = toLength(O.length);
	    var actualStart = toAbsoluteIndex(start, len);
	    var argumentsLength = arguments.length;
	    var insertCount, actualDeleteCount, A, k, from, to;
	    if (argumentsLength === 0) {
	      insertCount = actualDeleteCount = 0;
	    } else if (argumentsLength === 1) {
	      insertCount = 0;
	      actualDeleteCount = len - actualStart;
	    } else {
	      insertCount = argumentsLength - 2;
	      actualDeleteCount = min$6(max$4(toInteger(deleteCount), 0), len - actualStart);
	    }
	    if (len + insertCount - actualDeleteCount > MAX_SAFE_INTEGER$1) {
	      throw TypeError(MAXIMUM_ALLOWED_LENGTH_EXCEEDED);
	    }
	    A = arraySpeciesCreate(O, actualDeleteCount);
	    for (k = 0; k < actualDeleteCount; k++) {
	      from = actualStart + k;
	      if (from in O) createProperty(A, k, O[from]);
	    }
	    A.length = actualDeleteCount;
	    if (insertCount < actualDeleteCount) {
	      for (k = actualStart; k < len - actualDeleteCount; k++) {
	        from = k + actualDeleteCount;
	        to = k + insertCount;
	        if (from in O) O[to] = O[from];
	        else delete O[to];
	      }
	      for (k = len; k > len - actualDeleteCount + insertCount; k--) delete O[k - 1];
	    } else if (insertCount > actualDeleteCount) {
	      for (k = len - actualDeleteCount; k > actualStart; k--) {
	        from = k + actualDeleteCount - 1;
	        to = k + insertCount - 1;
	        if (from in O) O[to] = O[from];
	        else delete O[to];
	      }
	    }
	    for (k = 0; k < insertCount; k++) {
	      O[k + actualStart] = arguments[k + 2];
	    }
	    O.length = len - actualDeleteCount + insertCount;
	    return A;
	  }
	});

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
	 *      const personnel = [
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
	 *      const takeFive = R.take(5);
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
	  return slice$2(0, n < 0 ? Infinity : n, xs);
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
	 * Acts as a transducer if a transformer is given in list position.
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

	var last$1 =
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
	 *      const l = [1, -1, 1, 3, 4, -4, -4, -5, 5, 3, 3];
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
	      if (!pred(last$1(result), list[idx])) {
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
	 *      const mergeThree = (a, b, c) => [].concat(a, b, c);
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

	var fromPairs =
	/*#__PURE__*/
	_curry1(function fromPairs(pairs) {
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
	 * @see R.reduceBy, R.transduce
	 * @example
	 *
	 *      const byGrade = R.groupBy(function(student) {
	 *        const score = student.score;
	 *        return score < 65 ? 'F' :
	 *               score < 70 ? 'D' :
	 *               score < 80 ? 'C' :
	 *               score < 90 ? 'B' : 'A';
	 *      });
	 *      const students = [{name: 'Abby', score: 84},
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
	 *      const list = [{id: 'xyz', title: 'A'}, {id: 'abc', title: 'B'}];
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
	slice$2(0, -1);

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
	 * @param {String} method Name of any of the target object's methods to call.
	 * @return {Function} A new curried function.
	 * @see R.construct
	 * @example
	 *
	 *      const sliceFrom = R.invoker(1, 'slice');
	 *      sliceFrom(6, 'abcdefghijklm'); //=> 'ghijklm'
	 *      const sliceFrom6 = R.invoker(2, 'slice')(6);
	 *      sliceFrom6(8, 'abcdefghijklm'); //=> 'gh'
	 *
	 *      const dog = {
	 *        speak: async () => 'Woof!'
	 *      };
	 *      const speak = R.invoker(0, 'speak');
	 *      speak(dog).then(console.log) //~> 'Woof!'
	 *
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

	    throw new TypeError(toString$3(target) + ' does not have a method named "' + method + '"');
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
	 *      const spacer = R.join(' ');
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
	 *      const getRange = R.juxt([Math.min, Math.max]);
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

	// `Array.prototype.lastIndexOf` method
	// https://tc39.github.io/ecma262/#sec-array.prototype.lastindexof
	_export({ target: 'Array', proto: true, forced: arrayLastIndexOf !== [].lastIndexOf }, {
	  lastIndexOf: arrayLastIndexOf
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
	 *      const square = n => n * n;
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
	 *      const square = n => n * n;
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
	 *      const double = R.multiply(2);
	 *      const triple = R.multiply(3);
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
	 *      const greet = (salutation, title, firstName, lastName) =>
	 *        salutation + ', ' + title + ' ' + firstName + ' ' + lastName + '!';
	 *
	 *      const greetMsJaneJones = R.partialRight(greet, ['Ms.', 'Jane', 'Jones']);
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
	 *      R.partition(R.includes('s'), ['sss', 'ttt', 'foo', 'bars']);
	 *      // => [ [ 'sss', 'bars' ],  [ 'ttt', 'foo' ] ]
	 *
	 *      R.partition(R.includes('s'), { a: 'sss', b: 'ttt', foo: 'bars' });
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
	 *      const abby = {name: 'Abby', age: 7, hair: 'blond', grade: 2};
	 *      const fred = {name: 'Fred', age: 12, hair: 'brown', grade: 7};
	 *      const kids = [abby, fred];
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
	 * @return {Array} The array of strings from `str` separated by `sep`.
	 * @see R.join
	 * @example
	 *
	 *      const pathComponents = R.split('/');
	 *      R.tail(pathComponents('/usr/local/bin/node')); //=> ['usr', 'local', 'bin', 'node']
	 *
	 *      R.split('.', 'a.b.c.xyz.d'); //=> ['a', 'b', 'c', 'xyz', 'd']
	 */

	var split$1 =
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
	    if (_has(prop, obj)) {
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
	 *      const numbers = [1, 2, 3, 4];
	 *      const transducer = R.compose(R.map(R.add(1)), R.take(2));
	 *      R.transduce(transducer, R.flip(R.append), [], numbers); //=> [2, 3]
	 *
	 *      const isOdd = (x) => x % 2 === 1;
	 *      const firstOddTransducer = R.compose(R.filter(isOdd), R.take(1));
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

	var trim$2 = !hasProtoTrim ||
	/*#__PURE__*/
	ws.trim() || !
	/*#__PURE__*/
	zeroWidth.trim() ?
	/*#__PURE__*/
	_curry1(function trim(str) {
	  var beginRx = new RegExp('^[' + ws + '][' + ws + ']*');
	  var endRx = new RegExp('[' + ws + '][' + ws + ']*$');
	  return str.replace(beginRx, '').replace(endRx, '');
	}) :
	/*#__PURE__*/
	_curry1(function trim(str) {
	  return str.trim();
	});

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

	var $some$1 = arrayIteration.some;



	var STRICT_METHOD$6 = arrayMethodIsStrict('some');
	var USES_TO_LENGTH$a = arrayMethodUsesToLength('some');

	// `Array.prototype.some` method
	// https://tc39.github.io/ecma262/#sec-array.prototype.some
	_export({ target: 'Array', proto: true, forced: !STRICT_METHOD$6 || !USES_TO_LENGTH$a }, {
	  some: function some(callbackfn /* , thisArg */) {
	    return $some$1(this, callbackfn, arguments.length > 1 ? arguments[1] : undefined);
	  }
	});

	// `Date.now` method
	// https://tc39.github.io/ecma262/#sec-date.now
	_export({ target: 'Date', stat: true }, {
	  now: function now() {
	    return new Date().getTime();
	  }
	});

	var FORCED$a = fails(function () {
	  return new Date(NaN).toJSON() !== null
	    || Date.prototype.toJSON.call({ toISOString: function () { return 1; } }) !== 1;
	});

	// `Date.prototype.toJSON` method
	// https://tc39.github.io/ecma262/#sec-date.prototype.tojson
	_export({ target: 'Date', proto: true, forced: FORCED$a }, {
	  // eslint-disable-next-line no-unused-vars
	  toJSON: function toJSON(key) {
	    var O = toObject(this);
	    var pv = toPrimitive(O);
	    return typeof pv == 'number' && !isFinite(pv) ? null : O.toISOString();
	  }
	});

	var nativeGetOwnPropertyNames$2 = objectGetOwnPropertyNamesExternal.f;

	var FAILS_ON_PRIMITIVES$2 = fails(function () { return !Object.getOwnPropertyNames(1); });

	// `Object.getOwnPropertyNames` method
	// https://tc39.github.io/ecma262/#sec-object.getownpropertynames
	_export({ target: 'Object', stat: true, forced: FAILS_ON_PRIMITIVES$2 }, {
	  getOwnPropertyNames: nativeGetOwnPropertyNames$2
	});

	var nativeIsFrozen = Object.isFrozen;
	var FAILS_ON_PRIMITIVES$3 = fails(function () { });

	// `Object.isFrozen` method
	// https://tc39.github.io/ecma262/#sec-object.isfrozen
	_export({ target: 'Object', stat: true, forced: FAILS_ON_PRIMITIVES$3 }, {
	  isFrozen: function isFrozen(it) {
	    return isObject(it) ? nativeIsFrozen ? nativeIsFrozen(it) : false : true;
	  }
	});

	// `parseFloat` method
	// https://tc39.github.io/ecma262/#sec-parsefloat-string
	_export({ global: true, forced: parseFloat != numberParseFloat }, {
	  parseFloat: numberParseFloat
	});

	var trim$3 = stringTrim.trim;


	var $parseInt = global_1.parseInt;
	var hex = /^[+-]?0[Xx]/;
	var FORCED$b = $parseInt(whitespaces + '08') !== 8 || $parseInt(whitespaces + '0x16') !== 22;

	// `parseInt` method
	// https://tc39.github.io/ecma262/#sec-parseint-string-radix
	var numberParseInt = FORCED$b ? function parseInt(string, radix) {
	  var S = trim$3(String(string));
	  return $parseInt(S, (radix >>> 0) || (hex.test(S) ? 16 : 10));
	} : $parseInt;

	// `parseInt` method
	// https://tc39.github.io/ecma262/#sec-parseint-string-radix
	_export({ global: true, forced: parseInt != numberParseInt }, {
	  parseInt: numberParseInt
	});

	// `URL.prototype.toJSON` method
	// https://url.spec.whatwg.org/#dom-url-tojson
	_export({ target: 'URL', proto: true, enumerable: true }, {
	  toJSON: function toJSON() {
	    return URL.prototype.toString.call(this);
	  }
	});

	var moment = createCommonjsModule(function (module, exports) {

	  (function (global, factory) {
	    module.exports = factory();
	  })(commonjsGlobal, function () {

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

	      if (!locales[name] && 'object' !== 'undefined' && module && module.exports) {
	        try {
	          oldLocale = globalLocale._abbr;
	          var aliasedRequire = commonjsRequire;
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
	});

	// `Symbol.toPrimitive` well-known symbol
	// https://tc39.github.io/ecma262/#sec-symbol.toprimitive
	defineWellKnownSymbol('toPrimitive');

	var dateToPrimitive = function (hint) {
	  if (hint !== 'string' && hint !== 'number' && hint !== 'default') {
	    throw TypeError('Incorrect hint');
	  } return toPrimitive(anObject(this), hint !== 'number');
	};

	var TO_PRIMITIVE$1 = wellKnownSymbol('toPrimitive');
	var DatePrototype$2 = Date.prototype;

	// `Date.prototype[@@toPrimitive]` method
	// https://tc39.github.io/ecma262/#sec-date.prototype-@@toprimitive
	if (!(TO_PRIMITIVE$1 in DatePrototype$2)) {
	  createNonEnumerableProperty(DatePrototype$2, TO_PRIMITIVE$1, dateToPrimitive);
	}

	// `Object.defineProperties` method
	// https://tc39.github.io/ecma262/#sec-object.defineproperties
	_export({ target: 'Object', stat: true, forced: !descriptors, sham: !descriptors }, {
	  defineProperties: objectDefineProperties
	});

	// `Object.defineProperty` method
	// https://tc39.github.io/ecma262/#sec-object.defineproperty
	_export({ target: 'Object', stat: true, forced: !descriptors, sham: !descriptors }, {
	  defineProperty: objectDefineProperty.f
	});

	// @@search logic
	fixRegexpWellKnownSymbolLogic('search', 1, function (SEARCH, nativeSearch, maybeCallNative) {
	  return [
	    // `String.prototype.search` method
	    // https://tc39.github.io/ecma262/#sec-string.prototype.search
	    function search(regexp) {
	      var O = requireObjectCoercible(this);
	      var searcher = regexp == undefined ? undefined : regexp[SEARCH];
	      return searcher !== undefined ? searcher.call(regexp, O) : new RegExp(regexp)[SEARCH](String(O));
	    },
	    // `RegExp.prototype[@@search]` method
	    // https://tc39.github.io/ecma262/#sec-regexp.prototype-@@search
	    function (regexp) {
	      var res = maybeCallNative(nativeSearch, regexp, this);
	      if (res.done) return res.value;

	      var rx = anObject(regexp);
	      var S = String(this);

	      var previousLastIndex = rx.lastIndex;
	      if (!sameValue(previousLastIndex, 0)) rx.lastIndex = 0;
	      var result = regexpExecAbstract(rx, S);
	      if (!sameValue(rx.lastIndex, previousLastIndex)) rx.lastIndex = previousLastIndex;
	      return result === null ? -1 : result.index;
	    }
	  ];
	});

	var momentRange = createCommonjsModule(function (module, exports) {
	  !function (t, e) {
	    module.exports = e(moment);
	  }(commonjsGlobal, function (t) {
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
	          return t["default"];
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
	          "default": t
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
	              !r && a["return"] && a["return"]();
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
	          this.start = r || 0 === r ? (0, v["default"])(r) : (0, v["default"])(-864e13), this.end = o || 0 === o ? (0, v["default"])(o) : (0, v["default"])(864e13);
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
	            return this.overlaps(t, e) ? new this.constructor(v["default"].min(this.start, t.start), v["default"].max(this.end, t.end)) : null;
	          }
	        }, {
	          key: "by",
	          value: function value(t) {
	            var e = arguments.length > 1 && void 0 !== arguments[1] ? arguments[1] : {
	              excludeEnd: !1,
	              step: 1
	            },
	                n = this;
	            return o({}, p["default"].iterator, function () {
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
	            return e.hasOwnProperty("exclusive") && (a = e.exclusive), o({}, p["default"].iterator, function () {
	              return u === 1 / 0 ? {
	                done: !0
	              } : {
	                next: function next() {
	                  var e = (0, v["default"])(n.start.valueOf() + t.valueOf() * s * r),
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
	            return (0, v["default"])(t);
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
	            return o({}, p["default"].iterator, function () {
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
	            return e.hasOwnProperty("exclusive") && (a = e.exclusive), o({}, p["default"].iterator, function () {
	              return u === 1 / 0 ? {
	                done: !0
	              } : {
	                next: function next() {
	                  var e = (0, v["default"])(n.end.valueOf() - t.valueOf() * s * r),
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
	            return e.start.isSame((0, v["default"])(-864e13)) || (e.start = e.start.startOf(t)), e.end.isSame((0, v["default"])(864e13)) || (e.end = e.end.endOf(t)), e;
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
	        "for": a(function (t) {
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
	});
	var momentRange$1 = unwrapExports(momentRange);

	var moment$1 = momentRange$1.extendMoment(moment);
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
	  year: function year(range$$1) {
	    return Array.from(range$$1.snapTo('year').by('year')).map(function (d) {
	      return d.format('YYYY-01-01T00:00:00.000');
	    });
	  },
	  hour: function hour(range$$1) {
	    return Array.from(range$$1.by('hour')).map(function (d) {
	      return d.format('YYYY-MM-DDTHH:00:00.000');
	    });
	  },
	  minute: function minute(range$$1) {
	    return Array.from(range$$1.by('minute')).map(function (d) {
	      return d.format('YYYY-MM-DDTHH:mm:00.000');
	    });
	  },
	  second: function second(range$$1) {
	    return Array.from(range$$1.by('second')).map(function (d) {
	      return d.format('YYYY-MM-DDTHH:mm:ss.000');
	    });
	  },
	  week: function week(range$$1) {
	    return Array.from(range$$1.snapTo('isoweek').by('week')).map(function (d) {
	      return d.startOf('isoweek').format('YYYY-MM-DDT00:00:00.000');
	    });
	  }
	};
	var DateRegex = /^\d\d\d\d-\d\d-\d\d$/;
	var LocalDateRegex = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z?$/;

	var ResultSet =
	/*#__PURE__*/
	function () {
	  function ResultSet(loadResponse) {
	    var options = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};

	    _classCallCheck(this, ResultSet);

	    this.loadResponse = loadResponse;
	    this.parseDateMeasures = options.parseDateMeasures;
	    this.options = options;
	  }

	  _createClass(ResultSet, [{
	    key: "drillDown",
	    value: function drillDown(drillDownLocator, pivotConfig) {
	      var _drillDownLocator$xVa = drillDownLocator.xValues,
	          xValues = _drillDownLocator$xVa === void 0 ? [] : _drillDownLocator$xVa,
	          _drillDownLocator$yVa = drillDownLocator.yValues,
	          yValues = _drillDownLocator$yVa === void 0 ? [] : _drillDownLocator$yVa;
	      var normalizedPivotConfig = this.normalizePivotConfig(pivotConfig);
	      var values$$1 = [];
	      normalizedPivotConfig.x.forEach(function (member, currentIndex) {
	        return values$$1.push([member, xValues[currentIndex]]);
	      });
	      normalizedPivotConfig.y.forEach(function (member, currentIndex) {
	        return values$$1.push([member, yValues[currentIndex]]);
	      });
	      var measures = this.loadResponse.annotation.measures;

	      var _ref = values$$1.find(function (_ref3) {
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
	      values$$1.filter(function (_ref5) {
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
	          var range$$1 = moment$1.range(value, value).snapTo(granularity);
	          timeDimensions.push({
	            dimension: [cubeName, dimension].join('.'),
	            dateRange: [range$$1.start, range$$1.end].map(function (dt) {
	              return dt.format(moment$1.HTML5_FMT.DATETIME_LOCAL_MS);
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
	      return _objectSpread({}, measures[measureName].drillMembersGrouped, {
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
	          return row[ResultSet.timeDimensionMember(timeDimension)] && moment$1(row[ResultSet.timeDimensionMember(timeDimension)]);
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

	      var range$$1 = moment$1.range(start, end);

	      if (!TIME_SERIES[timeDimension.granularity]) {
	        throw new Error("Unsupported time granularity: ".concat(timeDimension.granularity));
	      }

	      return TIME_SERIES[timeDimension.granularity](padToDay ? range$$1.snapTo('day') : range$$1);
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
	              return moment$1(xValues[0]).format(moment$1.HTML5_FMT.DATETIME_LOCAL_MS);
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
	        return _objectSpread({
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
	          return _objectSpread({}, a, {}, b);
	        }, {});

	        var _ref30 = flatMeta[key] || {},
	            title = _ref30.title,
	            shortTitle = _ref30.shortTitle,
	            type$$1 = _ref30.type,
	            format = _ref30.format,
	            meta = _ref30.meta;

	        return {
	          key: key,
	          title: title,
	          shortTitle: shortTitle,
	          type: type$$1,
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
	            return _objectSpread({}, fields, {
	              key: key,
	              dataIndex: [].concat(_toConsumableArray(path), [key]).join('.'),
	              title: [title, dimensionValue].join(' ').trim(),
	              shortTitle: dimensionValue || shortTitle
	            });
	          }

	          return _objectSpread({}, fields, {
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
	          return _objectSpread({}, extractFields(key), {
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

	        return _objectSpread({}, extractFields(key), {
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
	          }) ? dropLast$1(1, axisValues).concat(_this5.loadResponse.annotation.measures[ResultSet.measureFromAxis(axisValues)].title) : axisValues, ', '),
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
	          return _objectSpread({}, row, {}, Object.keys(row).filter(function (field) {
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
	            return _objectSpread({}, a, {}, b);
	          }, {}));
	        });
	      }

	      return this.backwardCompatibleData;
	    }
	  }, {
	    key: "serialize",
	    value: function serialize() {
	      return {
	        loadResponse: clone(this.loadResponse)
	      };
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
	      pivotConfig = _objectSpread({}, pivotConfig, {
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
	  }, {
	    key: "deserialize",
	    value: function deserialize(data) {
	      var options = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};
	      return new ResultSet(data.loadResponse, options);
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
	      var member = memberTypes.map(function (type$$1) {
	        return _this.cubesMap[cube][type$$1] && _this.cubesMap[cube][type$$1][memberName];
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

	var ITERATOR$7 = wellKnownSymbol('iterator');

	var nativeUrl = !fails(function () {
	  var url = new URL('b?a=1&b=2&c=3', 'http://a');
	  var searchParams = url.searchParams;
	  var result = '';
	  url.pathname = 'c%20d';
	  searchParams.forEach(function (value, key) {
	    searchParams['delete']('b');
	    result += key + value;
	  });
	  return !searchParams.sort
	    || url.href !== 'http://a/c%20d?a=1&c=3'
	    || searchParams.get('c') !== '3'
	    || String(new URLSearchParams('?a=1')) !== 'a=1'
	    || !searchParams[ITERATOR$7]
	    // throws in Edge
	    || new URL('https://a@b').username !== 'a'
	    || new URLSearchParams(new URLSearchParams('a=b')).get('a') !== 'b'
	    // not punycoded in Edge
	    || new URL('http://тест').host !== 'xn--e1aybc'
	    // not escaped in Chrome 62-
	    || new URL('http://a#б').hash !== '#%D0%B1'
	    // fails in Chrome 66-
	    || result !== 'a1c3'
	    // throws in Safari
	    || new URL('http://x', undefined).host !== 'x';
	});

	// based on https://github.com/bestiejs/punycode.js/blob/master/punycode.js
	var maxInt = 2147483647; // aka. 0x7FFFFFFF or 2^31-1
	var base = 36;
	var tMin = 1;
	var tMax = 26;
	var skew = 38;
	var damp = 700;
	var initialBias = 72;
	var initialN = 128; // 0x80
	var delimiter = '-'; // '\x2D'
	var regexNonASCII = /[^\0-\u007E]/; // non-ASCII chars
	var regexSeparators = /[.\u3002\uFF0E\uFF61]/g; // RFC 3490 separators
	var OVERFLOW_ERROR = 'Overflow: input needs wider integers to process';
	var baseMinusTMin = base - tMin;
	var floor$6 = Math.floor;
	var stringFromCharCode = String.fromCharCode;

	/**
	 * Creates an array containing the numeric code points of each Unicode
	 * character in the string. While JavaScript uses UCS-2 internally,
	 * this function will convert a pair of surrogate halves (each of which
	 * UCS-2 exposes as separate characters) into a single code point,
	 * matching UTF-16.
	 */
	var ucs2decode = function (string) {
	  var output = [];
	  var counter = 0;
	  var length = string.length;
	  while (counter < length) {
	    var value = string.charCodeAt(counter++);
	    if (value >= 0xD800 && value <= 0xDBFF && counter < length) {
	      // It's a high surrogate, and there is a next character.
	      var extra = string.charCodeAt(counter++);
	      if ((extra & 0xFC00) == 0xDC00) { // Low surrogate.
	        output.push(((value & 0x3FF) << 10) + (extra & 0x3FF) + 0x10000);
	      } else {
	        // It's an unmatched surrogate; only append this code unit, in case the
	        // next code unit is the high surrogate of a surrogate pair.
	        output.push(value);
	        counter--;
	      }
	    } else {
	      output.push(value);
	    }
	  }
	  return output;
	};

	/**
	 * Converts a digit/integer into a basic code point.
	 */
	var digitToBasic = function (digit) {
	  //  0..25 map to ASCII a..z or A..Z
	  // 26..35 map to ASCII 0..9
	  return digit + 22 + 75 * (digit < 26);
	};

	/**
	 * Bias adaptation function as per section 3.4 of RFC 3492.
	 * https://tools.ietf.org/html/rfc3492#section-3.4
	 */
	var adapt = function (delta, numPoints, firstTime) {
	  var k = 0;
	  delta = firstTime ? floor$6(delta / damp) : delta >> 1;
	  delta += floor$6(delta / numPoints);
	  for (; delta > baseMinusTMin * tMax >> 1; k += base) {
	    delta = floor$6(delta / baseMinusTMin);
	  }
	  return floor$6(k + (baseMinusTMin + 1) * delta / (delta + skew));
	};

	/**
	 * Converts a string of Unicode symbols (e.g. a domain name label) to a
	 * Punycode string of ASCII-only symbols.
	 */
	// eslint-disable-next-line  max-statements
	var encode = function (input) {
	  var output = [];

	  // Convert the input in UCS-2 to an array of Unicode code points.
	  input = ucs2decode(input);

	  // Cache the length.
	  var inputLength = input.length;

	  // Initialize the state.
	  var n = initialN;
	  var delta = 0;
	  var bias = initialBias;
	  var i, currentValue;

	  // Handle the basic code points.
	  for (i = 0; i < input.length; i++) {
	    currentValue = input[i];
	    if (currentValue < 0x80) {
	      output.push(stringFromCharCode(currentValue));
	    }
	  }

	  var basicLength = output.length; // number of basic code points.
	  var handledCPCount = basicLength; // number of code points that have been handled;

	  // Finish the basic string with a delimiter unless it's empty.
	  if (basicLength) {
	    output.push(delimiter);
	  }

	  // Main encoding loop:
	  while (handledCPCount < inputLength) {
	    // All non-basic code points < n have been handled already. Find the next larger one:
	    var m = maxInt;
	    for (i = 0; i < input.length; i++) {
	      currentValue = input[i];
	      if (currentValue >= n && currentValue < m) {
	        m = currentValue;
	      }
	    }

	    // Increase `delta` enough to advance the decoder's <n,i> state to <m,0>, but guard against overflow.
	    var handledCPCountPlusOne = handledCPCount + 1;
	    if (m - n > floor$6((maxInt - delta) / handledCPCountPlusOne)) {
	      throw RangeError(OVERFLOW_ERROR);
	    }

	    delta += (m - n) * handledCPCountPlusOne;
	    n = m;

	    for (i = 0; i < input.length; i++) {
	      currentValue = input[i];
	      if (currentValue < n && ++delta > maxInt) {
	        throw RangeError(OVERFLOW_ERROR);
	      }
	      if (currentValue == n) {
	        // Represent delta as a generalized variable-length integer.
	        var q = delta;
	        for (var k = base; /* no condition */; k += base) {
	          var t = k <= bias ? tMin : (k >= bias + tMax ? tMax : k - bias);
	          if (q < t) break;
	          var qMinusT = q - t;
	          var baseMinusT = base - t;
	          output.push(stringFromCharCode(digitToBasic(t + qMinusT % baseMinusT)));
	          q = floor$6(qMinusT / baseMinusT);
	        }

	        output.push(stringFromCharCode(digitToBasic(q)));
	        bias = adapt(delta, handledCPCountPlusOne, handledCPCount == basicLength);
	        delta = 0;
	        ++handledCPCount;
	      }
	    }

	    ++delta;
	    ++n;
	  }
	  return output.join('');
	};

	var stringPunycodeToAscii = function (input) {
	  var encoded = [];
	  var labels = input.toLowerCase().replace(regexSeparators, '\u002E').split('.');
	  var i, label;
	  for (i = 0; i < labels.length; i++) {
	    label = labels[i];
	    encoded.push(regexNonASCII.test(label) ? 'xn--' + encode(label) : label);
	  }
	  return encoded.join('.');
	};

	var getIterator = function (it) {
	  var iteratorMethod = getIteratorMethod(it);
	  if (typeof iteratorMethod != 'function') {
	    throw TypeError(String(it) + ' is not iterable');
	  } return anObject(iteratorMethod.call(it));
	};

	// TODO: in core-js@4, move /modules/ dependencies to public entries for better optimization by tools like `preset-env`





















	var $fetch$1 = getBuiltIn('fetch');
	var Headers = getBuiltIn('Headers');
	var ITERATOR$8 = wellKnownSymbol('iterator');
	var URL_SEARCH_PARAMS = 'URLSearchParams';
	var URL_SEARCH_PARAMS_ITERATOR = URL_SEARCH_PARAMS + 'Iterator';
	var setInternalState$7 = internalState.set;
	var getInternalParamsState = internalState.getterFor(URL_SEARCH_PARAMS);
	var getInternalIteratorState = internalState.getterFor(URL_SEARCH_PARAMS_ITERATOR);

	var plus = /\+/g;
	var sequences = Array(4);

	var percentSequence = function (bytes) {
	  return sequences[bytes - 1] || (sequences[bytes - 1] = RegExp('((?:%[\\da-f]{2}){' + bytes + '})', 'gi'));
	};

	var percentDecode = function (sequence) {
	  try {
	    return decodeURIComponent(sequence);
	  } catch (error) {
	    return sequence;
	  }
	};

	var deserialize = function (it) {
	  var result = it.replace(plus, ' ');
	  var bytes = 4;
	  try {
	    return decodeURIComponent(result);
	  } catch (error) {
	    while (bytes) {
	      result = result.replace(percentSequence(bytes--), percentDecode);
	    }
	    return result;
	  }
	};

	var find$1 = /[!'()~]|%20/g;

	var replace$1 = {
	  '!': '%21',
	  "'": '%27',
	  '(': '%28',
	  ')': '%29',
	  '~': '%7E',
	  '%20': '+'
	};

	var replacer = function (match) {
	  return replace$1[match];
	};

	var serialize = function (it) {
	  return encodeURIComponent(it).replace(find$1, replacer);
	};

	var parseSearchParams = function (result, query) {
	  if (query) {
	    var attributes = query.split('&');
	    var index = 0;
	    var attribute, entry;
	    while (index < attributes.length) {
	      attribute = attributes[index++];
	      if (attribute.length) {
	        entry = attribute.split('=');
	        result.push({
	          key: deserialize(entry.shift()),
	          value: deserialize(entry.join('='))
	        });
	      }
	    }
	  }
	};

	var updateSearchParams = function (query) {
	  this.entries.length = 0;
	  parseSearchParams(this.entries, query);
	};

	var validateArgumentsLength = function (passed, required) {
	  if (passed < required) throw TypeError('Not enough arguments');
	};

	var URLSearchParamsIterator = createIteratorConstructor(function Iterator(params, kind) {
	  setInternalState$7(this, {
	    type: URL_SEARCH_PARAMS_ITERATOR,
	    iterator: getIterator(getInternalParamsState(params).entries),
	    kind: kind
	  });
	}, 'Iterator', function next() {
	  var state = getInternalIteratorState(this);
	  var kind = state.kind;
	  var step = state.iterator.next();
	  var entry = step.value;
	  if (!step.done) {
	    step.value = kind === 'keys' ? entry.key : kind === 'values' ? entry.value : [entry.key, entry.value];
	  } return step;
	});

	// `URLSearchParams` constructor
	// https://url.spec.whatwg.org/#interface-urlsearchparams
	var URLSearchParamsConstructor = function URLSearchParams(/* init */) {
	  anInstance(this, URLSearchParamsConstructor, URL_SEARCH_PARAMS);
	  var init = arguments.length > 0 ? arguments[0] : undefined;
	  var that = this;
	  var entries = [];
	  var iteratorMethod, iterator, next, step, entryIterator, entryNext, first, second, key;

	  setInternalState$7(that, {
	    type: URL_SEARCH_PARAMS,
	    entries: entries,
	    updateURL: function () { /* empty */ },
	    updateSearchParams: updateSearchParams
	  });

	  if (init !== undefined) {
	    if (isObject(init)) {
	      iteratorMethod = getIteratorMethod(init);
	      if (typeof iteratorMethod === 'function') {
	        iterator = iteratorMethod.call(init);
	        next = iterator.next;
	        while (!(step = next.call(iterator)).done) {
	          entryIterator = getIterator(anObject(step.value));
	          entryNext = entryIterator.next;
	          if (
	            (first = entryNext.call(entryIterator)).done ||
	            (second = entryNext.call(entryIterator)).done ||
	            !entryNext.call(entryIterator).done
	          ) throw TypeError('Expected sequence with length 2');
	          entries.push({ key: first.value + '', value: second.value + '' });
	        }
	      } else for (key in init) if (has(init, key)) entries.push({ key: key, value: init[key] + '' });
	    } else {
	      parseSearchParams(entries, typeof init === 'string' ? init.charAt(0) === '?' ? init.slice(1) : init : init + '');
	    }
	  }
	};

	var URLSearchParamsPrototype = URLSearchParamsConstructor.prototype;

	redefineAll(URLSearchParamsPrototype, {
	  // `URLSearchParams.prototype.appent` method
	  // https://url.spec.whatwg.org/#dom-urlsearchparams-append
	  append: function append(name, value) {
	    validateArgumentsLength(arguments.length, 2);
	    var state = getInternalParamsState(this);
	    state.entries.push({ key: name + '', value: value + '' });
	    state.updateURL();
	  },
	  // `URLSearchParams.prototype.delete` method
	  // https://url.spec.whatwg.org/#dom-urlsearchparams-delete
	  'delete': function (name) {
	    validateArgumentsLength(arguments.length, 1);
	    var state = getInternalParamsState(this);
	    var entries = state.entries;
	    var key = name + '';
	    var index = 0;
	    while (index < entries.length) {
	      if (entries[index].key === key) entries.splice(index, 1);
	      else index++;
	    }
	    state.updateURL();
	  },
	  // `URLSearchParams.prototype.get` method
	  // https://url.spec.whatwg.org/#dom-urlsearchparams-get
	  get: function get(name) {
	    validateArgumentsLength(arguments.length, 1);
	    var entries = getInternalParamsState(this).entries;
	    var key = name + '';
	    var index = 0;
	    for (; index < entries.length; index++) {
	      if (entries[index].key === key) return entries[index].value;
	    }
	    return null;
	  },
	  // `URLSearchParams.prototype.getAll` method
	  // https://url.spec.whatwg.org/#dom-urlsearchparams-getall
	  getAll: function getAll(name) {
	    validateArgumentsLength(arguments.length, 1);
	    var entries = getInternalParamsState(this).entries;
	    var key = name + '';
	    var result = [];
	    var index = 0;
	    for (; index < entries.length; index++) {
	      if (entries[index].key === key) result.push(entries[index].value);
	    }
	    return result;
	  },
	  // `URLSearchParams.prototype.has` method
	  // https://url.spec.whatwg.org/#dom-urlsearchparams-has
	  has: function has$$1(name) {
	    validateArgumentsLength(arguments.length, 1);
	    var entries = getInternalParamsState(this).entries;
	    var key = name + '';
	    var index = 0;
	    while (index < entries.length) {
	      if (entries[index++].key === key) return true;
	    }
	    return false;
	  },
	  // `URLSearchParams.prototype.set` method
	  // https://url.spec.whatwg.org/#dom-urlsearchparams-set
	  set: function set(name, value) {
	    validateArgumentsLength(arguments.length, 1);
	    var state = getInternalParamsState(this);
	    var entries = state.entries;
	    var found = false;
	    var key = name + '';
	    var val = value + '';
	    var index = 0;
	    var entry;
	    for (; index < entries.length; index++) {
	      entry = entries[index];
	      if (entry.key === key) {
	        if (found) entries.splice(index--, 1);
	        else {
	          found = true;
	          entry.value = val;
	        }
	      }
	    }
	    if (!found) entries.push({ key: key, value: val });
	    state.updateURL();
	  },
	  // `URLSearchParams.prototype.sort` method
	  // https://url.spec.whatwg.org/#dom-urlsearchparams-sort
	  sort: function sort() {
	    var state = getInternalParamsState(this);
	    var entries = state.entries;
	    // Array#sort is not stable in some engines
	    var slice = entries.slice();
	    var entry, entriesIndex, sliceIndex;
	    entries.length = 0;
	    for (sliceIndex = 0; sliceIndex < slice.length; sliceIndex++) {
	      entry = slice[sliceIndex];
	      for (entriesIndex = 0; entriesIndex < sliceIndex; entriesIndex++) {
	        if (entries[entriesIndex].key > entry.key) {
	          entries.splice(entriesIndex, 0, entry);
	          break;
	        }
	      }
	      if (entriesIndex === sliceIndex) entries.push(entry);
	    }
	    state.updateURL();
	  },
	  // `URLSearchParams.prototype.forEach` method
	  forEach: function forEach(callback /* , thisArg */) {
	    var entries = getInternalParamsState(this).entries;
	    var boundFunction = functionBindContext(callback, arguments.length > 1 ? arguments[1] : undefined, 3);
	    var index = 0;
	    var entry;
	    while (index < entries.length) {
	      entry = entries[index++];
	      boundFunction(entry.value, entry.key, this);
	    }
	  },
	  // `URLSearchParams.prototype.keys` method
	  keys: function keys() {
	    return new URLSearchParamsIterator(this, 'keys');
	  },
	  // `URLSearchParams.prototype.values` method
	  values: function values() {
	    return new URLSearchParamsIterator(this, 'values');
	  },
	  // `URLSearchParams.prototype.entries` method
	  entries: function entries() {
	    return new URLSearchParamsIterator(this, 'entries');
	  }
	}, { enumerable: true });

	// `URLSearchParams.prototype[@@iterator]` method
	redefine(URLSearchParamsPrototype, ITERATOR$8, URLSearchParamsPrototype.entries);

	// `URLSearchParams.prototype.toString` method
	// https://url.spec.whatwg.org/#urlsearchparams-stringification-behavior
	redefine(URLSearchParamsPrototype, 'toString', function toString() {
	  var entries = getInternalParamsState(this).entries;
	  var result = [];
	  var index = 0;
	  var entry;
	  while (index < entries.length) {
	    entry = entries[index++];
	    result.push(serialize(entry.key) + '=' + serialize(entry.value));
	  } return result.join('&');
	}, { enumerable: true });

	setToStringTag(URLSearchParamsConstructor, URL_SEARCH_PARAMS);

	_export({ global: true, forced: !nativeUrl }, {
	  URLSearchParams: URLSearchParamsConstructor
	});

	// Wrap `fetch` for correct work with polyfilled `URLSearchParams`
	// https://github.com/zloirock/core-js/issues/674
	if (!nativeUrl && typeof $fetch$1 == 'function' && typeof Headers == 'function') {
	  _export({ global: true, enumerable: true, forced: true }, {
	    fetch: function fetch(input /* , init */) {
	      var args = [input];
	      var init, body, headers;
	      if (arguments.length > 1) {
	        init = arguments[1];
	        if (isObject(init)) {
	          body = init.body;
	          if (classof(body) === URL_SEARCH_PARAMS) {
	            headers = init.headers ? new Headers(init.headers) : new Headers();
	            if (!headers.has('content-type')) {
	              headers.set('content-type', 'application/x-www-form-urlencoded;charset=UTF-8');
	            }
	            init = objectCreate(init, {
	              body: createPropertyDescriptor(0, String(body)),
	              headers: createPropertyDescriptor(0, headers)
	            });
	          }
	        }
	        args.push(init);
	      } return $fetch$1.apply(this, args);
	    }
	  });
	}

	var web_urlSearchParams = {
	  URLSearchParams: URLSearchParamsConstructor,
	  getState: getInternalParamsState
	};

	// TODO: in core-js@4, move /modules/ dependencies to public entries for better optimization by tools like `preset-env`











	var codeAt = stringMultibyte.codeAt;





	var NativeURL = global_1.URL;
	var URLSearchParams$1 = web_urlSearchParams.URLSearchParams;
	var getInternalSearchParamsState = web_urlSearchParams.getState;
	var setInternalState$8 = internalState.set;
	var getInternalURLState = internalState.getterFor('URL');
	var floor$7 = Math.floor;
	var pow$2 = Math.pow;

	var INVALID_AUTHORITY = 'Invalid authority';
	var INVALID_SCHEME = 'Invalid scheme';
	var INVALID_HOST = 'Invalid host';
	var INVALID_PORT = 'Invalid port';

	var ALPHA = /[A-Za-z]/;
	var ALPHANUMERIC = /[\d+-.A-Za-z]/;
	var DIGIT = /\d/;
	var HEX_START = /^(0x|0X)/;
	var OCT = /^[0-7]+$/;
	var DEC = /^\d+$/;
	var HEX = /^[\dA-Fa-f]+$/;
	// eslint-disable-next-line no-control-regex
	var FORBIDDEN_HOST_CODE_POINT = /[\u0000\u0009\u000A\u000D #%/:?@[\\]]/;
	// eslint-disable-next-line no-control-regex
	var FORBIDDEN_HOST_CODE_POINT_EXCLUDING_PERCENT = /[\u0000\u0009\u000A\u000D #/:?@[\\]]/;
	// eslint-disable-next-line no-control-regex
	var LEADING_AND_TRAILING_C0_CONTROL_OR_SPACE = /^[\u0000-\u001F ]+|[\u0000-\u001F ]+$/g;
	// eslint-disable-next-line no-control-regex
	var TAB_AND_NEW_LINE = /[\u0009\u000A\u000D]/g;
	var EOF;

	var parseHost = function (url, input) {
	  var result, codePoints, index;
	  if (input.charAt(0) == '[') {
	    if (input.charAt(input.length - 1) != ']') return INVALID_HOST;
	    result = parseIPv6(input.slice(1, -1));
	    if (!result) return INVALID_HOST;
	    url.host = result;
	  // opaque host
	  } else if (!isSpecial(url)) {
	    if (FORBIDDEN_HOST_CODE_POINT_EXCLUDING_PERCENT.test(input)) return INVALID_HOST;
	    result = '';
	    codePoints = arrayFrom(input);
	    for (index = 0; index < codePoints.length; index++) {
	      result += percentEncode(codePoints[index], C0ControlPercentEncodeSet);
	    }
	    url.host = result;
	  } else {
	    input = stringPunycodeToAscii(input);
	    if (FORBIDDEN_HOST_CODE_POINT.test(input)) return INVALID_HOST;
	    result = parseIPv4(input);
	    if (result === null) return INVALID_HOST;
	    url.host = result;
	  }
	};

	var parseIPv4 = function (input) {
	  var parts = input.split('.');
	  var partsLength, numbers, index, part, radix, number, ipv4;
	  if (parts.length && parts[parts.length - 1] == '') {
	    parts.pop();
	  }
	  partsLength = parts.length;
	  if (partsLength > 4) return input;
	  numbers = [];
	  for (index = 0; index < partsLength; index++) {
	    part = parts[index];
	    if (part == '') return input;
	    radix = 10;
	    if (part.length > 1 && part.charAt(0) == '0') {
	      radix = HEX_START.test(part) ? 16 : 8;
	      part = part.slice(radix == 8 ? 1 : 2);
	    }
	    if (part === '') {
	      number = 0;
	    } else {
	      if (!(radix == 10 ? DEC : radix == 8 ? OCT : HEX).test(part)) return input;
	      number = parseInt(part, radix);
	    }
	    numbers.push(number);
	  }
	  for (index = 0; index < partsLength; index++) {
	    number = numbers[index];
	    if (index == partsLength - 1) {
	      if (number >= pow$2(256, 5 - partsLength)) return null;
	    } else if (number > 255) return null;
	  }
	  ipv4 = numbers.pop();
	  for (index = 0; index < numbers.length; index++) {
	    ipv4 += numbers[index] * pow$2(256, 3 - index);
	  }
	  return ipv4;
	};

	// eslint-disable-next-line max-statements
	var parseIPv6 = function (input) {
	  var address = [0, 0, 0, 0, 0, 0, 0, 0];
	  var pieceIndex = 0;
	  var compress = null;
	  var pointer = 0;
	  var value, length, numbersSeen, ipv4Piece, number, swaps, swap;

	  var char = function () {
	    return input.charAt(pointer);
	  };

	  if (char() == ':') {
	    if (input.charAt(1) != ':') return;
	    pointer += 2;
	    pieceIndex++;
	    compress = pieceIndex;
	  }
	  while (char()) {
	    if (pieceIndex == 8) return;
	    if (char() == ':') {
	      if (compress !== null) return;
	      pointer++;
	      pieceIndex++;
	      compress = pieceIndex;
	      continue;
	    }
	    value = length = 0;
	    while (length < 4 && HEX.test(char())) {
	      value = value * 16 + parseInt(char(), 16);
	      pointer++;
	      length++;
	    }
	    if (char() == '.') {
	      if (length == 0) return;
	      pointer -= length;
	      if (pieceIndex > 6) return;
	      numbersSeen = 0;
	      while (char()) {
	        ipv4Piece = null;
	        if (numbersSeen > 0) {
	          if (char() == '.' && numbersSeen < 4) pointer++;
	          else return;
	        }
	        if (!DIGIT.test(char())) return;
	        while (DIGIT.test(char())) {
	          number = parseInt(char(), 10);
	          if (ipv4Piece === null) ipv4Piece = number;
	          else if (ipv4Piece == 0) return;
	          else ipv4Piece = ipv4Piece * 10 + number;
	          if (ipv4Piece > 255) return;
	          pointer++;
	        }
	        address[pieceIndex] = address[pieceIndex] * 256 + ipv4Piece;
	        numbersSeen++;
	        if (numbersSeen == 2 || numbersSeen == 4) pieceIndex++;
	      }
	      if (numbersSeen != 4) return;
	      break;
	    } else if (char() == ':') {
	      pointer++;
	      if (!char()) return;
	    } else if (char()) return;
	    address[pieceIndex++] = value;
	  }
	  if (compress !== null) {
	    swaps = pieceIndex - compress;
	    pieceIndex = 7;
	    while (pieceIndex != 0 && swaps > 0) {
	      swap = address[pieceIndex];
	      address[pieceIndex--] = address[compress + swaps - 1];
	      address[compress + --swaps] = swap;
	    }
	  } else if (pieceIndex != 8) return;
	  return address;
	};

	var findLongestZeroSequence = function (ipv6) {
	  var maxIndex = null;
	  var maxLength = 1;
	  var currStart = null;
	  var currLength = 0;
	  var index = 0;
	  for (; index < 8; index++) {
	    if (ipv6[index] !== 0) {
	      if (currLength > maxLength) {
	        maxIndex = currStart;
	        maxLength = currLength;
	      }
	      currStart = null;
	      currLength = 0;
	    } else {
	      if (currStart === null) currStart = index;
	      ++currLength;
	    }
	  }
	  if (currLength > maxLength) {
	    maxIndex = currStart;
	    maxLength = currLength;
	  }
	  return maxIndex;
	};

	var serializeHost = function (host) {
	  var result, index, compress, ignore0;
	  // ipv4
	  if (typeof host == 'number') {
	    result = [];
	    for (index = 0; index < 4; index++) {
	      result.unshift(host % 256);
	      host = floor$7(host / 256);
	    } return result.join('.');
	  // ipv6
	  } else if (typeof host == 'object') {
	    result = '';
	    compress = findLongestZeroSequence(host);
	    for (index = 0; index < 8; index++) {
	      if (ignore0 && host[index] === 0) continue;
	      if (ignore0) ignore0 = false;
	      if (compress === index) {
	        result += index ? ':' : '::';
	        ignore0 = true;
	      } else {
	        result += host[index].toString(16);
	        if (index < 7) result += ':';
	      }
	    }
	    return '[' + result + ']';
	  } return host;
	};

	var C0ControlPercentEncodeSet = {};
	var fragmentPercentEncodeSet = objectAssign({}, C0ControlPercentEncodeSet, {
	  ' ': 1, '"': 1, '<': 1, '>': 1, '`': 1
	});
	var pathPercentEncodeSet = objectAssign({}, fragmentPercentEncodeSet, {
	  '#': 1, '?': 1, '{': 1, '}': 1
	});
	var userinfoPercentEncodeSet = objectAssign({}, pathPercentEncodeSet, {
	  '/': 1, ':': 1, ';': 1, '=': 1, '@': 1, '[': 1, '\\': 1, ']': 1, '^': 1, '|': 1
	});

	var percentEncode = function (char, set) {
	  var code = codeAt(char, 0);
	  return code > 0x20 && code < 0x7F && !has(set, char) ? char : encodeURIComponent(char);
	};

	var specialSchemes = {
	  ftp: 21,
	  file: null,
	  http: 80,
	  https: 443,
	  ws: 80,
	  wss: 443
	};

	var isSpecial = function (url) {
	  return has(specialSchemes, url.scheme);
	};

	var includesCredentials = function (url) {
	  return url.username != '' || url.password != '';
	};

	var cannotHaveUsernamePasswordPort = function (url) {
	  return !url.host || url.cannotBeABaseURL || url.scheme == 'file';
	};

	var isWindowsDriveLetter = function (string, normalized) {
	  var second;
	  return string.length == 2 && ALPHA.test(string.charAt(0))
	    && ((second = string.charAt(1)) == ':' || (!normalized && second == '|'));
	};

	var startsWithWindowsDriveLetter = function (string) {
	  var third;
	  return string.length > 1 && isWindowsDriveLetter(string.slice(0, 2)) && (
	    string.length == 2 ||
	    ((third = string.charAt(2)) === '/' || third === '\\' || third === '?' || third === '#')
	  );
	};

	var shortenURLsPath = function (url) {
	  var path = url.path;
	  var pathSize = path.length;
	  if (pathSize && (url.scheme != 'file' || pathSize != 1 || !isWindowsDriveLetter(path[0], true))) {
	    path.pop();
	  }
	};

	var isSingleDot = function (segment) {
	  return segment === '.' || segment.toLowerCase() === '%2e';
	};

	var isDoubleDot = function (segment) {
	  segment = segment.toLowerCase();
	  return segment === '..' || segment === '%2e.' || segment === '.%2e' || segment === '%2e%2e';
	};

	// States:
	var SCHEME_START = {};
	var SCHEME = {};
	var NO_SCHEME = {};
	var SPECIAL_RELATIVE_OR_AUTHORITY = {};
	var PATH_OR_AUTHORITY = {};
	var RELATIVE = {};
	var RELATIVE_SLASH = {};
	var SPECIAL_AUTHORITY_SLASHES = {};
	var SPECIAL_AUTHORITY_IGNORE_SLASHES = {};
	var AUTHORITY = {};
	var HOST = {};
	var HOSTNAME = {};
	var PORT = {};
	var FILE = {};
	var FILE_SLASH = {};
	var FILE_HOST = {};
	var PATH_START = {};
	var PATH = {};
	var CANNOT_BE_A_BASE_URL_PATH = {};
	var QUERY = {};
	var FRAGMENT = {};

	// eslint-disable-next-line max-statements
	var parseURL = function (url, input, stateOverride, base) {
	  var state = stateOverride || SCHEME_START;
	  var pointer = 0;
	  var buffer = '';
	  var seenAt = false;
	  var seenBracket = false;
	  var seenPasswordToken = false;
	  var codePoints, char, bufferCodePoints, failure;

	  if (!stateOverride) {
	    url.scheme = '';
	    url.username = '';
	    url.password = '';
	    url.host = null;
	    url.port = null;
	    url.path = [];
	    url.query = null;
	    url.fragment = null;
	    url.cannotBeABaseURL = false;
	    input = input.replace(LEADING_AND_TRAILING_C0_CONTROL_OR_SPACE, '');
	  }

	  input = input.replace(TAB_AND_NEW_LINE, '');

	  codePoints = arrayFrom(input);

	  while (pointer <= codePoints.length) {
	    char = codePoints[pointer];
	    switch (state) {
	      case SCHEME_START:
	        if (char && ALPHA.test(char)) {
	          buffer += char.toLowerCase();
	          state = SCHEME;
	        } else if (!stateOverride) {
	          state = NO_SCHEME;
	          continue;
	        } else return INVALID_SCHEME;
	        break;

	      case SCHEME:
	        if (char && (ALPHANUMERIC.test(char) || char == '+' || char == '-' || char == '.')) {
	          buffer += char.toLowerCase();
	        } else if (char == ':') {
	          if (stateOverride && (
	            (isSpecial(url) != has(specialSchemes, buffer)) ||
	            (buffer == 'file' && (includesCredentials(url) || url.port !== null)) ||
	            (url.scheme == 'file' && !url.host)
	          )) return;
	          url.scheme = buffer;
	          if (stateOverride) {
	            if (isSpecial(url) && specialSchemes[url.scheme] == url.port) url.port = null;
	            return;
	          }
	          buffer = '';
	          if (url.scheme == 'file') {
	            state = FILE;
	          } else if (isSpecial(url) && base && base.scheme == url.scheme) {
	            state = SPECIAL_RELATIVE_OR_AUTHORITY;
	          } else if (isSpecial(url)) {
	            state = SPECIAL_AUTHORITY_SLASHES;
	          } else if (codePoints[pointer + 1] == '/') {
	            state = PATH_OR_AUTHORITY;
	            pointer++;
	          } else {
	            url.cannotBeABaseURL = true;
	            url.path.push('');
	            state = CANNOT_BE_A_BASE_URL_PATH;
	          }
	        } else if (!stateOverride) {
	          buffer = '';
	          state = NO_SCHEME;
	          pointer = 0;
	          continue;
	        } else return INVALID_SCHEME;
	        break;

	      case NO_SCHEME:
	        if (!base || (base.cannotBeABaseURL && char != '#')) return INVALID_SCHEME;
	        if (base.cannotBeABaseURL && char == '#') {
	          url.scheme = base.scheme;
	          url.path = base.path.slice();
	          url.query = base.query;
	          url.fragment = '';
	          url.cannotBeABaseURL = true;
	          state = FRAGMENT;
	          break;
	        }
	        state = base.scheme == 'file' ? FILE : RELATIVE;
	        continue;

	      case SPECIAL_RELATIVE_OR_AUTHORITY:
	        if (char == '/' && codePoints[pointer + 1] == '/') {
	          state = SPECIAL_AUTHORITY_IGNORE_SLASHES;
	          pointer++;
	        } else {
	          state = RELATIVE;
	          continue;
	        } break;

	      case PATH_OR_AUTHORITY:
	        if (char == '/') {
	          state = AUTHORITY;
	          break;
	        } else {
	          state = PATH;
	          continue;
	        }

	      case RELATIVE:
	        url.scheme = base.scheme;
	        if (char == EOF) {
	          url.username = base.username;
	          url.password = base.password;
	          url.host = base.host;
	          url.port = base.port;
	          url.path = base.path.slice();
	          url.query = base.query;
	        } else if (char == '/' || (char == '\\' && isSpecial(url))) {
	          state = RELATIVE_SLASH;
	        } else if (char == '?') {
	          url.username = base.username;
	          url.password = base.password;
	          url.host = base.host;
	          url.port = base.port;
	          url.path = base.path.slice();
	          url.query = '';
	          state = QUERY;
	        } else if (char == '#') {
	          url.username = base.username;
	          url.password = base.password;
	          url.host = base.host;
	          url.port = base.port;
	          url.path = base.path.slice();
	          url.query = base.query;
	          url.fragment = '';
	          state = FRAGMENT;
	        } else {
	          url.username = base.username;
	          url.password = base.password;
	          url.host = base.host;
	          url.port = base.port;
	          url.path = base.path.slice();
	          url.path.pop();
	          state = PATH;
	          continue;
	        } break;

	      case RELATIVE_SLASH:
	        if (isSpecial(url) && (char == '/' || char == '\\')) {
	          state = SPECIAL_AUTHORITY_IGNORE_SLASHES;
	        } else if (char == '/') {
	          state = AUTHORITY;
	        } else {
	          url.username = base.username;
	          url.password = base.password;
	          url.host = base.host;
	          url.port = base.port;
	          state = PATH;
	          continue;
	        } break;

	      case SPECIAL_AUTHORITY_SLASHES:
	        state = SPECIAL_AUTHORITY_IGNORE_SLASHES;
	        if (char != '/' || buffer.charAt(pointer + 1) != '/') continue;
	        pointer++;
	        break;

	      case SPECIAL_AUTHORITY_IGNORE_SLASHES:
	        if (char != '/' && char != '\\') {
	          state = AUTHORITY;
	          continue;
	        } break;

	      case AUTHORITY:
	        if (char == '@') {
	          if (seenAt) buffer = '%40' + buffer;
	          seenAt = true;
	          bufferCodePoints = arrayFrom(buffer);
	          for (var i = 0; i < bufferCodePoints.length; i++) {
	            var codePoint = bufferCodePoints[i];
	            if (codePoint == ':' && !seenPasswordToken) {
	              seenPasswordToken = true;
	              continue;
	            }
	            var encodedCodePoints = percentEncode(codePoint, userinfoPercentEncodeSet);
	            if (seenPasswordToken) url.password += encodedCodePoints;
	            else url.username += encodedCodePoints;
	          }
	          buffer = '';
	        } else if (
	          char == EOF || char == '/' || char == '?' || char == '#' ||
	          (char == '\\' && isSpecial(url))
	        ) {
	          if (seenAt && buffer == '') return INVALID_AUTHORITY;
	          pointer -= arrayFrom(buffer).length + 1;
	          buffer = '';
	          state = HOST;
	        } else buffer += char;
	        break;

	      case HOST:
	      case HOSTNAME:
	        if (stateOverride && url.scheme == 'file') {
	          state = FILE_HOST;
	          continue;
	        } else if (char == ':' && !seenBracket) {
	          if (buffer == '') return INVALID_HOST;
	          failure = parseHost(url, buffer);
	          if (failure) return failure;
	          buffer = '';
	          state = PORT;
	          if (stateOverride == HOSTNAME) return;
	        } else if (
	          char == EOF || char == '/' || char == '?' || char == '#' ||
	          (char == '\\' && isSpecial(url))
	        ) {
	          if (isSpecial(url) && buffer == '') return INVALID_HOST;
	          if (stateOverride && buffer == '' && (includesCredentials(url) || url.port !== null)) return;
	          failure = parseHost(url, buffer);
	          if (failure) return failure;
	          buffer = '';
	          state = PATH_START;
	          if (stateOverride) return;
	          continue;
	        } else {
	          if (char == '[') seenBracket = true;
	          else if (char == ']') seenBracket = false;
	          buffer += char;
	        } break;

	      case PORT:
	        if (DIGIT.test(char)) {
	          buffer += char;
	        } else if (
	          char == EOF || char == '/' || char == '?' || char == '#' ||
	          (char == '\\' && isSpecial(url)) ||
	          stateOverride
	        ) {
	          if (buffer != '') {
	            var port = parseInt(buffer, 10);
	            if (port > 0xFFFF) return INVALID_PORT;
	            url.port = (isSpecial(url) && port === specialSchemes[url.scheme]) ? null : port;
	            buffer = '';
	          }
	          if (stateOverride) return;
	          state = PATH_START;
	          continue;
	        } else return INVALID_PORT;
	        break;

	      case FILE:
	        url.scheme = 'file';
	        if (char == '/' || char == '\\') state = FILE_SLASH;
	        else if (base && base.scheme == 'file') {
	          if (char == EOF) {
	            url.host = base.host;
	            url.path = base.path.slice();
	            url.query = base.query;
	          } else if (char == '?') {
	            url.host = base.host;
	            url.path = base.path.slice();
	            url.query = '';
	            state = QUERY;
	          } else if (char == '#') {
	            url.host = base.host;
	            url.path = base.path.slice();
	            url.query = base.query;
	            url.fragment = '';
	            state = FRAGMENT;
	          } else {
	            if (!startsWithWindowsDriveLetter(codePoints.slice(pointer).join(''))) {
	              url.host = base.host;
	              url.path = base.path.slice();
	              shortenURLsPath(url);
	            }
	            state = PATH;
	            continue;
	          }
	        } else {
	          state = PATH;
	          continue;
	        } break;

	      case FILE_SLASH:
	        if (char == '/' || char == '\\') {
	          state = FILE_HOST;
	          break;
	        }
	        if (base && base.scheme == 'file' && !startsWithWindowsDriveLetter(codePoints.slice(pointer).join(''))) {
	          if (isWindowsDriveLetter(base.path[0], true)) url.path.push(base.path[0]);
	          else url.host = base.host;
	        }
	        state = PATH;
	        continue;

	      case FILE_HOST:
	        if (char == EOF || char == '/' || char == '\\' || char == '?' || char == '#') {
	          if (!stateOverride && isWindowsDriveLetter(buffer)) {
	            state = PATH;
	          } else if (buffer == '') {
	            url.host = '';
	            if (stateOverride) return;
	            state = PATH_START;
	          } else {
	            failure = parseHost(url, buffer);
	            if (failure) return failure;
	            if (url.host == 'localhost') url.host = '';
	            if (stateOverride) return;
	            buffer = '';
	            state = PATH_START;
	          } continue;
	        } else buffer += char;
	        break;

	      case PATH_START:
	        if (isSpecial(url)) {
	          state = PATH;
	          if (char != '/' && char != '\\') continue;
	        } else if (!stateOverride && char == '?') {
	          url.query = '';
	          state = QUERY;
	        } else if (!stateOverride && char == '#') {
	          url.fragment = '';
	          state = FRAGMENT;
	        } else if (char != EOF) {
	          state = PATH;
	          if (char != '/') continue;
	        } break;

	      case PATH:
	        if (
	          char == EOF || char == '/' ||
	          (char == '\\' && isSpecial(url)) ||
	          (!stateOverride && (char == '?' || char == '#'))
	        ) {
	          if (isDoubleDot(buffer)) {
	            shortenURLsPath(url);
	            if (char != '/' && !(char == '\\' && isSpecial(url))) {
	              url.path.push('');
	            }
	          } else if (isSingleDot(buffer)) {
	            if (char != '/' && !(char == '\\' && isSpecial(url))) {
	              url.path.push('');
	            }
	          } else {
	            if (url.scheme == 'file' && !url.path.length && isWindowsDriveLetter(buffer)) {
	              if (url.host) url.host = '';
	              buffer = buffer.charAt(0) + ':'; // normalize windows drive letter
	            }
	            url.path.push(buffer);
	          }
	          buffer = '';
	          if (url.scheme == 'file' && (char == EOF || char == '?' || char == '#')) {
	            while (url.path.length > 1 && url.path[0] === '') {
	              url.path.shift();
	            }
	          }
	          if (char == '?') {
	            url.query = '';
	            state = QUERY;
	          } else if (char == '#') {
	            url.fragment = '';
	            state = FRAGMENT;
	          }
	        } else {
	          buffer += percentEncode(char, pathPercentEncodeSet);
	        } break;

	      case CANNOT_BE_A_BASE_URL_PATH:
	        if (char == '?') {
	          url.query = '';
	          state = QUERY;
	        } else if (char == '#') {
	          url.fragment = '';
	          state = FRAGMENT;
	        } else if (char != EOF) {
	          url.path[0] += percentEncode(char, C0ControlPercentEncodeSet);
	        } break;

	      case QUERY:
	        if (!stateOverride && char == '#') {
	          url.fragment = '';
	          state = FRAGMENT;
	        } else if (char != EOF) {
	          if (char == "'" && isSpecial(url)) url.query += '%27';
	          else if (char == '#') url.query += '%23';
	          else url.query += percentEncode(char, C0ControlPercentEncodeSet);
	        } break;

	      case FRAGMENT:
	        if (char != EOF) url.fragment += percentEncode(char, fragmentPercentEncodeSet);
	        break;
	    }

	    pointer++;
	  }
	};

	// `URL` constructor
	// https://url.spec.whatwg.org/#url-class
	var URLConstructor = function URL(url /* , base */) {
	  var that = anInstance(this, URLConstructor, 'URL');
	  var base = arguments.length > 1 ? arguments[1] : undefined;
	  var urlString = String(url);
	  var state = setInternalState$8(that, { type: 'URL' });
	  var baseState, failure;
	  if (base !== undefined) {
	    if (base instanceof URLConstructor) baseState = getInternalURLState(base);
	    else {
	      failure = parseURL(baseState = {}, String(base));
	      if (failure) throw TypeError(failure);
	    }
	  }
	  failure = parseURL(state, urlString, null, baseState);
	  if (failure) throw TypeError(failure);
	  var searchParams = state.searchParams = new URLSearchParams$1();
	  var searchParamsState = getInternalSearchParamsState(searchParams);
	  searchParamsState.updateSearchParams(state.query);
	  searchParamsState.updateURL = function () {
	    state.query = String(searchParams) || null;
	  };
	  if (!descriptors) {
	    that.href = serializeURL.call(that);
	    that.origin = getOrigin.call(that);
	    that.protocol = getProtocol.call(that);
	    that.username = getUsername.call(that);
	    that.password = getPassword.call(that);
	    that.host = getHost.call(that);
	    that.hostname = getHostname.call(that);
	    that.port = getPort.call(that);
	    that.pathname = getPathname.call(that);
	    that.search = getSearch.call(that);
	    that.searchParams = getSearchParams.call(that);
	    that.hash = getHash.call(that);
	  }
	};

	var URLPrototype = URLConstructor.prototype;

	var serializeURL = function () {
	  var url = getInternalURLState(this);
	  var scheme = url.scheme;
	  var username = url.username;
	  var password = url.password;
	  var host = url.host;
	  var port = url.port;
	  var path = url.path;
	  var query = url.query;
	  var fragment = url.fragment;
	  var output = scheme + ':';
	  if (host !== null) {
	    output += '//';
	    if (includesCredentials(url)) {
	      output += username + (password ? ':' + password : '') + '@';
	    }
	    output += serializeHost(host);
	    if (port !== null) output += ':' + port;
	  } else if (scheme == 'file') output += '//';
	  output += url.cannotBeABaseURL ? path[0] : path.length ? '/' + path.join('/') : '';
	  if (query !== null) output += '?' + query;
	  if (fragment !== null) output += '#' + fragment;
	  return output;
	};

	var getOrigin = function () {
	  var url = getInternalURLState(this);
	  var scheme = url.scheme;
	  var port = url.port;
	  if (scheme == 'blob') try {
	    return new URL(scheme.path[0]).origin;
	  } catch (error) {
	    return 'null';
	  }
	  if (scheme == 'file' || !isSpecial(url)) return 'null';
	  return scheme + '://' + serializeHost(url.host) + (port !== null ? ':' + port : '');
	};

	var getProtocol = function () {
	  return getInternalURLState(this).scheme + ':';
	};

	var getUsername = function () {
	  return getInternalURLState(this).username;
	};

	var getPassword = function () {
	  return getInternalURLState(this).password;
	};

	var getHost = function () {
	  var url = getInternalURLState(this);
	  var host = url.host;
	  var port = url.port;
	  return host === null ? ''
	    : port === null ? serializeHost(host)
	    : serializeHost(host) + ':' + port;
	};

	var getHostname = function () {
	  var host = getInternalURLState(this).host;
	  return host === null ? '' : serializeHost(host);
	};

	var getPort = function () {
	  var port = getInternalURLState(this).port;
	  return port === null ? '' : String(port);
	};

	var getPathname = function () {
	  var url = getInternalURLState(this);
	  var path = url.path;
	  return url.cannotBeABaseURL ? path[0] : path.length ? '/' + path.join('/') : '';
	};

	var getSearch = function () {
	  var query = getInternalURLState(this).query;
	  return query ? '?' + query : '';
	};

	var getSearchParams = function () {
	  return getInternalURLState(this).searchParams;
	};

	var getHash = function () {
	  var fragment = getInternalURLState(this).fragment;
	  return fragment ? '#' + fragment : '';
	};

	var accessorDescriptor = function (getter, setter) {
	  return { get: getter, set: setter, configurable: true, enumerable: true };
	};

	if (descriptors) {
	  objectDefineProperties(URLPrototype, {
	    // `URL.prototype.href` accessors pair
	    // https://url.spec.whatwg.org/#dom-url-href
	    href: accessorDescriptor(serializeURL, function (href) {
	      var url = getInternalURLState(this);
	      var urlString = String(href);
	      var failure = parseURL(url, urlString);
	      if (failure) throw TypeError(failure);
	      getInternalSearchParamsState(url.searchParams).updateSearchParams(url.query);
	    }),
	    // `URL.prototype.origin` getter
	    // https://url.spec.whatwg.org/#dom-url-origin
	    origin: accessorDescriptor(getOrigin),
	    // `URL.prototype.protocol` accessors pair
	    // https://url.spec.whatwg.org/#dom-url-protocol
	    protocol: accessorDescriptor(getProtocol, function (protocol) {
	      var url = getInternalURLState(this);
	      parseURL(url, String(protocol) + ':', SCHEME_START);
	    }),
	    // `URL.prototype.username` accessors pair
	    // https://url.spec.whatwg.org/#dom-url-username
	    username: accessorDescriptor(getUsername, function (username) {
	      var url = getInternalURLState(this);
	      var codePoints = arrayFrom(String(username));
	      if (cannotHaveUsernamePasswordPort(url)) return;
	      url.username = '';
	      for (var i = 0; i < codePoints.length; i++) {
	        url.username += percentEncode(codePoints[i], userinfoPercentEncodeSet);
	      }
	    }),
	    // `URL.prototype.password` accessors pair
	    // https://url.spec.whatwg.org/#dom-url-password
	    password: accessorDescriptor(getPassword, function (password) {
	      var url = getInternalURLState(this);
	      var codePoints = arrayFrom(String(password));
	      if (cannotHaveUsernamePasswordPort(url)) return;
	      url.password = '';
	      for (var i = 0; i < codePoints.length; i++) {
	        url.password += percentEncode(codePoints[i], userinfoPercentEncodeSet);
	      }
	    }),
	    // `URL.prototype.host` accessors pair
	    // https://url.spec.whatwg.org/#dom-url-host
	    host: accessorDescriptor(getHost, function (host) {
	      var url = getInternalURLState(this);
	      if (url.cannotBeABaseURL) return;
	      parseURL(url, String(host), HOST);
	    }),
	    // `URL.prototype.hostname` accessors pair
	    // https://url.spec.whatwg.org/#dom-url-hostname
	    hostname: accessorDescriptor(getHostname, function (hostname) {
	      var url = getInternalURLState(this);
	      if (url.cannotBeABaseURL) return;
	      parseURL(url, String(hostname), HOSTNAME);
	    }),
	    // `URL.prototype.port` accessors pair
	    // https://url.spec.whatwg.org/#dom-url-port
	    port: accessorDescriptor(getPort, function (port) {
	      var url = getInternalURLState(this);
	      if (cannotHaveUsernamePasswordPort(url)) return;
	      port = String(port);
	      if (port == '') url.port = null;
	      else parseURL(url, port, PORT);
	    }),
	    // `URL.prototype.pathname` accessors pair
	    // https://url.spec.whatwg.org/#dom-url-pathname
	    pathname: accessorDescriptor(getPathname, function (pathname) {
	      var url = getInternalURLState(this);
	      if (url.cannotBeABaseURL) return;
	      url.path = [];
	      parseURL(url, pathname + '', PATH_START);
	    }),
	    // `URL.prototype.search` accessors pair
	    // https://url.spec.whatwg.org/#dom-url-search
	    search: accessorDescriptor(getSearch, function (search) {
	      var url = getInternalURLState(this);
	      search = String(search);
	      if (search == '') {
	        url.query = null;
	      } else {
	        if ('?' == search.charAt(0)) search = search.slice(1);
	        url.query = '';
	        parseURL(url, search, QUERY);
	      }
	      getInternalSearchParamsState(url.searchParams).updateSearchParams(url.query);
	    }),
	    // `URL.prototype.searchParams` getter
	    // https://url.spec.whatwg.org/#dom-url-searchparams
	    searchParams: accessorDescriptor(getSearchParams),
	    // `URL.prototype.hash` accessors pair
	    // https://url.spec.whatwg.org/#dom-url-hash
	    hash: accessorDescriptor(getHash, function (hash) {
	      var url = getInternalURLState(this);
	      hash = String(hash);
	      if (hash == '') {
	        url.fragment = null;
	        return;
	      }
	      if ('#' == hash.charAt(0)) hash = hash.slice(1);
	      url.fragment = '';
	      parseURL(url, hash, FRAGMENT);
	    })
	  });
	}

	// `URL.prototype.toJSON` method
	// https://url.spec.whatwg.org/#dom-url-tojson
	redefine(URLPrototype, 'toJSON', function toJSON() {
	  return serializeURL.call(this);
	}, { enumerable: true });

	// `URL.prototype.toString` method
	// https://url.spec.whatwg.org/#URL-stringification-behavior
	redefine(URLPrototype, 'toString', function toString() {
	  return serializeURL.call(this);
	}, { enumerable: true });

	if (NativeURL) {
	  var nativeCreateObjectURL = NativeURL.createObjectURL;
	  var nativeRevokeObjectURL = NativeURL.revokeObjectURL;
	  // `URL.createObjectURL` method
	  // https://developer.mozilla.org/en-US/docs/Web/API/URL/createObjectURL
	  // eslint-disable-next-line no-unused-vars
	  if (nativeCreateObjectURL) redefine(URLConstructor, 'createObjectURL', function createObjectURL(blob) {
	    return nativeCreateObjectURL.apply(NativeURL, arguments);
	  });
	  // `URL.revokeObjectURL` method
	  // https://developer.mozilla.org/en-US/docs/Web/API/URL/revokeObjectURL
	  // eslint-disable-next-line no-unused-vars
	  if (nativeRevokeObjectURL) redefine(URLConstructor, 'revokeObjectURL', function revokeObjectURL(url) {
	    return nativeRevokeObjectURL.apply(NativeURL, arguments);
	  });
	}

	setToStringTag(URLConstructor, 'URL');

	_export({ global: true, forced: !nativeUrl, sham: !descriptors }, {
	  URL: URLConstructor
	});

	var ARRAY_BUFFER$1 = 'ArrayBuffer';
	var ArrayBuffer$3 = arrayBuffer[ARRAY_BUFFER$1];
	var NativeArrayBuffer$1 = global_1[ARRAY_BUFFER$1];

	// `ArrayBuffer` constructor
	// https://tc39.github.io/ecma262/#sec-arraybuffer-constructor
	_export({ global: true, forced: NativeArrayBuffer$1 !== ArrayBuffer$3 }, {
	  ArrayBuffer: ArrayBuffer$3
	});

	setSpecies(ARRAY_BUFFER$1);

	var NATIVE_ARRAY_BUFFER_VIEWS$2 = arrayBufferViewCore.NATIVE_ARRAY_BUFFER_VIEWS;

	// `ArrayBuffer.isView` method
	// https://tc39.github.io/ecma262/#sec-arraybuffer.isview
	_export({ target: 'ArrayBuffer', stat: true, forced: !NATIVE_ARRAY_BUFFER_VIEWS$2 }, {
	  isView: arrayBufferViewCore.isView
	});

	// `DataView` constructor
	// https://tc39.github.io/ecma262/#sec-dataview-constructor
	_export({ global: true, forced: !arrayBufferNative }, {
	  DataView: arrayBuffer.DataView
	});

	var browserPonyfill = createCommonjsModule(function (module, exports) {
	  var __self__ = function (root) {
	    function F() {
	      this.fetch = false;
	      this.DOMException = root.DOMException;
	    }

	    F.prototype = root;
	    return new F();
	  }(typeof self !== 'undefined' ? self : commonjsGlobal);

	  (function (self) {
	    var irrelevant = function (exports) {
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

	      exports.DOMException = self.DOMException;

	      try {
	        new exports.DOMException();
	      } catch (err) {
	        exports.DOMException = function (message, name) {
	          this.message = message;
	          this.name = name;
	          var error = Error(message);
	          this.stack = error.stack;
	        };

	        exports.DOMException.prototype = Object.create(Error.prototype);
	        exports.DOMException.prototype.constructor = exports.DOMException;
	      }

	      function fetch(input, init) {
	        return new Promise(function (resolve, reject) {
	          var request = new Request(input, init);

	          if (request.signal && request.signal.aborted) {
	            return reject(new exports.DOMException('Aborted', 'AbortError'));
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
	            reject(new exports.DOMException('Aborted', 'AbortError'));
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

	      exports.Headers = Headers;
	      exports.Request = Request;
	      exports.Response = Response;
	      exports.fetch = fetch;
	      return exports;
	    }({});
	  })(__self__);

	  delete __self__.fetch.polyfill;
	  exports = __self__.fetch; // To enable: import fetch from 'cross-fetch'

	  exports["default"] = __self__.fetch; // For TypeScript consumers without esModuleInterop.

	  exports.fetch = __self__.fetch; // To enable: import {fetch} from 'cross-fetch'

	  exports.Headers = __self__.Headers;
	  exports.Request = __self__.Request;
	  exports.Response = __self__.Response;
	  module.exports = exports;
	});
	var browserPonyfill_1 = browserPonyfill.fetch;
	var browserPonyfill_2 = browserPonyfill.Headers;
	var browserPonyfill_3 = browserPonyfill.Request;
	var browserPonyfill_4 = browserPonyfill.Response;

	/**
	 *
	 *
	 * @author Jerry Bendy <jerry@icewingcc.com>
	 * @licence MIT
	 *
	 */

	(function (self) {

	  var nativeURLSearchParams = self.URLSearchParams && self.URLSearchParams.prototype.get ? self.URLSearchParams : null,
	      isSupportObjectConstructor = nativeURLSearchParams && new nativeURLSearchParams({
	    a: 1
	  }).toString() === 'a=1',
	      // There is a bug in safari 10.1 (and earlier) that incorrectly decodes `%2B` as an empty space and not a plus.
	  decodesPlusesCorrectly = nativeURLSearchParams && new nativeURLSearchParams('s=%2B').get('s') === '+',
	      __URLSearchParams__ = "__URLSearchParams__",
	      // Fix bug in Edge which cannot encode ' &' correctly
	  encodesAmpersandsCorrectly = nativeURLSearchParams ? function () {
	    var ampersandTest = new nativeURLSearchParams();
	    ampersandTest.append('s', ' &');
	    return ampersandTest.toString() === 's=+%26';
	  }() : true,
	      prototype = URLSearchParamsPolyfill.prototype,
	      iterable = !!(self.Symbol && self.Symbol.iterator);

	  if (nativeURLSearchParams && isSupportObjectConstructor && decodesPlusesCorrectly && encodesAmpersandsCorrectly) {
	    return;
	  }
	  /**
	   * Make a URLSearchParams instance
	   *
	   * @param {object|string|URLSearchParams} search
	   * @constructor
	   */


	  function URLSearchParamsPolyfill(search) {
	    search = search || ""; // support construct object with another URLSearchParams instance

	    if (search instanceof URLSearchParams || search instanceof URLSearchParamsPolyfill) {
	      search = search.toString();
	    }

	    this[__URLSearchParams__] = parseToDict(search);
	  }
	  /**
	   * Appends a specified key/value pair as a new search parameter.
	   *
	   * @param {string} name
	   * @param {string} value
	   */


	  prototype.append = function (name, value) {
	    appendTo(this[__URLSearchParams__], name, value);
	  };
	  /**
	   * Deletes the given search parameter, and its associated value,
	   * from the list of all search parameters.
	   *
	   * @param {string} name
	   */


	  prototype['delete'] = function (name) {
	    delete this[__URLSearchParams__][name];
	  };
	  /**
	   * Returns the first value associated to the given search parameter.
	   *
	   * @param {string} name
	   * @returns {string|null}
	   */


	  prototype.get = function (name) {
	    var dict = this[__URLSearchParams__];
	    return name in dict ? dict[name][0] : null;
	  };
	  /**
	   * Returns all the values association with a given search parameter.
	   *
	   * @param {string} name
	   * @returns {Array}
	   */


	  prototype.getAll = function (name) {
	    var dict = this[__URLSearchParams__];
	    return name in dict ? dict[name].slice(0) : [];
	  };
	  /**
	   * Returns a Boolean indicating if such a search parameter exists.
	   *
	   * @param {string} name
	   * @returns {boolean}
	   */


	  prototype.has = function (name) {
	    return name in this[__URLSearchParams__];
	  };
	  /**
	   * Sets the value associated to a given search parameter to
	   * the given value. If there were several values, delete the
	   * others.
	   *
	   * @param {string} name
	   * @param {string} value
	   */


	  prototype.set = function set(name, value) {
	    this[__URLSearchParams__][name] = ['' + value];
	  };
	  /**
	   * Returns a string containg a query string suitable for use in a URL.
	   *
	   * @returns {string}
	   */


	  prototype.toString = function () {
	    var dict = this[__URLSearchParams__],
	        query = [],
	        i,
	        key,
	        name,
	        value;

	    for (key in dict) {
	      name = encode(key);

	      for (i = 0, value = dict[key]; i < value.length; i++) {
	        query.push(name + '=' + encode(value[i]));
	      }
	    }

	    return query.join('&');
	  }; // There is a bug in Safari 10.1 and `Proxy`ing it is not enough.


	  var forSureUsePolyfill = !decodesPlusesCorrectly;
	  var useProxy = !forSureUsePolyfill && nativeURLSearchParams && !isSupportObjectConstructor && self.Proxy;
	  /*
	   * Apply polifill to global object and append other prototype into it
	   */

	  Object.defineProperty(self, 'URLSearchParams', {
	    value: useProxy ? // Safari 10.0 doesn't support Proxy, so it won't extend URLSearchParams on safari 10.0
	    new Proxy(nativeURLSearchParams, {
	      construct: function construct(target, args) {
	        return new target(new URLSearchParamsPolyfill(args[0]).toString());
	      }
	    }) : URLSearchParamsPolyfill
	  });
	  var USPProto = self.URLSearchParams.prototype;
	  USPProto.polyfill = true;
	  /**
	   *
	   * @param {function} callback
	   * @param {object} thisArg
	   */

	  USPProto.forEach = USPProto.forEach || function (callback, thisArg) {
	    var dict = parseToDict(this.toString());
	    Object.getOwnPropertyNames(dict).forEach(function (name) {
	      dict[name].forEach(function (value) {
	        callback.call(thisArg, value, name, this);
	      }, this);
	    }, this);
	  };
	  /**
	   * Sort all name-value pairs
	   */


	  USPProto.sort = USPProto.sort || function () {
	    var dict = parseToDict(this.toString()),
	        keys = [],
	        k,
	        i,
	        j;

	    for (k in dict) {
	      keys.push(k);
	    }

	    keys.sort();

	    for (i = 0; i < keys.length; i++) {
	      this['delete'](keys[i]);
	    }

	    for (i = 0; i < keys.length; i++) {
	      var key = keys[i],
	          values = dict[key];

	      for (j = 0; j < values.length; j++) {
	        this.append(key, values[j]);
	      }
	    }
	  };
	  /**
	   * Returns an iterator allowing to go through all keys of
	   * the key/value pairs contained in this object.
	   *
	   * @returns {function}
	   */


	  USPProto.keys = USPProto.keys || function () {
	    var items = [];
	    this.forEach(function (item, name) {
	      items.push(name);
	    });
	    return makeIterator(items);
	  };
	  /**
	   * Returns an iterator allowing to go through all values of
	   * the key/value pairs contained in this object.
	   *
	   * @returns {function}
	   */


	  USPProto.values = USPProto.values || function () {
	    var items = [];
	    this.forEach(function (item) {
	      items.push(item);
	    });
	    return makeIterator(items);
	  };
	  /**
	   * Returns an iterator allowing to go through all key/value
	   * pairs contained in this object.
	   *
	   * @returns {function}
	   */


	  USPProto.entries = USPProto.entries || function () {
	    var items = [];
	    this.forEach(function (item, name) {
	      items.push([name, item]);
	    });
	    return makeIterator(items);
	  };

	  if (iterable) {
	    USPProto[self.Symbol.iterator] = USPProto[self.Symbol.iterator] || USPProto.entries;
	  }

	  function encode(str) {
	    var replace = {
	      '!': '%21',
	      "'": '%27',
	      '(': '%28',
	      ')': '%29',
	      '~': '%7E',
	      '%20': '+',
	      '%00': '\x00'
	    };
	    return encodeURIComponent(str).replace(/[!'\(\)~]|%20|%00/g, function (match) {
	      return replace[match];
	    });
	  }

	  function decode(str) {
	    return str.replace(/[ +]/g, '%20').replace(/(%[a-f0-9]{2})+/ig, function (match) {
	      return decodeURIComponent(match);
	    });
	  }

	  function makeIterator(arr) {
	    var iterator = {
	      next: function next() {
	        var value = arr.shift();
	        return {
	          done: value === undefined,
	          value: value
	        };
	      }
	    };

	    if (iterable) {
	      iterator[self.Symbol.iterator] = function () {
	        return iterator;
	      };
	    }

	    return iterator;
	  }

	  function parseToDict(search) {
	    var dict = {};

	    if (_typeof(search) === "object") {
	      // if `search` is an array, treat it as a sequence
	      if (isArray(search)) {
	        for (var i = 0; i < search.length; i++) {
	          var item = search[i];

	          if (isArray(item) && item.length === 2) {
	            appendTo(dict, item[0], item[1]);
	          } else {
	            throw new TypeError("Failed to construct 'URLSearchParams': Sequence initializer must only contain pair elements");
	          }
	        }
	      } else {
	        for (var key in search) {
	          if (search.hasOwnProperty(key)) {
	            appendTo(dict, key, search[key]);
	          }
	        }
	      }
	    } else {
	      // remove first '?'
	      if (search.indexOf("?") === 0) {
	        search = search.slice(1);
	      }

	      var pairs = search.split("&");

	      for (var j = 0; j < pairs.length; j++) {
	        var value = pairs[j],
	            index = value.indexOf('=');

	        if (-1 < index) {
	          appendTo(dict, decode(value.slice(0, index)), decode(value.slice(index + 1)));
	        } else {
	          if (value) {
	            appendTo(dict, decode(value), '');
	          }
	        }
	      }
	    }

	    return dict;
	  }

	  function appendTo(dict, name, value) {
	    var val = typeof value === 'string' ? value : value !== null && value !== undefined && typeof value.toString === 'function' ? value.toString() : JSON.stringify(value);

	    if (name in dict) {
	      dict[name].push(val);
	    } else {
	      dict[name] = [val];
	    }
	  }

	  function isArray(val) {
	    return !!val && '[object Array]' === Object.prototype.toString.call(val);
	  }
	})(typeof commonjsGlobal !== 'undefined' ? commonjsGlobal : typeof window !== 'undefined' ? window : commonjsGlobal);

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
	        return _objectSpread({}, a, {}, b);
	      }, {}));
	      var spanCounter = 1; // Currently, all methods make GET requests. If a method makes a request with a body payload,
	      // remember to add a 'Content-Type' header.

	      var runRequest = function runRequest() {
	        return browserPonyfill("".concat(_this.apiUrl, "/").concat(method).concat(searchParams.toString().length ? "?".concat(searchParams) : ''), {
	          headers: _objectSpread({
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
	          regeneratorRuntime.mark(function _callee() {
	            var result;
	            return regeneratorRuntime.wrap(function _callee$(_context) {
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
	      return this.transport.request(method, _objectSpread({
	        baseRequestId: v4_1()
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
	        regeneratorRuntime.mark(function _callee() {
	          var requestInstance;
	          return regeneratorRuntime.wrap(function _callee$(_context) {
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
	        regeneratorRuntime.mark(function _callee4(response, next) {
	          var requestInstance, subscribeNext, continueWait, body, error, result;
	          return regeneratorRuntime.wrap(function _callee4$(_context4) {
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
	                    regeneratorRuntime.mark(function _callee2() {
	                      return regeneratorRuntime.wrap(function _callee2$(_context2) {
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
	                    regeneratorRuntime.mark(function _callee3(wait) {
	                      return regeneratorRuntime.wrap(function _callee3$(_context3) {
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
	            regeneratorRuntime.mark(function _callee5() {
	              var requestInstance;
	              return regeneratorRuntime.wrap(function _callee5$(_context5) {
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
	      regeneratorRuntime.mark(function _callee6() {
	        var token;
	        return regeneratorRuntime.wrap(function _callee6$(_context6) {
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
	      }, _objectSpread({}, options, {
	        subscribe: true
	      }), callback);
	    }
	  }]);

	  return CubejsApi;
	}();

	var index$1 = (function (apiToken, options) {
	  return new CubejsApi(apiToken, options);
	});

	var src = /*#__PURE__*/Object.freeze({
		default: index$1,
		HttpTransport: HttpTransport,
		ResultSet: ResultSet
	});

	var index$2 = ( src && index$1 ) || src;

	var index_umd = index$2;

	return index_umd;

}));
