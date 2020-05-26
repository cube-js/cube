(function (global, factory) {
  typeof exports === 'object' && typeof module !== 'undefined' ? factory(exports, require('react'), require('prop-types')) :
  typeof define === 'function' && define.amd ? define(['exports', 'react', 'prop-types'], factory) :
  (global = global || self, factory(global.cubejsReact = {}, global.React, global.PropTypes));
}(this, function (exports, React, PropTypes) { 'use strict';

  var React__default = 'default' in React ? React['default'] : React;

  var fails = function (exec) {
    try {
      return !!exec();
    } catch (error) {
      return true;
    }
  };

  var toString = {}.toString;

  var classofRaw = function (it) {
    return toString.call(it).slice(8, -1);
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

  var commonjsGlobal = typeof window !== 'undefined' ? window : typeof global !== 'undefined' ? global : typeof self !== 'undefined' ? self : {};

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

  var hiddenKeys = {};

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

  var path = global_1;

  var aFunction = function (variable) {
    return typeof variable == 'function' ? variable : undefined;
  };

  var getBuiltIn = function (namespace, method) {
    return arguments.length < 2 ? aFunction(path[namespace]) || aFunction(global_1[namespace])
      : path[namespace] && path[namespace][method] || global_1[namespace] && global_1[namespace][method];
  };

  var html = getBuiltIn('document', 'documentElement');

  var keys = shared('keys');

  var sharedKey = function (key) {
    return keys[key] || (keys[key] = uid(key));
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

  var UNSCOPABLES = wellKnownSymbol('unscopables');
  var ArrayPrototype = Array.prototype;

  // Array.prototype[@@unscopables]
  // https://tc39.github.io/ecma262/#sec-array.prototype-@@unscopables
  if (ArrayPrototype[UNSCOPABLES] == undefined) {
    objectDefineProperty.f(ArrayPrototype, UNSCOPABLES, {
      configurable: true,
      value: objectCreate(null)
    });
  }

  // add a key to Array.prototype[@@unscopables]
  var addToUnscopables = function (key) {
    ArrayPrototype[UNSCOPABLES][key] = true;
  };

  var iterators = {};

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

  // `ToObject` abstract operation
  // https://tc39.github.io/ecma262/#sec-toobject
  var toObject = function (argument) {
    return Object(requireObjectCoercible(argument));
  };

  var correctPrototypeGetter = !fails(function () {
    function F() { /* empty */ }
    F.prototype.constructor = null;
    return Object.getPrototypeOf(new F()) !== F.prototype;
  });

  var IE_PROTO$1 = sharedKey('IE_PROTO');
  var ObjectPrototype = Object.prototype;

  // `Object.getPrototypeOf` method
  // https://tc39.github.io/ecma262/#sec-object.getprototypeof
  var objectGetPrototypeOf = correctPrototypeGetter ? Object.getPrototypeOf : function (O) {
    O = toObject(O);
    if (has(O, IE_PROTO$1)) return O[IE_PROTO$1];
    if (typeof O.constructor == 'function' && O instanceof O.constructor) {
      return O.constructor.prototype;
    } return O instanceof Object ? ObjectPrototype : null;
  };

  var ITERATOR = wellKnownSymbol('iterator');
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
  if (!has(IteratorPrototype, ITERATOR)) {
    createNonEnumerableProperty(IteratorPrototype, ITERATOR, returnThis);
  }

  var iteratorsCore = {
    IteratorPrototype: IteratorPrototype,
    BUGGY_SAFARI_ITERATORS: BUGGY_SAFARI_ITERATORS
  };

  var defineProperty = objectDefineProperty.f;



  var TO_STRING_TAG = wellKnownSymbol('toStringTag');

  var setToStringTag = function (it, TAG, STATIC) {
    if (it && !has(it = STATIC ? it : it.prototype, TO_STRING_TAG)) {
      defineProperty(it, TO_STRING_TAG, { configurable: true, value: TAG });
    }
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
  var ITERATOR$1 = wellKnownSymbol('iterator');
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
    var nativeIterator = IterablePrototype[ITERATOR$1]
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
          } else if (typeof CurrentIteratorPrototype[ITERATOR$1] != 'function') {
            createNonEnumerableProperty(CurrentIteratorPrototype, ITERATOR$1, returnThis$2);
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
    if (IterablePrototype[ITERATOR$1] !== defaultIterator) {
      createNonEnumerableProperty(IterablePrototype, ITERATOR$1, defaultIterator);
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
  var setInternalState = internalState.set;
  var getInternalState = internalState.getterFor(ARRAY_ITERATOR);

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
    setInternalState(this, {
      type: ARRAY_ITERATOR,
      target: toIndexedObject(iterated), // target
      index: 0,                          // next index
      kind: kind                         // kind
    });
  // `%ArrayIteratorPrototype%.next` method
  // https://tc39.github.io/ecma262/#sec-%arrayiteratorprototype%.next
  }, function () {
    var state = getInternalState(this);
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

  var aFunction$1 = function (it) {
    if (typeof it != 'function') {
      throw TypeError(String(it) + ' is not a function');
    } return it;
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

  // `IsArray` abstract operation
  // https://tc39.github.io/ecma262/#sec-isarray
  var isArray = Array.isArray || function isArray(arg) {
    return classofRaw(arg) == 'Array';
  };

  var SPECIES = wellKnownSymbol('species');

  // `ArraySpeciesCreate` abstract operation
  // https://tc39.github.io/ecma262/#sec-arrayspeciescreate
  var arraySpeciesCreate = function (originalArray, length) {
    var C;
    if (isArray(originalArray)) {
      C = originalArray.constructor;
      // cross-realm fallback
      if (typeof C == 'function' && (C === Array || isArray(C.prototype))) C = undefined;
      else if (isObject(C)) {
        C = C[SPECIES];
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

  var engineUserAgent = getBuiltIn('navigator', 'userAgent') || '';

  var process = global_1.process;
  var versions = process && process.versions;
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

  var SPECIES$1 = wellKnownSymbol('species');

  var arrayMethodHasSpeciesSupport = function (METHOD_NAME) {
    // We can't use this feature detection in V8 since it causes
    // deoptimization and serious performance degradation
    // https://github.com/zloirock/core-js/issues/677
    return engineV8Version >= 51 || !fails(function () {
      var array = [];
      var constructor = array.constructor = {};
      constructor[SPECIES$1] = function () {
        return { foo: 1 };
      };
      return array[METHOD_NAME](Boolean).foo !== 1;
    });
  };

  var defineProperty$1 = Object.defineProperty;
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

      if (ACCESSORS) defineProperty$1(O, 1, { enumerable: true, get: thrower });
      else O[1] = 1;

      method.call(O, argument0, argument1);
    });
  };

  var $map = arrayIteration.map;



  var HAS_SPECIES_SUPPORT = arrayMethodHasSpeciesSupport('map');
  // FF49- issue
  var USES_TO_LENGTH = arrayMethodUsesToLength('map');

  // `Array.prototype.map` method
  // https://tc39.github.io/ecma262/#sec-array.prototype.map
  // with adding support of @@species
  _export({ target: 'Array', proto: true, forced: !HAS_SPECIES_SUPPORT || !USES_TO_LENGTH }, {
    map: function map(callbackfn /* , thisArg */) {
      return $map(this, callbackfn, arguments.length > 1 ? arguments[1] : undefined);
    }
  });

  var TO_STRING_TAG$1 = wellKnownSymbol('toStringTag');
  var test = {};

  test[TO_STRING_TAG$1] = 'z';

  var toStringTagSupport = String(test) === '[object z]';

  var TO_STRING_TAG$2 = wellKnownSymbol('toStringTag');
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
      : typeof (tag = tryGet(O = Object(it), TO_STRING_TAG$2)) == 'string' ? tag
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

  var nativePromiseConstructor = global_1.Promise;

  var redefineAll = function (target, src, options) {
    for (var key in src) redefine(target, key, src[key], options);
    return target;
  };

  var SPECIES$2 = wellKnownSymbol('species');

  var setSpecies = function (CONSTRUCTOR_NAME) {
    var Constructor = getBuiltIn(CONSTRUCTOR_NAME);
    var defineProperty = objectDefineProperty.f;

    if (descriptors && Constructor && !Constructor[SPECIES$2]) {
      defineProperty(Constructor, SPECIES$2, {
        configurable: true,
        get: function () { return this; }
      });
    }
  };

  var anInstance = function (it, Constructor, name) {
    if (!(it instanceof Constructor)) {
      throw TypeError('Incorrect ' + (name ? name + ' ' : '') + 'invocation');
    } return it;
  };

  var ITERATOR$2 = wellKnownSymbol('iterator');
  var ArrayPrototype$1 = Array.prototype;

  // check on default Array iterator
  var isArrayIteratorMethod = function (it) {
    return it !== undefined && (iterators.Array === it || ArrayPrototype$1[ITERATOR$2] === it);
  };

  var ITERATOR$3 = wellKnownSymbol('iterator');

  var getIteratorMethod = function (it) {
    if (it != undefined) return it[ITERATOR$3]
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

  var ITERATOR$4 = wellKnownSymbol('iterator');
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
    iteratorWithReturn[ITERATOR$4] = function () {
      return this;
    };
  } catch (error) { /* empty */ }

  var checkCorrectnessOfIteration = function (exec, SKIP_CLOSING) {
    if (!SKIP_CLOSING && !SAFE_CLOSING) return false;
    var ITERATION_SUPPORT = false;
    try {
      var object = {};
      object[ITERATOR$4] = function () {
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

  var SPECIES$3 = wellKnownSymbol('species');

  // `SpeciesConstructor` abstract operation
  // https://tc39.github.io/ecma262/#sec-speciesconstructor
  var speciesConstructor = function (O, defaultConstructor) {
    var C = anObject(O).constructor;
    var S;
    return C === undefined || (S = anObject(C)[SPECIES$3]) == undefined ? defaultConstructor : aFunction$1(S);
  };

  var engineIsIos = /(iphone|ipod|ipad).*applewebkit/i.test(engineUserAgent);

  var location = global_1.location;
  var set$1 = global_1.setImmediate;
  var clear = global_1.clearImmediate;
  var process$1 = global_1.process;
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
    if (classofRaw(process$1) == 'process') {
      defer = function (id) {
        process$1.nextTick(runner(id));
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
  var process$2 = global_1.process;
  var Promise$1 = global_1.Promise;
  var IS_NODE = classofRaw(process$2) == 'process';
  // Node.js 11 shows ExperimentalWarning on getting `queueMicrotask`
  var queueMicrotaskDescriptor = getOwnPropertyDescriptor$2(global_1, 'queueMicrotask');
  var queueMicrotask = queueMicrotaskDescriptor && queueMicrotaskDescriptor.value;

  var flush, head, last, notify, toggle, node, promise, then;

  // modern engines have queueMicrotask method
  if (!queueMicrotask) {
    flush = function () {
      var parent, fn;
      if (IS_NODE && (parent = process$2.domain)) parent.exit();
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
        process$2.nextTick(flush);
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

  var task$1 = task.set;










  var SPECIES$4 = wellKnownSymbol('species');
  var PROMISE = 'Promise';
  var getInternalState$1 = internalState.get;
  var setInternalState$1 = internalState.set;
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
    constructor[SPECIES$4] = FakePromise;
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
      var state = getInternalState$1(this);
      try {
        executor(bind(internalResolve, this, state), bind(internalReject, this, state));
      } catch (error) {
        internalReject(this, state, error);
      }
    };
    // eslint-disable-next-line no-unused-vars
    Internal = function Promise(executor) {
      setInternalState$1(this, {
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
      var state = getInternalState$1(promise);
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
  var setInternalState$2 = internalState.set;
  var getInternalState$2 = internalState.getterFor(STRING_ITERATOR);

  // `String.prototype[@@iterator]` method
  // https://tc39.github.io/ecma262/#sec-string.prototype-@@iterator
  defineIterator(String, 'String', function (iterated) {
    setInternalState$2(this, {
      type: STRING_ITERATOR,
      string: String(iterated),
      index: 0
    });
  // `%StringIteratorPrototype%.next` method
  // https://tc39.github.io/ecma262/#sec-%stringiteratorprototype%.next
  }, function next() {
    var state = getInternalState$2(this);
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

  var ITERATOR$5 = wellKnownSymbol('iterator');
  var TO_STRING_TAG$3 = wellKnownSymbol('toStringTag');
  var ArrayValues = es_array_iterator.values;

  for (var COLLECTION_NAME in domIterables) {
    var Collection = global_1[COLLECTION_NAME];
    var CollectionPrototype = Collection && Collection.prototype;
    if (CollectionPrototype) {
      // some Chrome versions have non-configurable methods on DOMTokenList
      if (CollectionPrototype[ITERATOR$5] !== ArrayValues) try {
        createNonEnumerableProperty(CollectionPrototype, ITERATOR$5, ArrayValues);
      } catch (error) {
        CollectionPrototype[ITERATOR$5] = ArrayValues;
      }
      if (!CollectionPrototype[TO_STRING_TAG$3]) {
        createNonEnumerableProperty(CollectionPrototype, TO_STRING_TAG$3, COLLECTION_NAME);
      }
      if (domIterables[COLLECTION_NAME]) for (var METHOD_NAME in es_array_iterator) {
        // some Chrome versions have non-configurable methods on DOMTokenList
        if (CollectionPrototype[METHOD_NAME] !== es_array_iterator[METHOD_NAME]) try {
          createNonEnumerableProperty(CollectionPrototype, METHOD_NAME, es_array_iterator[METHOD_NAME]);
        } catch (error) {
          CollectionPrototype[METHOD_NAME] = es_array_iterator[METHOD_NAME];
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

  // a string of all valid unicode whitespaces
  // eslint-disable-next-line max-len
  var whitespaces = '\u0009\u000A\u000B\u000C\u000D\u0020\u00A0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200A\u202F\u205F\u3000\u2028\u2029\uFEFF';

  var whitespace = '[' + whitespaces + ']';
  var ltrim = RegExp('^' + whitespace + whitespace + '*');
  var rtrim = RegExp(whitespace + whitespace + '*$');

  // `String.prototype.{ trim, trimStart, trimEnd, trimLeft, trimRight }` methods implementation
  var createMethod$3 = function (TYPE) {
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
    start: createMethod$3(1),
    // `String.prototype.{ trimRight, trimEnd }` methods
    // https://tc39.github.io/ecma262/#sec-string.prototype.trimend
    end: createMethod$3(2),
    // `String.prototype.trim` method
    // https://tc39.github.io/ecma262/#sec-string.prototype.trim
    trim: createMethod$3(3)
  };

  var getOwnPropertyNames = objectGetOwnPropertyNames.f;
  var getOwnPropertyDescriptor$3 = objectGetOwnPropertyDescriptor.f;
  var defineProperty$2 = objectDefineProperty.f;
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
    for (var keys$1 = descriptors ? getOwnPropertyNames(NativeNumber) : (
      // ES3:
      'MAX_VALUE,MIN_VALUE,NaN,NEGATIVE_INFINITY,POSITIVE_INFINITY,' +
      // ES2015 (in case, if modules with ES2015 Number statics required before):
      'EPSILON,isFinite,isInteger,isNaN,isSafeInteger,MAX_SAFE_INTEGER,' +
      'MIN_SAFE_INTEGER,parseFloat,parseInt,isInteger'
    ).split(','), j = 0, key; keys$1.length > j; j++) {
      if (has(NativeNumber, key = keys$1[j]) && !has(NumberWrapper, key)) {
        defineProperty$2(NumberWrapper, key, getOwnPropertyDescriptor$3(NativeNumber, key));
      }
    }
    NumberWrapper.prototype = NumberPrototype;
    NumberPrototype.constructor = NumberWrapper;
    redefine(global_1, NUMBER, NumberWrapper);
  }

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

  var createProperty = function (object, key, value) {
    var propertyKey = toPrimitive(key);
    if (propertyKey in object) objectDefineProperty.f(object, propertyKey, createPropertyDescriptor(0, value));
    else object[propertyKey] = value;
  };

  var HAS_SPECIES_SUPPORT$1 = arrayMethodHasSpeciesSupport('slice');
  var USES_TO_LENGTH$1 = arrayMethodUsesToLength('slice', { ACCESSORS: true, 0: 0, 1: 2 });

  var SPECIES$5 = wellKnownSymbol('species');
  var nativeSlice = [].slice;
  var max$1 = Math.max;

  // `Array.prototype.slice` method
  // https://tc39.github.io/ecma262/#sec-array.prototype.slice
  // fallback for not array-like ES3 strings and DOM objects
  _export({ target: 'Array', proto: true, forced: !HAS_SPECIES_SUPPORT$1 || !USES_TO_LENGTH$1 }, {
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

  var defineProperty$3 = objectDefineProperty.f;

  var defineWellKnownSymbol = function (NAME) {
    var Symbol = path.Symbol || (path.Symbol = {});
    if (!has(Symbol, NAME)) defineProperty$3(Symbol, NAME, {
      value: wellKnownSymbolWrapped.f(NAME)
    });
  };

  var $forEach = arrayIteration.forEach;

  var HIDDEN = sharedKey('hidden');
  var SYMBOL = 'Symbol';
  var PROTOTYPE$1 = 'prototype';
  var TO_PRIMITIVE = wellKnownSymbol('toPrimitive');
  var setInternalState$3 = internalState.set;
  var getInternalState$3 = internalState.getterFor(SYMBOL);
  var ObjectPrototype$1 = Object[PROTOTYPE$1];
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
    var ObjectPrototypeDescriptor = nativeGetOwnPropertyDescriptor$1(ObjectPrototype$1, P);
    if (ObjectPrototypeDescriptor) delete ObjectPrototype$1[P];
    nativeDefineProperty$1(O, P, Attributes);
    if (ObjectPrototypeDescriptor && O !== ObjectPrototype$1) {
      nativeDefineProperty$1(ObjectPrototype$1, P, ObjectPrototypeDescriptor);
    }
  } : nativeDefineProperty$1;

  var wrap = function (tag, description) {
    var symbol = AllSymbols[tag] = objectCreate($Symbol[PROTOTYPE$1]);
    setInternalState$3(symbol, {
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
    if (O === ObjectPrototype$1) $defineProperty(ObjectPrototypeSymbols, P, Attributes);
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
    if (this === ObjectPrototype$1 && has(AllSymbols, P) && !has(ObjectPrototypeSymbols, P)) return false;
    return enumerable || !has(this, P) || !has(AllSymbols, P) || has(this, HIDDEN) && this[HIDDEN][P] ? enumerable : true;
  };

  var $getOwnPropertyDescriptor = function getOwnPropertyDescriptor(O, P) {
    var it = toIndexedObject(O);
    var key = toPrimitive(P, true);
    if (it === ObjectPrototype$1 && has(AllSymbols, key) && !has(ObjectPrototypeSymbols, key)) return;
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
    var IS_OBJECT_PROTOTYPE = O === ObjectPrototype$1;
    var names = nativeGetOwnPropertyNames$1(IS_OBJECT_PROTOTYPE ? ObjectPrototypeSymbols : toIndexedObject(O));
    var result = [];
    $forEach(names, function (key) {
      if (has(AllSymbols, key) && (!IS_OBJECT_PROTOTYPE || has(ObjectPrototype$1, key))) {
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
        if (this === ObjectPrototype$1) setter.call(ObjectPrototypeSymbols, value);
        if (has(this, HIDDEN) && has(this[HIDDEN], tag)) this[HIDDEN][tag] = false;
        setSymbolDescriptor(this, tag, createPropertyDescriptor(1, value));
      };
      if (descriptors && USE_SETTER) setSymbolDescriptor(ObjectPrototype$1, tag, { configurable: true, set: setter });
      return wrap(tag, description);
    };

    redefine($Symbol[PROTOTYPE$1], 'toString', function toString() {
      return getInternalState$3(this).tag;
    });

    redefine($Symbol, 'withoutSetter', function (description) {
      return wrap(uid(description), description);
    });

    objectPropertyIsEnumerable.f = $propertyIsEnumerable;
    objectDefineProperty.f = $defineProperty;
    objectGetOwnPropertyDescriptor.f = $getOwnPropertyDescriptor;
    objectGetOwnPropertyNames.f = objectGetOwnPropertyNamesExternal.f = $getOwnPropertyNames;
    objectGetOwnPropertySymbols.f = $getOwnPropertySymbols;

    wellKnownSymbolWrapped.f = function (name) {
      return wrap(wellKnownSymbol(name), name);
    };

    if (descriptors) {
      // https://github.com/tc39/proposal-Symbol-description
      nativeDefineProperty$1($Symbol[PROTOTYPE$1], 'description', {
        configurable: true,
        get: function description() {
          return getInternalState$3(this).description;
        }
      });
      {
        redefine(ObjectPrototype$1, 'propertyIsEnumerable', $propertyIsEnumerable, { unsafe: true });
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

  var defineProperty$4 = objectDefineProperty.f;


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
    defineProperty$4(symbolPrototype, 'description', {
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

  // `Symbol.iterator` well-known symbol
  // https://tc39.github.io/ecma262/#sec-symbol.iterator
  defineWellKnownSymbol('iterator');

  // `Array.prototype.{ reduce, reduceRight }` methods implementation
  var createMethod$4 = function (IS_RIGHT) {
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
    left: createMethod$4(false),
    // `Array.prototype.reduceRight` method
    // https://tc39.github.io/ecma262/#sec-array.prototype.reduceright
    right: createMethod$4(true)
  };

  var arrayMethodIsStrict = function (METHOD_NAME, argument) {
    var method = [][METHOD_NAME];
    return !!method && fails(function () {
      // eslint-disable-next-line no-useless-call,no-throw-literal
      method.call(null, argument || function () { throw 1; }, 1);
    });
  };

  var $reduce = arrayReduce.left;



  var STRICT_METHOD = arrayMethodIsStrict('reduce');
  var USES_TO_LENGTH$2 = arrayMethodUsesToLength('reduce', { 1: 0 });

  // `Array.prototype.reduce` method
  // https://tc39.github.io/ecma262/#sec-array.prototype.reduce
  _export({ target: 'Array', proto: true, forced: !STRICT_METHOD || !USES_TO_LENGTH$2 }, {
    reduce: function reduce(callbackfn /* , initialValue */) {
      return $reduce(this, callbackfn, arguments.length, arguments.length > 1 ? arguments[1] : undefined);
    }
  });

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

  var FAILS_ON_PRIMITIVES = fails(function () { objectKeys(1); });

  // `Object.keys` method
  // https://tc39.github.io/ecma262/#sec-object.keys
  _export({ target: 'Object', stat: true, forced: FAILS_ON_PRIMITIVES }, {
    keys: function keys(it) {
      return objectKeys(toObject(it));
    }
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


  var keys$2 = typeof Object.keys === 'function' && !hasArgsEnumBug ?
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
        }, {}, keys$2(functor));

      default:
        return _map(fn, functor);
    }
  }));

  var floor$1 = Math.floor;

  // `Number.isInteger` method implementation
  // https://tc39.github.io/ecma262/#sec-number.isinteger
  var isInteger = function isInteger(it) {
    return !isObject(it) && isFinite(it) && floor$1(it) === it;
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

  var FORCED$1 = !IS_CONCAT_SPREADABLE_SUPPORT || !SPECIES_SUPPORT;

  // `Array.prototype.concat` method
  // https://tc39.github.io/ecma262/#sec-array.prototype.concat
  // with adding support of @@isConcatSpreadable and @@species
  _export({ target: 'Array', proto: true, forced: FORCED$1 }, {
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

  var MATCH = wellKnownSymbol('match');

  // `IsRegExp` abstract operation
  // https://tc39.github.io/ecma262/#sec-isregexp
  var isRegexp = function (it) {
    var isRegExp;
    return isObject(it) && ((isRegExp = it[MATCH]) !== undefined ? !!isRegExp : classofRaw(it) == 'RegExp');
  };

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

  var defineProperty$5 = objectDefineProperty.f;
  var getOwnPropertyNames$1 = objectGetOwnPropertyNames.f;





  var setInternalState$4 = internalState.set;



  var MATCH$1 = wellKnownSymbol('match');
  var NativeRegExp = global_1.RegExp;
  var RegExpPrototype$1 = NativeRegExp.prototype;
  var re1 = /a/g;
  var re2 = /a/g;

  // "new" should create a new object, old webkit bug
  var CORRECT_NEW = new NativeRegExp(re1) !== re1;

  var UNSUPPORTED_Y$1 = regexpStickyHelpers.UNSUPPORTED_Y;

  var FORCED$2 = descriptors && isForced_1('RegExp', (!CORRECT_NEW || UNSUPPORTED_Y$1 || fails(function () {
    re2[MATCH$1] = false;
    // RegExp constructor can alter flags and IsRegExp works correct with @@match
    return NativeRegExp(re1) != re1 || NativeRegExp(re2) == re2 || NativeRegExp(re1, 'i') != '/a/i';
  })));

  // `RegExp` constructor
  // https://tc39.github.io/ecma262/#sec-regexp-constructor
  if (FORCED$2) {
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

      if (UNSUPPORTED_Y$1) {
        sticky = !!flags && flags.indexOf('y') > -1;
        if (sticky) flags = flags.replace(/y/g, '');
      }

      var result = inheritIfRequired(
        CORRECT_NEW ? new NativeRegExp(pattern, flags) : NativeRegExp(pattern, flags),
        thisIsRegExp ? this : RegExpPrototype$1,
        RegExpWrapper
      );

      if (UNSUPPORTED_Y$1 && sticky) setInternalState$4(result, { sticky: sticky });

      return result;
    };
    var proxy = function (key) {
      key in RegExpWrapper || defineProperty$5(RegExpWrapper, key, {
        configurable: true,
        get: function () { return NativeRegExp[key]; },
        set: function (it) { NativeRegExp[key] = it; }
      });
    };
    var keys$3 = getOwnPropertyNames$1(NativeRegExp);
    var index = 0;
    while (keys$3.length > index) proxy(keys$3[index++]);
    RegExpPrototype$1.constructor = RegExpWrapper;
    RegExpWrapper.prototype = RegExpPrototype$1;
    redefine(global_1, 'RegExp', RegExpWrapper);
  }

  // https://tc39.github.io/ecma262/#sec-get-regexp-@@species
  setSpecies('RegExp');

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

  var UNSUPPORTED_Y$2 = regexpStickyHelpers.UNSUPPORTED_Y || regexpStickyHelpers.BROKEN_CARET;

  // nonparticipating capturing group, copied from es5-shim's String#split patch.
  var NPCG_INCLUDED = /()??/.exec('')[1] !== undefined;

  var PATCH = UPDATES_LAST_INDEX_WRONG || NPCG_INCLUDED || UNSUPPORTED_Y$2;

  if (PATCH) {
    patchedExec = function exec(str) {
      var re = this;
      var lastIndex, reCopy, match, i;
      var sticky = UNSUPPORTED_Y$2 && re.sticky;
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

  var nativeJoin = [].join;

  var ES3_STRINGS = indexedObject != Object;
  var STRICT_METHOD$1 = arrayMethodIsStrict('join', ',');

  // `Array.prototype.join` method
  // https://tc39.github.io/ecma262/#sec-array.prototype.join
  _export({ target: 'Array', proto: true, forced: ES3_STRINGS || !STRICT_METHOD$1 }, {
    join: function join(separator) {
      return nativeJoin.call(toIndexedObject(this), separator === undefined ? ',' : separator);
    }
  });

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

  var arrayPush = [].push;
  var min$2 = Math.min;
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
            (e = min$2(toLength(splitter.lastIndex + (SUPPORTS_Y ? 0 : q)), S.length)) === p
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
  var STRICT_METHOD$2 = arrayMethodIsStrict('sort');

  var FORCED$3 = FAILS_ON_UNDEFINED || !FAILS_ON_NULL || !STRICT_METHOD$2;

  // `Array.prototype.sort` method
  // https://tc39.github.io/ecma262/#sec-array.prototype.sort
  _export({ target: 'Array', proto: true, forced: FORCED$3 }, {
    sort: function sort(comparefn) {
      return comparefn === undefined
        ? nativeSort.call(toObject(this))
        : nativeSort.call(toObject(this), aFunction$1(comparefn));
    }
  });

  var $indexOf = arrayIncludes.indexOf;



  var nativeIndexOf = [].indexOf;

  var NEGATIVE_ZERO = !!nativeIndexOf && 1 / [1].indexOf(1, -0) < 0;
  var STRICT_METHOD$3 = arrayMethodIsStrict('indexOf');
  var USES_TO_LENGTH$3 = arrayMethodUsesToLength('indexOf', { ACCESSORS: true, 1: 0 });

  // `Array.prototype.indexOf` method
  // https://tc39.github.io/ecma262/#sec-array.prototype.indexof
  _export({ target: 'Array', proto: true, forced: NEGATIVE_ZERO || !STRICT_METHOD$3 || !USES_TO_LENGTH$3 }, {
    indexOf: function indexOf(searchElement /* , fromIndex = 0 */) {
      return NEGATIVE_ZERO
        // convert -0 to +0
        ? nativeIndexOf.apply(this, arguments) || 0
        : $indexOf(this, searchElement, arguments.length > 1 ? arguments[1] : undefined);
    }
  });

  var defineProperty$6 = objectDefineProperty.f;

  var FunctionPrototype = Function.prototype;
  var FunctionPrototypeToString = FunctionPrototype.toString;
  var nameRE = /^\s*function ([^ (]*)/;
  var NAME = 'name';

  // Function instances `.name` property
  // https://tc39.github.io/ecma262/#sec-function-instances-name
  if (descriptors && !(NAME in FunctionPrototype)) {
    defineProperty$6(FunctionPrototype, NAME, {
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

    var keysA = keys$2(a);

    if (keysA.length !== keys$2(b).length) {
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
  var min$3 = Math.min;
  var floor$2 = Math.floor;
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
          var position = max$3(min$3(toInteger(result.index), S.length), 0);
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
              var f = floor$2(n / 10);
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
  var createMethod$5 = function (IS_END) {
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
    start: createMethod$5(false),
    // `String.prototype.padEnd` method
    // https://tc39.github.io/ecma262/#sec-string.prototype.padend
    end: createMethod$5(true)
  };

  var padStart = stringPad.start;

  var abs = Math.abs;
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
    return sign + padStart(abs(year), sign ? 6 : 4, 0) +
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
  var floor$3 = Math.floor;

  var pow = function (x, n, acc) {
    return n === 0 ? acc : n % 2 === 1 ? pow(x, n - 1, acc * x) : pow(x * x, n / 2, acc);
  };

  var log = function (x) {
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

  var FORCED$4 = nativeToFixed && (
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
  _export({ target: 'Number', proto: true, forced: FORCED$4 }, {
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
          c2 = floor$3(c2 / 1e7);
        }
      };

      var divide = function (n) {
        var index = 6;
        var c = 0;
        while (--index >= 0) {
          c += data[index];
          data[index] = floor$3(c / n);
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
        e = log(number * pow(2, 69, 1)) - 69;
        z = e < 0 ? number * pow(2, -e, 1) : number / pow(2, e, 1);
        z *= 0x10000000000000;
        e = 52 - e;
        if (e > 0) {
          multiply(0, z);
          j = fractDigits;
          while (j >= 7) {
            multiply(1e7, 0);
            j -= 7;
          }
          multiply(pow(10, j, 1), 0);
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
    }, {}, keys$2(filterable)) : // else
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
        }, keys$2(x)))).join(', ') + ']';

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

        return '{' + mapPairs(x, keys$2(x)).join(', ') + '}';
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

  var defineProperty$7 = objectDefineProperty.f;








  var fastKey = internalMetadata.fastKey;


  var setInternalState$5 = internalState.set;
  var internalStateGetterFor = internalState.getterFor;

  var collectionStrong = {
    getConstructor: function (wrapper, CONSTRUCTOR_NAME, IS_MAP, ADDER) {
      var C = wrapper(function (that, iterable) {
        anInstance(that, C, CONSTRUCTOR_NAME);
        setInternalState$5(that, {
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
      if (descriptors) defineProperty$7(C.prototype, 'size', {
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
        setInternalState$5(this, {
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

  var HAS_SPECIES_SUPPORT$2 = arrayMethodHasSpeciesSupport('splice');
  var USES_TO_LENGTH$4 = arrayMethodUsesToLength('splice', { ACCESSORS: true, 0: 0, 1: 2 });

  var max$4 = Math.max;
  var min$4 = Math.min;
  var MAX_SAFE_INTEGER$1 = 0x1FFFFFFFFFFFFF;
  var MAXIMUM_ALLOWED_LENGTH_EXCEEDED = 'Maximum allowed length exceeded';

  // `Array.prototype.splice` method
  // https://tc39.github.io/ecma262/#sec-array.prototype.splice
  // with adding support of @@species
  _export({ target: 'Array', proto: true, forced: !HAS_SPECIES_SUPPORT$2 || !USES_TO_LENGTH$4 }, {
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
        actualDeleteCount = min$4(max$4(toInteger(deleteCount), 0), len - actualStart);
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
  slice(0, -1);

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

  var min$5 = Math.min;
  var nativeLastIndexOf = [].lastIndexOf;
  var NEGATIVE_ZERO$1 = !!nativeLastIndexOf && 1 / [1].lastIndexOf(1, -0) < 0;
  var STRICT_METHOD$4 = arrayMethodIsStrict('lastIndexOf');
  // For preventing possible almost infinite loop in non-standard implementations, test the forward version of the method
  var USES_TO_LENGTH$5 = arrayMethodUsesToLength('indexOf', { ACCESSORS: true, 1: 0 });
  var FORCED$5 = NEGATIVE_ZERO$1 || !STRICT_METHOD$4 || !USES_TO_LENGTH$5;

  // `Array.prototype.lastIndexOf` method implementation
  // https://tc39.github.io/ecma262/#sec-array.prototype.lastindexof
  var arrayLastIndexOf = FORCED$5 ? function lastIndexOf(searchElement /* , fromIndex = @[*-1] */) {
    // convert -0 to +0
    if (NEGATIVE_ZERO$1) return nativeLastIndexOf.apply(this, arguments) || 0;
    var O = toIndexedObject(this);
    var length = toLength(O.length);
    var index = length - 1;
    if (arguments.length > 1) index = min$5(index, toInteger(arguments[1]));
    if (index < 0) index = length + index;
    for (;index >= 0; index--) if (index in O && O[index] === searchElement) return index || 0;
    return -1;
  } : nativeLastIndexOf;

  // `Array.prototype.lastIndexOf` method
  // https://tc39.github.io/ecma262/#sec-array.prototype.lastindexof
  _export({ target: 'Array', proto: true, forced: arrayLastIndexOf !== [].lastIndexOf }, {
    lastIndexOf: arrayLastIndexOf
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

  var trim$1 = !hasProtoTrim ||
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

  var isQueryPresent = (function (query) {
    return query.measures && query.measures.length || query.dimensions && query.dimensions.length || query.timeDimensions && query.timeDimensions.length;
  });

  var CubeContext = React.createContext(null);

  var QueryRenderer =
  /*#__PURE__*/
  function (_React$Component) {
    _inherits(QueryRenderer, _React$Component);

    _createClass(QueryRenderer, null, [{
      key: "isQueryPresent",
      value: function isQueryPresent$$1(query) {
        return isQueryPresent(query);
      }
    }]);

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
        var _this$props = this.props,
            query = _this$props.query,
            queries = _this$props.queries;

        if (query) {
          this.load(query);
        }

        if (queries) {
          this.loadQueries(queries);
        }
      }
    }, {
      key: "shouldComponentUpdate",
      value: function shouldComponentUpdate(nextProps, nextState) {
        var _this$props2 = this.props,
            query = _this$props2.query,
            queries = _this$props2.queries,
            render = _this$props2.render,
            cubejsApi = _this$props2.cubejsApi,
            loadSql = _this$props2.loadSql,
            updateOnlyOnStateChange = _this$props2.updateOnlyOnStateChange;

        if (!updateOnlyOnStateChange) {
          return true;
        }

        return !equals(nextProps.query, query) || !equals(nextProps.queries, queries) || (nextProps.render == null || render == null) && nextProps.render !== render || nextProps.cubejsApi !== cubejsApi || nextProps.loadSql !== loadSql || !equals(nextState, this.state) || nextProps.updateOnlyOnStateChange !== updateOnlyOnStateChange;
      }
    }, {
      key: "componentDidUpdate",
      value: function componentDidUpdate(prevProps) {
        var _this$props3 = this.props,
            query = _this$props3.query,
            queries = _this$props3.queries;

        if (!equals(prevProps.query, query)) {
          this.load(query);
        }

        if (!equals(prevProps.queries, queries)) {
          this.loadQueries(queries);
        }
      }
    }, {
      key: "cubejsApi",
      value: function cubejsApi() {
        // eslint-disable-next-line react/destructuring-assignment
        return this.props.cubejsApi || this.context && this.context.cubejsApi;
      }
    }, {
      key: "load",
      value: function load(query) {
        var _this2 = this;

        var resetResultSetOnChange = this.props.resetResultSetOnChange;
        this.setState(_objectSpread({
          isLoading: true,
          error: null,
          sqlQuery: null
        }, resetResultSetOnChange ? {
          resultSet: null
        } : {}));
        var loadSql = this.props.loadSql;
        var cubejsApi = this.cubejsApi();

        if (query && QueryRenderer.isQueryPresent(query)) {
          if (loadSql === 'only') {
            cubejsApi.sql(query, {
              mutexObj: this.mutexObj,
              mutexKey: 'sql'
            }).then(function (sqlQuery) {
              return _this2.setState({
                sqlQuery: sqlQuery,
                error: null,
                isLoading: false
              });
            })["catch"](function (error) {
              return _this2.setState(_objectSpread({}, resetResultSetOnChange ? {
                resultSet: null
              } : {}, {
                error: error,
                isLoading: false
              }));
            });
          } else if (loadSql) {
            Promise.all([cubejsApi.sql(query, {
              mutexObj: this.mutexObj,
              mutexKey: 'sql'
            }), cubejsApi.load(query, {
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
            })["catch"](function (error) {
              return _this2.setState(_objectSpread({}, resetResultSetOnChange ? {
                resultSet: null
              } : {}, {
                error: error,
                isLoading: false
              }));
            });
          } else {
            cubejsApi.load(query, {
              mutexObj: this.mutexObj,
              mutexKey: 'query'
            }).then(function (resultSet) {
              return _this2.setState({
                resultSet: resultSet,
                error: null,
                isLoading: false
              });
            })["catch"](function (error) {
              return _this2.setState(_objectSpread({}, resetResultSetOnChange ? {
                resultSet: null
              } : {}, {
                error: error,
                isLoading: false
              }));
            });
          }
        }
      }
    }, {
      key: "loadQueries",
      value: function loadQueries(queries) {
        var _this3 = this;

        var cubejsApi = this.cubejsApi();
        var resetResultSetOnChange = this.props.resetResultSetOnChange;
        this.setState(_objectSpread({
          isLoading: true
        }, resetResultSetOnChange ? {
          resultSet: null
        } : {}, {
          error: null
        }));
        var resultPromises = Promise.all(toPairs(queries).map(function (_ref3) {
          var _ref4 = _slicedToArray(_ref3, 2),
              name = _ref4[0],
              query = _ref4[1];

          return cubejsApi.load(query, {
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
        })["catch"](function (error) {
          return _this3.setState(_objectSpread({}, resetResultSetOnChange ? {
            resultSet: null
          } : {}, {
            error: error,
            isLoading: false
          }));
        });
      }
    }, {
      key: "render",
      value: function render() {
        var _this$state = this.state,
            error = _this$state.error,
            queries = _this$state.queries,
            resultSet = _this$state.resultSet,
            isLoading = _this$state.isLoading,
            sqlQuery = _this$state.sqlQuery;
        var render = this.props.render;
        var loadState = {
          error: error,
          resultSet: queries ? resultSet || {} : resultSet,
          loadingState: {
            isLoading: isLoading
          },
          sqlQuery: sqlQuery
        };

        if (render) {
          return render(loadState);
        }

        return null;
      }
    }]);

    return QueryRenderer;
  }(React__default.Component);
  QueryRenderer.contextType = CubeContext;
  QueryRenderer.propTypes = {
    render: PropTypes.func,
    cubejsApi: PropTypes.object,
    query: PropTypes.object,
    queries: PropTypes.object,
    loadSql: PropTypes.any,
    resetResultSetOnChange: PropTypes.bool,
    updateOnlyOnStateChange: PropTypes.bool
  };
  QueryRenderer.defaultProps = {
    cubejsApi: null,
    query: null,
    render: null,
    queries: null,
    loadSql: null,
    updateOnlyOnStateChange: false,
    resetResultSetOnChange: true
  };

  var QueryRendererWithTotals = function QueryRendererWithTotals(_ref) {
    var query = _ref.query,
        restProps = _objectWithoutProperties(_ref, ["query"]);

    return React__default.createElement(QueryRenderer, _extends({
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
  };

  QueryRendererWithTotals.propTypes = {
    render: PropTypes.func,
    cubejsApi: PropTypes.object.isRequired,
    query: PropTypes.object,
    queries: PropTypes.object,
    loadSql: PropTypes.any
  };
  QueryRendererWithTotals.defaultProps = {
    query: null,
    render: null,
    queries: null,
    loadSql: null
  };

  var $filter = arrayIteration.filter;



  var HAS_SPECIES_SUPPORT$3 = arrayMethodHasSpeciesSupport('filter');
  // Edge 14- issue
  var USES_TO_LENGTH$6 = arrayMethodUsesToLength('filter');

  // `Array.prototype.filter` method
  // https://tc39.github.io/ecma262/#sec-array.prototype.filter
  // with adding support of @@species
  _export({ target: 'Array', proto: true, forced: !HAS_SPECIES_SUPPORT$3 || !USES_TO_LENGTH$6 }, {
    filter: function filter(callbackfn /* , thisArg */) {
      return $filter(this, callbackfn, arguments.length > 1 ? arguments[1] : undefined);
    }
  });

  // `Symbol.asyncIterator` well-known symbol
  // https://tc39.github.io/ecma262/#sec-symbol.asynciterator
  defineWellKnownSymbol('asyncIterator');

  // `Symbol.toStringTag` well-known symbol
  // https://tc39.github.io/ecma262/#sec-symbol.tostringtag
  defineWellKnownSymbol('toStringTag');

  var $forEach$1 = arrayIteration.forEach;



  var STRICT_METHOD$5 = arrayMethodIsStrict('forEach');
  var USES_TO_LENGTH$7 = arrayMethodUsesToLength('forEach');

  // `Array.prototype.forEach` method implementation
  // https://tc39.github.io/ecma262/#sec-array.prototype.foreach
  var arrayForEach = (!STRICT_METHOD$5 || !USES_TO_LENGTH$7) ? function forEach(callbackfn /* , thisArg */) {
    return $forEach$1(this, callbackfn, arguments.length > 1 ? arguments[1] : undefined);
  } : [].forEach;

  // `Array.prototype.forEach` method
  // https://tc39.github.io/ecma262/#sec-array.prototype.foreach
  _export({ target: 'Array', proto: true, forced: [].forEach != arrayForEach }, {
    forEach: arrayForEach
  });

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

  var FAILS_ON_PRIMITIVES$1 = fails(function () { objectGetPrototypeOf(1); });

  // `Object.getPrototypeOf` method
  // https://tc39.github.io/ecma262/#sec-object.getprototypeof
  _export({ target: 'Object', stat: true, forced: FAILS_ON_PRIMITIVES$1, sham: !correctPrototypeGetter }, {
    getPrototypeOf: function getPrototypeOf(it) {
      return objectGetPrototypeOf(toObject(it));
    }
  });

  // `Object.setPrototypeOf` method
  // https://tc39.github.io/ecma262/#sec-object.setprototypeof
  _export({ target: 'Object', stat: true }, {
    setPrototypeOf: objectSetPrototypeOf
  });

  for (var COLLECTION_NAME$1 in domIterables) {
    var Collection$1 = global_1[COLLECTION_NAME$1];
    var CollectionPrototype$1 = Collection$1 && Collection$1.prototype;
    // some Chrome versions have non-configurable methods on DOMTokenList
    if (CollectionPrototype$1 && CollectionPrototype$1.forEach !== arrayForEach) try {
      createNonEnumerableProperty(CollectionPrototype$1, 'forEach', arrayForEach);
    } catch (error) {
      CollectionPrototype$1.forEach = arrayForEach;
    }
  }

  var runtime = createCommonjsModule(function (module) {
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
      var runtime = global.regeneratorRuntime;

      if (runtime) {
        {
          // If regeneratorRuntime is defined globally and we're in a module,
          // make the exports object identical to regeneratorRuntime.
          module.exports = runtime;
        } // Don't bother evaluating the rest of this file if the runtime was
        // already defined globally.


        return;
      } // Define the runtime globally (as expected by generated code) as either
      // module.exports (if we're in a module) or a new, empty object.


      runtime = global.regeneratorRuntime = module.exports;

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
  });

  var QueryBuilder =
  /*#__PURE__*/
  function (_React$Component) {
    _inherits(QueryBuilder, _React$Component);

    function QueryBuilder(props$$1) {
      var _this;

      _classCallCheck(this, QueryBuilder);

      _this = _possibleConstructorReturn(this, _getPrototypeOf(QueryBuilder).call(this, props$$1));
      _this.state = _objectSpread({
        query: props$$1.query,
        chartType: 'line'
      }, props$$1.vizState);
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
                  return this.cubejsApi().meta();

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

        function componentDidMount() {
          return _componentDidMount.apply(this, arguments);
        }

        return componentDidMount;
      }()
    }, {
      key: "componentDidUpdate",
      value: function componentDidUpdate(prevProps) {
        var _this$props = this.props,
            query = _this$props.query,
            vizState = _this$props.vizState;

        if (!equals(prevProps.query, query)) {
          // eslint-disable-next-line react/no-did-update-set-state
          this.setState({
            query: query
          });
        }

        if (!equals(prevProps.vizState, vizState)) {
          // eslint-disable-next-line react/no-did-update-set-state
          this.setState(vizState);
        }
      }
    }, {
      key: "cubejsApi",
      value: function cubejsApi() {
        var cubejsApi = this.props.cubejsApi; // eslint-disable-next-line react/destructuring-assignment

        return cubejsApi || this.context && this.context.cubejsApi;
      }
    }, {
      key: "isQueryPresent",
      value: function isQueryPresent() {
        var query = this.state.query;
        return QueryRenderer.isQueryPresent(query);
      }
    }, {
      key: "prepareRenderProps",
      value: function prepareRenderProps(queryRendererProps) {
        var _this2 = this;

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

        var toFilter = function toFilter(member) {
          return {
            dimension: member.dimension.name,
            operator: member.operator,
            values: member.values
          };
        };

        var updateMethods = function updateMethods(memberType) {
          var toQuery = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : getName;
          return {
            add: function add$$1(member) {
              var query = _this2.state.query;

              _this2.updateQuery(_defineProperty({}, memberType, (query[memberType] || []).concat(toQuery(member))));
            },
            remove: function remove$$1(member) {
              var query = _this2.state.query;
              var members = (query[memberType] || []).concat([]);
              members.splice(member.index, 1);
              return _this2.updateQuery(_defineProperty({}, memberType, members));
            },
            update: function update$$1(member, updateWith) {
              var query = _this2.state.query;
              var members = (query[memberType] || []).concat([]);
              members.splice(member.index, 1, toQuery(updateWith));
              return _this2.updateQuery(_defineProperty({}, memberType, members));
            }
          };
        };

        var granularities = [{
          name: undefined,
          title: 'w/o grouping'
        }, {
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
        var _this$state = this.state,
            meta = _this$state.meta,
            query = _this$state.query,
            chartType = _this$state.chartType;
        return _objectSpread({
          meta: meta,
          query: query,
          validatedQuery: this.validatedQuery(),
          isQueryPresent: this.isQueryPresent(),
          chartType: chartType,
          measures: (meta && query.measures || []).map(function (m, i) {
            return _objectSpread({
              index: i
            }, meta.resolveMember(m, 'measures'));
          }),
          dimensions: (meta && query.dimensions || []).map(function (m, i) {
            return _objectSpread({
              index: i
            }, meta.resolveMember(m, 'dimensions'));
          }),
          segments: (meta && query.segments || []).map(function (m, i) {
            return _objectSpread({
              index: i
            }, meta.resolveMember(m, 'segments'));
          }),
          timeDimensions: (meta && query.timeDimensions || []).map(function (m, i) {
            return _objectSpread({}, m, {
              dimension: _objectSpread({}, meta.resolveMember(m.dimension, 'dimensions'), {
                granularities: granularities
              }),
              index: i
            });
          }),
          filters: (meta && query.filters || []).map(function (m, i) {
            return _objectSpread({}, m, {
              dimension: meta.resolveMember(m.dimension, ['dimensions', 'measures']),
              operators: meta.filterOperatorsForMember(m.dimension, ['dimensions', 'measures']),
              index: i
            });
          }),
          availableMeasures: meta && meta.membersForQuery(query, 'measures') || [],
          availableDimensions: meta && meta.membersForQuery(query, 'dimensions') || [],
          availableTimeDimensions: (meta && meta.membersForQuery(query, 'dimensions') || []).filter(function (m) {
            return m.type === 'time';
          }),
          availableSegments: meta && meta.membersForQuery(query, 'segments') || [],
          updateMeasures: updateMethods('measures'),
          updateDimensions: updateMethods('dimensions'),
          updateSegments: updateMethods('segments'),
          updateTimeDimensions: updateMethods('timeDimensions', toTimeDimension),
          updateFilters: updateMethods('filters', toFilter),
          updateChartType: function updateChartType(newChartType) {
            return _this2.updateVizState({
              chartType: newChartType
            });
          }
        }, queryRendererProps);
      }
    }, {
      key: "updateQuery",
      value: function updateQuery(queryUpdate) {
        var query = this.state.query;
        this.updateVizState({
          query: _objectSpread({}, query, {}, queryUpdate)
        });
      }
    }, {
      key: "updateVizState",
      value: function updateVizState(state) {
        var _this$props2 = this.props,
            setQuery = _this$props2.setQuery,
            setVizState = _this$props2.setVizState;
        var finalState = this.applyStateChangeHeuristics(state);
        this.setState(finalState);
        finalState = _objectSpread({}, this.state, {}, finalState);

        if (setQuery) {
          setQuery(finalState.query);
        }

        if (setVizState) {
          var _finalState = finalState,
              meta = _finalState.meta,
              toSet = _objectWithoutProperties(_finalState, ["meta"]);

          setVizState(toSet);
        }
      }
    }, {
      key: "validatedQuery",
      value: function validatedQuery() {
        var query = this.state.query;
        return _objectSpread({}, query, {
          filters: (query.filters || []).filter(function (f) {
            return f.operator;
          })
        });
      }
    }, {
      key: "defaultHeuristics",
      value: function defaultHeuristics(newState) {
        var _this$state2 = this.state,
            query = _this$state2.query,
            sessionGranularity = _this$state2.sessionGranularity;
        var defaultGranularity = sessionGranularity || 'day';

        if (newState.query) {
          var oldQuery = query;
          var newQuery = newState.query;
          var meta = this.state.meta;

          if ((oldQuery.timeDimensions || []).length === 1 && (newQuery.timeDimensions || []).length === 1 && newQuery.timeDimensions[0].granularity && oldQuery.timeDimensions[0].granularity !== newQuery.timeDimensions[0].granularity) {
            newState = _objectSpread({}, newState, {
              sessionGranularity: newQuery.timeDimensions[0].granularity
            });
          }

          if ((oldQuery.measures || []).length === 0 && (newQuery.measures || []).length > 0 || (oldQuery.measures || []).length === 1 && (newQuery.measures || []).length === 1 && oldQuery.measures[0] !== newQuery.measures[0]) {
            var defaultTimeDimension = meta.defaultTimeDimensionNameFor(newQuery.measures[0]);
            newQuery = _objectSpread({}, newQuery, {
              timeDimensions: defaultTimeDimension ? [{
                dimension: defaultTimeDimension,
                granularity: defaultGranularity
              }] : []
            });
            return _objectSpread({}, newState, {
              query: newQuery,
              chartType: defaultTimeDimension ? 'line' : 'number'
            });
          }

          if ((oldQuery.dimensions || []).length === 0 && (newQuery.dimensions || []).length > 0) {
            newQuery = _objectSpread({}, newQuery, {
              timeDimensions: (newQuery.timeDimensions || []).map(function (td) {
                return _objectSpread({}, td, {
                  granularity: undefined
                });
              })
            });
            return _objectSpread({}, newState, {
              query: newQuery,
              chartType: 'table'
            });
          }

          if ((oldQuery.dimensions || []).length > 0 && (newQuery.dimensions || []).length === 0) {
            newQuery = _objectSpread({}, newQuery, {
              timeDimensions: (newQuery.timeDimensions || []).map(function (td) {
                return _objectSpread({}, td, {
                  granularity: td.granularity || defaultGranularity
                });
              })
            });
            return _objectSpread({}, newState, {
              query: newQuery,
              chartType: (newQuery.timeDimensions || []).length ? 'line' : 'number'
            });
          }

          if (((oldQuery.dimensions || []).length > 0 || (oldQuery.measures || []).length > 0) && (newQuery.dimensions || []).length === 0 && (newQuery.measures || []).length === 0) {
            newQuery = _objectSpread({}, newQuery, {
              timeDimensions: [],
              filters: []
            });
            return _objectSpread({}, newState, {
              query: newQuery,
              sessionGranularity: null
            });
          }

          return newState;
        }

        if (newState.chartType) {
          var newChartType = newState.chartType;

          if ((newChartType === 'line' || newChartType === 'area') && (query.timeDimensions || []).length === 1 && !query.timeDimensions[0].granularity) {
            var _query$timeDimensions = _slicedToArray(query.timeDimensions, 1),
                td = _query$timeDimensions[0];

            return _objectSpread({}, newState, {
              query: _objectSpread({}, query, {
                timeDimensions: [_objectSpread({}, td, {
                  granularity: defaultGranularity
                })]
              })
            });
          }

          if ((newChartType === 'pie' || newChartType === 'table' || newChartType === 'number') && (query.timeDimensions || []).length === 1 && query.timeDimensions[0].granularity) {
            var _query$timeDimensions2 = _slicedToArray(query.timeDimensions, 1),
                _td = _query$timeDimensions2[0];

            return _objectSpread({}, newState, {
              query: _objectSpread({}, query, {
                timeDimensions: [_objectSpread({}, _td, {
                  granularity: undefined
                })]
              })
            });
          }
        }

        return newState;
      }
    }, {
      key: "applyStateChangeHeuristics",
      value: function applyStateChangeHeuristics(newState) {
        var _this$props3 = this.props,
            stateChangeHeuristics = _this$props3.stateChangeHeuristics,
            disableHeuristics = _this$props3.disableHeuristics;

        if (disableHeuristics) {
          return newState;
        }

        return stateChangeHeuristics && stateChangeHeuristics(this.state, newState) || this.defaultHeuristics(newState);
      }
    }, {
      key: "render",
      value: function render() {
        var _this3 = this;

        var _this$props4 = this.props,
            cubejsApi = _this$props4.cubejsApi,
            _render = _this$props4.render,
            wrapWithQueryRenderer = _this$props4.wrapWithQueryRenderer;

        if (wrapWithQueryRenderer) {
          return React__default.createElement(QueryRenderer, {
            query: this.validatedQuery(),
            cubejsApi: cubejsApi,
            render: function render(queryRendererProps) {
              if (_render) {
                return _render(_this3.prepareRenderProps(queryRendererProps));
              }

              return null;
            }
          });
        } else {
          if (_render) {
            return _render(this.prepareRenderProps());
          }

          return null;
        }
      }
    }]);

    return QueryBuilder;
  }(React__default.Component);
  QueryBuilder.contextType = CubeContext;
  QueryBuilder.propTypes = {
    render: PropTypes.func,
    stateChangeHeuristics: PropTypes.func,
    setQuery: PropTypes.func,
    setVizState: PropTypes.func,
    cubejsApi: PropTypes.object,
    disableHeuristics: PropTypes.bool,
    wrapWithQueryRenderer: PropTypes.bool,
    query: PropTypes.object,
    vizState: PropTypes.object
  };
  QueryBuilder.defaultProps = {
    cubejsApi: null,
    query: {},
    setQuery: null,
    setVizState: null,
    stateChangeHeuristics: null,
    disableHeuristics: false,
    render: null,
    wrapWithQueryRenderer: true,
    vizState: {}
  };

  var CubeProvider = function CubeProvider(_ref) {
    var cubejsApi = _ref.cubejsApi,
        children = _ref.children;
    return React__default.createElement(CubeContext.Provider, {
      value: {
        cubejsApi: cubejsApi
      }
    }, children);
  };

  CubeProvider.propTypes = {
    cubejsApi: PropTypes.object.isRequired,
    children: PropTypes.any.isRequired
  };

  function useDeepCompareMemoize(value) {
    var ref = React.useRef([]);

    if (!equals(value, ref.current)) {
      ref.current = value;
    }

    return ref.current;
  }

  var useCubeQuery = (function (query) {
    var options = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};

    var _useState = React.useState({}),
        _useState2 = _slicedToArray(_useState, 1),
        mutexObj = _useState2[0];

    var _useState3 = React.useState(null),
        _useState4 = _slicedToArray(_useState3, 2),
        currentQuery = _useState4[0],
        setCurrentQuery = _useState4[1];

    var _useState5 = React.useState(false),
        _useState6 = _slicedToArray(_useState5, 2),
        isLoading = _useState6[0],
        setLoading = _useState6[1];

    var _useState7 = React.useState(null),
        _useState8 = _slicedToArray(_useState7, 2),
        resultSet = _useState8[0],
        setResultSet = _useState8[1];

    var _useState9 = React.useState(null),
        _useState10 = _slicedToArray(_useState9, 2),
        error = _useState10[0],
        setError = _useState10[1];

    var context = React.useContext(CubeContext);
    var resetResultSetOnChange = options.resetResultSetOnChange;
    var subscribeRequest = null;
    React.useEffect(function () {
      function loadQuery() {
        return _loadQuery.apply(this, arguments);
      }

      function _loadQuery() {
        _loadQuery = _asyncToGenerator(
        /*#__PURE__*/
        regeneratorRuntime.mark(function _callee() {
          var cubejsApi;
          return regeneratorRuntime.wrap(function _callee$(_context) {
            while (1) {
              switch (_context.prev = _context.next) {
                case 0:
                  if (!(query && isQueryPresent(query))) {
                    _context.next = 25;
                    break;
                  }

                  if (!equals(currentQuery, query)) {
                    if (resetResultSetOnChange == null || resetResultSetOnChange) {
                      setResultSet(null);
                    }

                    setError(null);
                    setCurrentQuery(query);
                  }

                  setLoading(true);
                  _context.prev = 3;

                  if (!subscribeRequest) {
                    _context.next = 8;
                    break;
                  }

                  _context.next = 7;
                  return subscribeRequest.unsubscribe();

                case 7:
                  subscribeRequest = null;

                case 8:
                  cubejsApi = options.cubejsApi || context && context.cubejsApi;

                  if (!options.subscribe) {
                    _context.next = 13;
                    break;
                  }

                  subscribeRequest = cubejsApi.subscribe(query, {
                    mutexObj: mutexObj,
                    mutexKey: 'query'
                  }, function (e, result) {
                    setLoading(false);

                    if (e) {
                      setError(e);
                    } else {
                      setResultSet(result);
                    }
                  });
                  _context.next = 19;
                  break;

                case 13:
                  _context.t0 = setResultSet;
                  _context.next = 16;
                  return cubejsApi.load(query, {
                    mutexObj: mutexObj,
                    mutexKey: 'query'
                  });

                case 16:
                  _context.t1 = _context.sent;
                  (0, _context.t0)(_context.t1);
                  setLoading(false);

                case 19:
                  _context.next = 25;
                  break;

                case 21:
                  _context.prev = 21;
                  _context.t2 = _context["catch"](3);
                  setError(_context.t2);
                  setLoading(false);

                case 25:
                case "end":
                  return _context.stop();
              }
            }
          }, _callee, null, [[3, 21]]);
        }));
        return _loadQuery.apply(this, arguments);
      }

      loadQuery();
      return function () {
        if (subscribeRequest) {
          subscribeRequest.unsubscribe();
          subscribeRequest = null;
        }
      };
    }, useDeepCompareMemoize([query, options, context]));
    return {
      isLoading: isLoading,
      resultSet: resultSet,
      error: error
    };
  });

  exports.QueryRenderer = QueryRenderer;
  exports.QueryRendererWithTotals = QueryRendererWithTotals;
  exports.QueryBuilder = QueryBuilder;
  exports.isQueryPresent = isQueryPresent;
  exports.CubeContext = CubeContext;
  exports.CubeProvider = CubeProvider;
  exports.useCubeQuery = useCubeQuery;

  Object.defineProperty(exports, '__esModule', { value: true });

}));
