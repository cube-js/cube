/* eslint-disable no-use-before-define */

/**
 * Stateless compiler for a member's user-written `sql` function.
 *
 * It invokes the function with recording proxies (the `{CUBE}`,
 * `FILTER_PARAMS`, `FILTER_GROUP`, `SECURITY_CONTEXT`, `SQL_UTILS` arguments the
 * data model expects) and returns the produced SQL template together with the
 * dependencies the function touched, as plain data:
 *
 *   {
 *     template: string | string[],
 *     symbolPaths: string[][],                       // {arg:N}
 *     filterParams: [{ cube_name, name, column }],   // {fp:N}, column = fn|string
 *     filterGroups: [{ filterParams: [...] }],       // {fg:N}
 *     securityContextValues: string[]                // {sv:N}
 *   }
 *
 * Member references are returned as recorded paths — the caller resolves them
 * to symbols. FILTER_PARAMS column callbacks are deferred (returned as the raw
 * JS function for the caller to invoke at render time); SECURITY_CONTEXT is
 * resolved eagerly here against the provided context.
 *
 * The module holds no planner state — `securityContext` and `sqlUtils` are
 * passed in — so it can be unit-tested in isolation.
 */

const ARG_PREFIX = 'arg';
const FILTER_PARAM_PREFIX = 'fp';
const FILTER_GROUP_PREFIX = 'fg';
const SECURITY_VALUE_PREFIX = 'sv';

function placeholder(prefix, index) {
  return `{${prefix}:${index}}`;
}

// Returns the index of an equal path if it already exists, otherwise appends
// and returns the new index. Paths are short arrays of strings, compared by
// value, so repeated references collapse to a single placeholder.
function uniqueInsertPath(paths, path) {
  const key = JSON.stringify(path);
  for (let i = 0; i < paths.length; i++) {
    if (JSON.stringify(paths[i]) === key) {
      return i;
    }
  }
  paths.push(path);
  return paths.length - 1;
}

function uniqueInsertString(values, value) {
  const i = values.indexOf(value);
  if (i !== -1) {
    return i;
  }
  values.push(value);
  return values.length - 1;
}

// ---- member reference (`{CUBE}` / nested paths / `.sql`) --------------------

function memberReferenceProxy(path, state) {
  return new Proxy({}, {
    get(_target, prop) {
      if (typeof prop !== 'string') {
        // Symbol access (e.g. Symbol.toPrimitive) — return undefined so JS
        // falls back to toString/valueOf for string coercion.
        return undefined;
      }
      if (prop === 'sql') {
        const index = uniqueInsertPath(state.symbolPaths, [...path, '__sql_fn']);
        const ph = placeholder(ARG_PREFIX, index);
        return () => ph;
      }
      if (prop === 'toString' || prop === 'valueOf') {
        const index = uniqueInsertPath(state.symbolPaths, path);
        const ph = placeholder(ARG_PREFIX, index);
        return () => ph;
      }
      return memberReferenceProxy([...path, prop], state);
    },
  });
}

// ---- FILTER_PARAMS / FILTER_GROUP ------------------------------------------

function filterParamsItemProxy(cubeName, name, state) {
  return {
    filter(column) {
      const item = { cube_name: cubeName, name, column };
      const toString = () => {
        const index = state.filterParams.length;
        state.filterParams.push(item);
        return placeholder(FILTER_PARAM_PREFIX, index);
      };
      // `__member` lets FILTER_GROUP recover the item; `toString` records and
      // yields the {fp:N} placeholder on coercion.
      return { __member: item, toString };
    },
  };
}

function filterParamsProxy(state) {
  return new Proxy({}, {
    get(_t, cubeName) {
      return new Proxy({}, {
        get(_t2, memberName) {
          return filterParamsItemProxy(cubeName, memberName, state);
        },
      });
    },
  });
}

function filterGroupFn(state) {
  return (...args) => {
    const filterParams = args.map(arg => {
      if (!arg || typeof arg.__member === 'undefined') {
        throw new Error('FILTER_GROUP expects FILTER_PARAMS args to be passed.');
      }
      return arg.__member;
    });
    const index = state.filterGroups.length;
    state.filterGroups.push({ filterParams });
    return placeholder(FILTER_GROUP_PREFIX, index);
  };
}

// ---- SECURITY_CONTEXT ------------------------------------------------------

function coerceScalarToString(value) {
  if (typeof value === 'string') return value;
  if (typeof value === 'number') return `${value}`;
  if (typeof value === 'boolean') return `${value}`;
  throw new Error('Invalid param for security context');
}

// Coercion used by `.filter()` — falsy scalars collapse to "no value".
function coerceFilterValue(value) {
  if (value === undefined || value === null) return { kind: 'none' };
  if (Array.isArray(value)) {
    return { kind: 'vec', values: value.map(coerceScalarToString) };
  }
  if (typeof value === 'string') {
    return value === '' ? { kind: 'none' } : { kind: 'string', value };
  }
  if (typeof value === 'number') {
    return value === 0 || Number.isNaN(value) ? { kind: 'none' } : { kind: 'string', value: `${value}` };
  }
  if (typeof value === 'boolean') {
    return value ? { kind: 'string', value: 'true' } : { kind: 'none' };
  }
  throw new Error('Invalid param for security context');
}

// Coercion used by toString/valueOf — keeps all non-null values.
function coerceToStringValue(value) {
  if (value === undefined || value === null) return null;
  if (Array.isArray(value)) return value.map(coerceScalarToString);
  if (typeof value === 'string') return [value];
  if (typeof value === 'number') return [`${value}`];
  if (typeof value === 'boolean') return [`${value}`];
  return null;
}

function recordSecurityValue(value, state) {
  return placeholder(SECURITY_VALUE_PREFIX, uniqueInsertString(state.securityContextValues, value));
}

function securityFilterFn(value, required, state) {
  const param = coerceFilterValue(value);
  return (column) => {
    if (param.kind === 'string') {
      const ph = recordSecurityValue(param.value, state);
      if (typeof column === 'function') return column(ph);
      if (typeof column === 'string') return `${column} = ${ph}`;
      return '';
    }
    if (param.kind === 'vec') {
      if (param.values.length === 0) {
        if (typeof column === 'function') return column([]);
        return '1 = 0';
      }
      const phs = param.values.map(v => recordSecurityValue(v, state));
      if (typeof column === 'function') return column(phs);
      if (typeof column === 'string') return `${column} IN (${phs.join(', ')})`;
      return '';
    }
    // none
    if (required) {
      throw new Error(`Filter for ${column} is required`);
    }
    return '1 = 1';
  };
}

function securityToStringFn(value, state) {
  const values = coerceToStringValue(value);
  return () => {
    if (values === null) return '';
    return values.map(v => recordSecurityValue(v, state)).join(',');
  };
}

function securityContextProxy(value, state) {
  return new Proxy({}, {
    get(_t, prop) {
      if (typeof prop !== 'string') return undefined;
      // Methods coerce the current value lazily — only on access, so reading a
      // nested object property does not attempt to coerce the object itself.
      if (prop === 'filter') return securityFilterFn(value, false, state);
      if (prop === 'requiredFilter') return securityFilterFn(value, true, state);
      if (prop === 'unsafeValue') return () => value;
      if (prop === 'toString' || prop === 'valueOf') return securityToStringFn(value, state);
      const propertyValue = value != null && typeof value === 'object' ? value[prop] : undefined;
      return securityContextProxy(propertyValue, state);
    },
  });
}

// ---- entry point -----------------------------------------------------------

function buildArg(argName, state, securityContext, sqlUtils) {
  if (argName === 'FILTER_PARAMS') return filterParamsProxy(state);
  if (argName === 'FILTER_GROUP') return filterGroupFn(state);
  if (argName === 'SECURITY_CONTEXT' || argName === 'security_context' || argName === 'securityContext') {
    return securityContextProxy(securityContext, state);
  }
  if (argName === 'SQL_UTILS') return sqlUtils;
  return memberReferenceProxy([argName], state);
}

function parseTemplateResult(result) {
  if (Array.isArray(result)) {
    return result.map(r => `${r}`);
  }
  if (result === null || result === undefined) {
    return '';
  }
  return `${result}`;
}

/**
 * @param {Function} sqlFn the member's `sql` function from the data model
 * @param {string[]} argNames its argument names (CUBE, FILTER_PARAMS, ...)
 * @param {object} securityContext the request security context object
 * @param {object} sqlUtils the SQL_UTILS object passed through to the template
 */
function compileMemberSql(sqlFn, argNames, securityContext, sqlUtils) {
  const state = {
    symbolPaths: [],
    filterParams: [],
    filterGroups: [],
    securityContextValues: [],
  };

  const args = argNames.map(name => buildArg(name, state, securityContext, sqlUtils));
  const template = parseTemplateResult(sqlFn(...args));

  return {
    template,
    symbolPaths: state.symbolPaths,
    filterParams: state.filterParams,
    filterGroups: state.filterGroups,
    securityContextValues: state.securityContextValues,
  };
}

exports.compileMemberSql = compileMemberSql;
exports.uniqueInsertPath = uniqueInsertPath;
exports.uniqueInsertString = uniqueInsertString;
