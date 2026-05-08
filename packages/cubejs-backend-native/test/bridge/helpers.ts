import { loadNative } from '../../js';

const native = loadNative();

// Bridge test endpoints are only present when the native module was built with
// `--features bridge-test-harness` (e.g. `yarn native:build-debug-bridge-tests`).
// Test suites should gate themselves on this flag rather than calling the
// helpers blindly, so a regular debug build doesn't blow up the unit run.
export const bridgeHarnessAvailable: boolean =
  typeof native.__testBridgeCompileMemberSql === 'function';

export type SqlTemplate = string | string[];

export interface FilterParamsItem {
  cube_name: string;
  name: string;
  // String column → string. Callback column → JS function (call to inspect).
  column: string | Function;
}

export interface FilterGroupItem {
  filter_params: FilterParamsItem[];
}

export interface SqlTemplateArgs {
  symbol_paths: string[][];
  filter_params: FilterParamsItem[];
  filter_groups: FilterGroupItem[];
  security_context: { values: string[] };
}

export interface CompiledMemberSql {
  template: SqlTemplate;
  args: SqlTemplateArgs;
}

export function compileMemberSql(
  fn: Function,
  securityContext: object = {}
): CompiledMemberSql {
  if (!bridgeHarnessAvailable) {
    throw new Error(
      'Bridge test harness is not built. Rebuild with `yarn native:build-debug-bridge-tests`.'
    );
  }
  return native.__testBridgeCompileMemberSql(fn, securityContext);
}

export function parseArgsNames(fn: Function): string[] {
  return native.__testBridgeParseArgsNames(fn);
}

export function invokeFilterParamsCallback(
  fn: Function,
  args: string[]
): string {
  return native.__testBridgeInvokeFilterParamsCallback(fn, args);
}

export type BridgeFieldKind = 'field' | 'call' | 'static';

export interface BridgeFieldMeta {
  name: string;
  jsName: string;
  kind: BridgeFieldKind;
  optional: boolean;
  vec: boolean;
}

export function listBridgeFields(name: string): BridgeFieldMeta[] {
  if (!bridgeHarnessAvailable) {
    throw new Error(
      'Bridge test harness is not built. Rebuild with `yarn native:build-debug-bridge-tests`.'
    );
  }
  return native.__testBridgeListFields(name);
}

export function parseBridge(name: string, obj: object): void {
  if (!bridgeHarnessAvailable) {
    throw new Error(
      'Bridge test harness is not built. Rebuild with `yarn native:build-debug-bridge-tests`.'
    );
  }
  native.__testBridgeParse(name, obj);
}

export function fieldNames(meta: BridgeFieldMeta[]): string[] {
  return meta.map((m) => m.name).sort();
}

/**
 * Builds a stub fixture from bridge field meta.
 *
 * Override keys MUST match `BridgeFieldMeta.jsName` (i.e. the JS-side name
 * after `#[serde(rename)]` — not the Rust ident). An unknown key is treated
 * as a typo and throws, so a misspelled `primaryKey` does not silently
 * cascade into a confusing serde error.
 *
 * Required trait fields default to `() => null` — that satisfies the
 * `has_field` check try_new performs (both for `field` getters and for
 * `call` methods). Required static fields default to `null`; serde rejects
 * `null` for non-Option primitives, so callers must supply a typed override
 * for those (e.g. `{ name: '' }` for `cubeDefinition`).
 */
export function buildFixture(
  meta: BridgeFieldMeta[],
  overrides: Record<string, unknown> = {}
): Record<string, unknown> {
  const known = new Set(meta.map((m) => m.jsName));
  for (const k of Object.keys(overrides)) {
    if (!known.has(k)) {
      throw new Error(
        `buildFixture: override key '${k}' is not a known field of this bridge ` +
          `(known jsName keys: ${[...known].sort().join(', ') || '(none)'})`
      );
    }
  }
  const fixture: Record<string, unknown> = {};
  for (const field of meta) {
    if (Object.prototype.hasOwnProperty.call(overrides, field.jsName)) {
      fixture[field.jsName] = overrides[field.jsName];
    } else if (!field.optional) {
      fixture[field.jsName] = field.kind === 'static' ? null : () => null;
    }
  }
  return fixture;
}
