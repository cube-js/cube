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

export function listBridgeNames(): string[] {
  if (!bridgeHarnessAvailable) {
    throw new Error(
      'Bridge test harness is not built. Rebuild with `yarn native:build-debug-bridge-tests`.'
    );
  }
  return native.__testBridgeListBridgeNames();
}

export function parseBridge(name: string, obj: unknown): void {
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

export type InvokeStatus =
  | { status: 'ok' }
  | { status: 'error'; message: string }
  | { status: 'skipped'; reason: string };

export type InvokeResult = Record<string, InvokeStatus>;

export function invokeBridge(name: string, fixture: unknown): InvokeResult {
  if (!bridgeHarnessAvailable) {
    throw new Error(
      'Bridge test harness is not built. Rebuild with `yarn native:build-debug-bridge-tests`.'
    );
  }
  return native.__testBridgeInvoke(name, fixture);
}

/**
 * Asserts every recorded invocation is `ok` or `skipped`. Errors surface
 * the offending field, the Rust-side message, and the kind of failure so
 * they read naturally in CI logs. Skipped entries are allowed because some
 * call-methods take Rust-only argument types (e.g. `Rc<dyn MemberSql>`)
 * that have no auto-default.
 */
export function expectAllInvocationsOk(result: InvokeResult): void {
  const failures: string[] = [];
  for (const [field, entry] of Object.entries(result)) {
    if (entry.status === 'error') {
      failures.push(`${field}: error: ${entry.message}`);
    }
  }
  if (failures.length > 0) {
    throw new Error(
      `Bridge invocation failed for ${failures.length} field(s):\n  ${failures.join('\n  ')}`
    );
  }
}

export interface RustBoxProbeView {
  value: number;
  label: string;
  type_name: string;
}

export function createRustBoxProbe(value: number, label: string): unknown {
  return native.__testBridgeRustBoxCreate(value, label);
}

export function createRustBoxProbeAlt(note: string): unknown {
  return native.__testBridgeRustBoxCreateAlt(note);
}

export function unwrapRustBoxProbe(handle: unknown): RustBoxProbeView {
  return native.__testBridgeRustBoxUnwrap(handle);
}

export interface ModelCubeView {
  name: string;
  is_view: boolean;
  measure_count: number;
  dimension_count: number;
  segment_count: number;
  hierarchy_count: number;
  join_count: number;
  pre_aggregation_count: number;
  access_policy_count: number;
}

export interface ModelView {
  cubes: ModelCubeView[];
}

export function prepareModelRaw(schemaSource: unknown): unknown {
  return native.prepareModel(schemaSource);
}

export function describeModel(handle: unknown): ModelView {
  return native.__testBridgeModelDescribe(handle);
}
