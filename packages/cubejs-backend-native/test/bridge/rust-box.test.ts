import {
  bridgeHarnessAvailable,
  createRustBoxProbe,
  createRustBoxProbeAlt,
  unwrapRustBoxProbe,
} from './helpers';

const describeBridge = bridgeHarnessAvailable ? describe : describe.skip;

describeBridge('bridge: NativeRustHandle / JsBox roundtrip', () => {
  describe('happy path', () => {
    it('returns the same value and label after a roundtrip through JS', () => {
      const handle = createRustBoxProbe(42.5, 'hello');
      const view = unwrapRustBoxProbe(handle);
      expect(view.value).toBe(42.5);
      expect(view.label).toBe('hello');
      expect(view.type_name).toContain('RustBoxProbe');
    });

    it('keeps independent state across multiple boxes', () => {
      const a = createRustBoxProbe(1, 'a');
      const b = createRustBoxProbe(2, 'b');
      expect(unwrapRustBoxProbe(a).value).toBe(1);
      expect(unwrapRustBoxProbe(b).value).toBe(2);
      expect(unwrapRustBoxProbe(a).label).toBe('a');
      expect(unwrapRustBoxProbe(b).label).toBe('b');
    });

    it('survives many unwrap calls on the same handle', () => {
      const handle = createRustBoxProbe(7, 'persist');
      for (let i = 0; i < 100; i += 1) {
        const view = unwrapRustBoxProbe(handle);
        expect(view.value).toBe(7);
        expect(view.label).toBe('persist');
      }
    });
  });

  describe('non-RustBox arguments', () => {
    // RootHolder dispatch path: `is_a::<JsBox<NativeRustHandle>>` is false,
    // value gets classified as some other JS type (or null/undefined),
    // `.into_rust_box()` then surfaces the "Object is not a Rust box" error.
    const message = /Object is not a Rust box/;

    it('rejects a plain object', () => {
      expect(() => unwrapRustBoxProbe({})).toThrow(message);
    });

    it('rejects a string', () => {
      expect(() => unwrapRustBoxProbe('not a box')).toThrow(message);
    });

    it('rejects a number', () => {
      expect(() => unwrapRustBoxProbe(123)).toThrow(message);
    });

    it('rejects null', () => {
      expect(() => unwrapRustBoxProbe(null)).toThrow(message);
    });

    it('rejects undefined', () => {
      expect(() => unwrapRustBoxProbe(undefined)).toThrow(message);
    });
  });

  describe('type-mismatch on downcast', () => {
    // RootHolder accepts the value (it really is JsBox<NativeRustHandle>),
    // but the inner type tag does not match: NativeRustHandle::downcast
    // surfaces a message naming both the stored and the requested type.
    // The format is the public diagnostic contract for cache misses and
    // similar lookups, so we pin it down with a regex.
    it('reports source and target type names in the error', () => {
      const altHandle = createRustBoxProbeAlt('payload');
      expect(() => unwrapRustBoxProbe(altHandle)).toThrow(
        /cannot downcast.*RustBoxProbeAlt.*RustBoxProbe(?!Alt)/
      );
    });
  });
});
