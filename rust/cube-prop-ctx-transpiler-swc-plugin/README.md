# JS Transpiler plugin for SWC compiler

This plugin is used during compilation phases of Cube data models. Cube uses SWC for compiling 
and transpiling JS-based model definitions.

SWC is written in Rust and is ~20-70x faster then Babel. Plugins for SWC are written in Rust and
then compiled to WebAssembly target (wasm-wasi32).

Useful links:
* [SWC](https://swc.rs)
* [How to implement a SWC Plugin](https://swc.rs/docs/plugin/ecmascript/getting-started)
* [A comprehensive list of possible visitor methods](https://rustdoc.swc.rs/swc_ecma_visit/trait.VisitMut.html)
