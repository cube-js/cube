# Overview

Rust implementation of HyperLogLog++ directly ported from the Java code in [ZetaSketch](https://github.com/google/zetasketch).
Based on commit `a2f2692fae8cf61103330f9f70e696c4ba8b94b0`.

This library allows to directly interoperate with sketches produced by `ZetaSketch`.
Only portion of the code is ported. In particular, we currently support:
  - reading and writing sketches in the binary proto format,
  - computing set cardinality estimates,
  - merging sketches.

The major unsupported bits are:
  - adding values to the sketches,
  - mixing sketches of different precisions.
