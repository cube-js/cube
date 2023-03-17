# Overview

Rust implementation of HyperLogLog directly ported from the Java code in [AirLift](https://github.com/airlift/airlift/tree/master/stats/src/main/java/io/airlift/stats/cardinality).
Based on commit `736098d96c8e7f9200ceb75438d85220def88d15`.

This library allows to directly read the sketches produced by `AirLift`.
