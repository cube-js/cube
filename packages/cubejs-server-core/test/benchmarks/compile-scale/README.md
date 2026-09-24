# Data model compile-time benchmark

Measures how long a data model of 10k to 100k cube members takes to compile, and to recompile
when its schema version changes, and where that time goes.

```bash
yarn tsc                    # from the repo root; the benchmark runs from dist/
cd packages/cubejs-server-core
yarn bench:compile          # 10k, 20k, 50k, 100k; Jinja model
```

## The model

`model-generator.ts` builds 100 distinct cubes: 10 business domains (ecommerce, marketing,
finance, saas, support, hr, logistics, manufacturing, iot, media) with 10 entities each. Every
cube has exactly 100 members:

- 60 dimensions: primary key, foreign keys, time dimensions (custom granularities on fact
  cubes), booleans, numbers, strings, derived dimensions that reference other members, a `case`
  dimension, and dimensions that reference joined cubes
- 35 measures: `count`, `count_distinct` over foreign keys, `sum`/`avg`/`min`/`max`, filtered
  measures, calculated measures that reference other measures, rolling windows
- 5 segments

Each entity joins the entities it references (`many_to_one`). The first entity of each domain is
its fact table and carries a daily rollup and the domain's view, which pulls the fact cube and
a dozen members of each cube it joins.

Half of every cube's members (30 dimensions, 17 measures, 3 segments) are not in the model
files. They are loaded at compile time from a member API: a JSON file standing in for a remote
endpoint or a manifest, which the model reads on every compile and maps onto each cube copy.

The base model (10k members, 100 cubes, 10 views) is multiplied into copies: copy `i` of
`orders` joins copy `i` of `customers`. 20k members is 2 copies, 100k is 10.

`--formats`:

- `jinja` (default): 100 YAML files, each looping `COMPILE_CONTEXT.securityContext.copies`
  times over its cube. `globals.py` serves the dynamic members to the template.
- `js`: one file per cube copy; the dynamic members are spread in from `dynamic_members.js`,
  which builds their `sql` functions.
- `yaml`: every member written out statically, one file per cube copy, as a reference point.

## What a run does

Each run compiles in a fresh process, through `CompilerApi` the way `CubejsServerCore` drives
it: the compile context is the security context `{ copies, schemaVersion }`, and
`schemaVersion` is `({ securityContext }) => securityContext.schemaVersion`.

1. **cold**: the first `getCompilers()`, at version `v1`.
2. **same ver**: `getCompilers()` again, which should return the compiled model.
3. **recompile**: the member API moves to revision 2, the request's security context moves to
   `v2`, and `getCompilers()` recompiles with the compile caches the first compile left behind.
   **new members** says whether the recompiled model carries the revision 2 members.

After each compile the run plans a query over dynamic members of a fact cube and fails if the
SQL still has unresolved `{...}` references.

## Options

| Option | Default | |
| --- | --- | --- |
| `--scales` | `10k,20k,50k,100k` | Cube member counts, multiples of 10k |
| `--formats` | `jinja` | `jinja`, `js`, `yaml` |
| `--repeat` | `1` | Runs per configuration; the table shows the median |
| `--no-views` | | Leave the views out |
| `--compiler-cache-size` | server default (250) | `compilerCacheSize`: compiled scripts, YAML and Jinja output kept between compiles |
| `--cpu-prof` | | Write `.cpuprofile` files per run and print the top functions by self time |
| `--prof-dir` | `compile-bench-profiles` | Where `--cpu-prof` writes |
| `--out` | | Append each run as a JSON line |

Transpilation is always native (`CUBEJS_TRANSPILATION_NATIVE=true`), as Cube Cloud runs it. The
native transpiler (`transpileJs`, `transpileYaml`) is not part of the open source
`@cubejs-backend/native` build; it ships with the Cube Cloud runtime's native module, which has
to be built and copied over `packages/cubejs-backend-native/index.node`.

## Reading the output

- `transpile`: time before each stage's files are evaluated. Jinja rendering and YAML to JS in
  the first stage; the JS transpilers in every stage.
- `evaluate`: running the transpiled files in the VM context, which includes loading dynamic
  members in the JS model.
- `compilers`: the compiler passes (`CubeSymbols`, `CubeValidator`, `CubeEvaluator`,
  `JoinGraph`, `CubeToMetaTransformer`, ...), listed per stage for the largest scale. Stages are
  numbered in the order they run; the view stage is skipped when no view needs it.

`profile-summary.js <dir>` re-prints a profile summary. Open the `.cpuprofile` files in Chrome
DevTools for call trees.
