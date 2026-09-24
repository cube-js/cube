# Data model compile-time benchmark

Measures how long `DataSchemaCompiler.compile()` takes on data models of 10k to 100k cube members,
and where that time goes.

```bash
yarn tsc                    # from the repo root; the benchmark runs from dist/
cd packages/cubejs-schema-compiler
yarn bench:compile          # 10k, 20k, 50k, 100k; Jinja model; Babel and native transpilers
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

That base model (10k members, 100 cubes, 10 views) is multiplied by the dynamic model:
each of the 100 files loops `COMPILE_CONTEXT.copies` times in Jinja, so copy `i` of `orders`
joins copy `i` of `customers`. 20k members is 2 copies, 100k is 10.

`--formats` also takes `yaml` and `js`: the same cubes written out statically, one file per cube.
Comparing them separates the cost of Jinja, of YAML, and of the JS transpilers.

## Options

| Option | Default | |
| --- | --- | --- |
| `--scales` | `10k,20k,50k,100k` | Cube member counts, multiples of 10k |
| `--formats` | `jinja` | `jinja`, `yaml`, `js` |
| `--transpilers` | `babel,native` | `CUBEJS_TRANSPILATION_NATIVE` off / on |
| `--repeat` | `1` | Runs per configuration; the table shows the median |
| `--no-views` | | Leave the views out |
| `--cpu-prof` | | Write `.cpuprofile` files per run and print the top functions by self time |
| `--prof-dir` | `compile-bench-profiles` | Where `--cpu-prof` writes |
| `--out` | | Append each run as a JSON line |

Every run compiles in a fresh process. The native transpiler needs `@cubejs-backend/native`
built from the same revision (`transpileJs` and `transpileYaml`).

## Reading the output

- `transpile`: time before each stage's files are evaluated. Jinja rendering and YAML to JS in
  the first stage; the JS transpilers in every stage.
- `evaluate`: running the transpiled files in the VM context.
- `compilers`: the compiler passes (`CubeSymbols`, `CubeValidator`, `CubeEvaluator`,
  `JoinGraph`, `CubeToMetaTransformer`, ...), listed per stage for the largest scale. Stages are
  numbered in the order they run; the view stage is skipped when no view needs it.

`profile-summary.js <dir>` re-prints a profile summary. Open the `.cpuprofile` files in Chrome
DevTools for call trees. With the Babel transpiler, each worker thread writes its own profile.
