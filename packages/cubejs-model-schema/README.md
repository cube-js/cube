# @cubejs-backend/model-schema

A JSON Schema (draft-07) of the YAML data model language, generated from the Joi schemas that
`CubeValidator` in `@cubejs-backend/schema-compiler` validates cubes, views and view groups with.
Editors can use it for completion, hover and validation of model files.

Published to npm with every Core release, at Core's version. `index.json` names the manifest of
that release and its SHA-256, which stays the same across releases that don't change the manifest:

```json
{
  "manifestFormatVersion": 1,
  "latest": "1.8.22",
  "manifests": { "1.8.22": "manifests/1.8.22.json" },
  "sha256": { "1.8.22": "<hex digest of manifests/1.8.22.json>" }
}
```

`require('@cubejs-backend/model-schema')` returns the latest manifest.

## Generating

```bash
yarn build      # tsc, then writes manifests/<version>.json, index.json and index.js
yarn unit       # the snapshot and the differential test, after a build
```

The generated files are not committed. The test snapshot is the reviewed copy of the manifest.

## Where the schema is looser than Core

Not every Joi rule has a JSON Schema equivalent: functions passed to `.custom()`, `condition(fn)`
branches, flagged regexes, limits on a number written as a string. A code field such as `sql` or
`time_dimension` takes a string, a list or an object, since Joi checks only that the YAML compiler
made it a function; whether a list fits the field is found when Core evaluates the model. The schema
accepts such values; how many places of each kind there are is in the `stats` snapshot. The schema
has no custom keywords.

The schema must not reject a model Core accepts. `test/differential.test.ts` checks this on the YAML
models in the fixture directories it lists (`CORPUS`), each compiled by Core and validated with the
schema; the few the schema accepts and Core rejects are listed there with the reason.
`test/converter.test.ts` does the same for Joi forms Core doesn't use yet.

## Definition names

Each entity (`cube`, `view`, `view_group`, `measure`, `dimension`, `segment`, `pre_aggregation`,
`hierarchy`, `join`, `folder`, `access_policy`) and each `Joi.link` target has a definition named
after it, so a `$ref` to it can be relied on. The other definitions are parts the manifest repeats;
their names and their set change with the schema.

## Changing `CubeValidator`

A change to the schemas in `CubeValidator` changes the manifest, so the snapshot test fails until it
is updated: build this package, run `yarn unit -u`, and review the manifest diff with the Core
change. Editors show a key's `.description()`, and a test fails on a key without one. A key Core
accepts but editors shouldn't offer, such as an old spelling, takes `.meta({ hidden: true })`
and keeps its type and `when` constraints. Only a key the compiler always overwrites after reading
the file takes `.meta({ setByCompiler: true })`: it accepts any value, unless it is a function the
compiler adds, which is left out. A change inside a JS function there doesn't show up in the
manifest. A new model fixture in one of the `CORPUS` directories is picked up by the differential
test.
