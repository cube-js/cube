// Runs every YAML model in the Core repo through Core's compiler and through ajv with the generated
// manifest, and fails where they disagree. Core's verdict on each cube, view and view group is the
// one CubeValidator returns during a real compile, so it sees the real YAML transpile.
import fs from 'fs';
import path from 'path';
import Ajv from 'ajv';
import YAML from 'js-yaml';
import { describeModelSchemas, prepareCompiler } from '@cubejs-backend/schema-compiler';
import { CubeValidator } from '@cubejs-backend/schema-compiler/dist/src/compiler/CubeValidator';

import { modelSchemaFromJoi } from '../src/converter';

const REPO = path.resolve(__dirname, '../../../..');

const CORPUS = [
  'packages/cubejs-schema-compiler/test/unit/fixtures',
  'packages/cubejs-testing/birdbox-fixtures',
  'packages/cubejs-backend-native/test/fixtures',
  'rust/cube/cubesqlplanner/cubesqlplanner/src/test_fixtures/schemas/yaml_files',
  'packages/cubejs-model-schema/test/fixtures',
];

const COLLECTIONS = { cubes: 'cube', views: 'view', view_groups: 'view group' } as const;

type Collection = keyof typeof COLLECTIONS;

const PLANNER = 'rust/cube/cubesqlplanner/cubesqlplanner/src/test_fixtures/schemas/yaml_files';

// Objects Core rejects and the manifest accepts, because the rule that rejects them is JS only.
const LOOSER_THAN_CORE: Record<string, string> = {
  [`${PLANNER}/common/custom_granularity_test.yaml > cube orders`]:
    'a granularity interval that needs an origin is a .custom() check',
  'packages/cubejs-model-schema/test/fixtures/empty-pre-aggregation.yml > cube orders':
    'Core gives an empty pre-aggregation no default type; the schema cannot tell it is empty',
  'packages/cubejs-model-schema/test/fixtures/empty-pre-aggregation.yml > view orders_view':
    'Core gives an empty pre-aggregation no default type; the schema cannot tell it is empty',
};

// Objects whose file fails to compile before CubeValidator sees them, so Core gives no verdict.
const NOT_VALIDATED: Record<string, string> = {
  'packages/cubejs-schema-compiler/test/unit/fixtures/orders_ext.yml > cube ordersExt':
    'extends `orders` from orders.yml, compiled alone here',
  [`${PLANNER}/common/dimension_kind_tests.yaml > cube test_dims`]: 'planner-only syntax: CUBE is not defined',
  [`${PLANNER}/common/integration_view_members.yaml > cube customers`]: 'planner-only syntax: CUBE is not defined',
  [`${PLANNER}/common/integration_view_members.yaml > cube orders`]: 'planner-only syntax: CUBE is not defined',
  [`${PLANNER}/common/integration_view_members.yaml > view customers_view`]: 'planner-only syntax: CUBE is not defined',
  [`${PLANNER}/common/integration_view_members.yaml > view orders_view`]: 'planner-only syntax: CUBE is not defined',
  [`${PLANNER}/compilation_tests/ownership_test.yaml > cube orders`]: 'planner-only syntax: CUBE is not defined',
  [`${PLANNER}/compilation_tests/ownership_test.yaml > cube users`]: 'planner-only syntax: CUBE is not defined',
  [`${PLANNER}/compilation_tests/ownership_test.yaml > view users_to_orders`]: 'planner-only syntax: CUBE is not defined',
  [`${PLANNER}/compilation_tests/wrong_cube_refs_test.yaml > cube orders`]: 'wrong cube references on purpose',
  [`${PLANNER}/compilation_tests/wrong_cube_refs_test.yaml > cube users`]: 'wrong cube references on purpose',
};

function modelFiles(dir: string): string[] {
  return fs.readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      return entry.name === 'node_modules' ? [] : modelFiles(full);
    }
    return /\.ya?ml$/.test(entry.name) && /^(cubes|views|view_groups):/m.test(fs.readFileSync(full, 'utf8')) ? [full] : [];
  });
}

// Core's verdict, keyed like the YAML side: `<collection>/<name>` -> Joi's error message or null.
async function coreVerdicts(fileName: string, content: string): Promise<Map<string, string | null>> {
  const validate = jest.spyOn(CubeValidator.prototype, 'validate');
  const validateViewGroup = jest.spyOn(CubeValidator.prototype, 'validateViewGroup');
  const verdicts = new Map<string, string | null>();

  try {
    await prepareCompiler({
      localPath: () => REPO,
      dataSchemaFiles: () => Promise.resolve([{ fileName: path.basename(fileName), content }]),
    }, { adapter: 'postgres' }).compile();
  } catch {
    // Errors other than the schema's (joins, SQL, references) don't matter here.
  }

  validate.mock.calls.forEach(([cube], i) => {
    const { value } = validate.mock.results[i];
    verdicts.set(`${cube.isView ? 'views' : 'cubes'}/${cube.name}`, value.error?.message ?? null);
  });
  validateViewGroup.mock.calls.forEach(([group], i) => {
    verdicts.set(`view_groups/${group.name}`, validateViewGroup.mock.results[i].value.error?.message ?? null);
  });
  validate.mockRestore();
  validateViewGroup.mockRestore();
  return verdicts;
}

describe('the generated manifest agrees with Core on every YAML model in the repo', () => {
  const stricter: Record<string, string> = {};
  const looser: Record<string, string> = {};
  const notValidated: string[] = [];

  beforeAll(async () => {
    const ajv = new Ajv({ strict: false, allErrors: true });
    ajv.addSchema(modelSchemaFromJoi(describeModelSchemas()).schema, 'manifest');

    for (const file of CORPUS.flatMap((dir) => modelFiles(path.join(REPO, dir)))) {
      const content = fs.readFileSync(file, 'utf8');
      const relative = path.relative(REPO, file);
      // An editor reads YAML 1.2, where `2022-01-01` is a string; js-yaml's default makes it a Date.
      const model = YAML.load(content, { schema: YAML.CORE_SCHEMA }) as Partial<Record<Collection, { name: string }[]>>;
      const verdicts = await coreVerdicts(relative, content);

      for (const collection of Object.keys(COLLECTIONS) as Collection[]) {
        const check = ajv.getSchema(`manifest#/properties/${collection}/items`)!;

        for (const item of model[collection] ?? []) {
          const key = `${relative} > ${COLLECTIONS[collection]} ${item.name}`;
          const coreError = verdicts.get(`${collection}/${item.name}`);
          const accepted = check(item);

          if (coreError === undefined) {
            notValidated.push(key);
          } else if (coreError === null && !accepted) {
            stricter[key] = ajv.errorsText(check.errors);
          } else if (coreError !== null && accepted) {
            looser[key] = coreError;
          }
        }
      }
    }
  }, 120_000);

  it('accepts every object Core accepts', () => {
    expect(stricter).toEqual({});
  });

  it('rejects every object Core rejects, except the known JS-only rules', () => {
    expect(Object.keys(looser).sort()).toEqual(Object.keys(LOOSER_THAN_CORE).sort());
  });

  it('gets a verdict from Core on every object, except the known compile failures', () => {
    expect(notValidated.sort()).toEqual(Object.keys(NOT_VALIDATED).sort());
  });
});
