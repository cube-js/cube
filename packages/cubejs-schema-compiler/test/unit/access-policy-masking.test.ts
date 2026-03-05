import { PostgresQuery } from '../../src';
import { prepareJsCompiler, prepareYamlCompiler } from './PrepareCompiler';

/**
 * Tests for data access policy member masking feature.
 * When a member is not accessible via member_level but has a masking match,
 * users see masked values instead of an error.
 */
describe('Access Policy - Data Masking', () => {
  const createOrdersSchemaYaml = (extraOptions: string = '') => `
cubes:
  - name: orders
    sql_table: orders

    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

      - name: status
        sql: status
        type: string

      - name: secret_string
        sql: secret_string
        type: string
        mask:
          sql: "CONCAT('***', RIGHT({CUBE}.secret_string, 3))"

      - name: secret_number
        sql: secret_number
        type: number
        mask: -1

      - name: secret_boolean
        sql: secret_boolean
        type: boolean
        mask: false

      - name: no_mask_string
        sql: no_mask_string
        type: string

    measures:
      - name: count
        type: count
        mask: 12345

      - name: count_d
        sql: id
        type: count_distinct
        mask: 34567

      - name: revenue
        sql: revenue
        type: sum

${extraOptions}
`;

  const createSchemaWithMaskingPolicy = (policies: string) => createOrdersSchemaYaml(`
    access_policy:
${policies}
`);

  describe('Mask parameter compilation', () => {
    it('should compile mask with sql function', async () => {
      const { compiler, cubeEvaluator } = prepareYamlCompiler(
        createOrdersSchemaYaml()
      );
      await compiler.compile();
      compiler.throwIfAnyErrors();

      const cube = cubeEvaluator.cubeFromPath('orders');
      const secretStringDim = cube.dimensions.secret_string;
      expect(secretStringDim.mask).toBeDefined();
      expect(secretStringDim.mask).toHaveProperty('sql');
      expect(typeof (secretStringDim.mask as any).sql).toBe('function');
    });

    it('should compile mask with number literal', async () => {
      const { compiler, cubeEvaluator } = prepareYamlCompiler(
        createOrdersSchemaYaml()
      );
      await compiler.compile();
      compiler.throwIfAnyErrors();

      const cube = cubeEvaluator.cubeFromPath('orders');
      const secretNumberDim = cube.dimensions.secret_number;
      expect(secretNumberDim.mask).toBe(-1);
    });

    it('should compile mask with boolean literal', async () => {
      const { compiler, cubeEvaluator } = prepareYamlCompiler(
        createOrdersSchemaYaml()
      );
      await compiler.compile();
      compiler.throwIfAnyErrors();

      const cube = cubeEvaluator.cubeFromPath('orders');
      const secretBooleanDim = cube.dimensions.secret_boolean;
      expect(secretBooleanDim.mask).toBe(false);
    });

    it('should compile mask for measures', async () => {
      const { compiler, cubeEvaluator } = prepareYamlCompiler(
        createOrdersSchemaYaml()
      );
      await compiler.compile();
      compiler.throwIfAnyErrors();

      const cube = cubeEvaluator.cubeFromPath('orders');
      expect(cube.measures.count.mask).toBe(12345);
      expect(cube.measures.count_d.mask).toBe(34567);
    });

    it('should compile member_masking in access policy', async () => {
      const { compiler, cubeEvaluator } = prepareYamlCompiler(
        createSchemaWithMaskingPolicy(`
      - role: admin
        member_level:
          includes:
            - status
            - count
        member_masking:
          includes: '*'
`)
      );
      await compiler.compile();
      compiler.throwIfAnyErrors();

      const cube = cubeEvaluator.cubeFromPath('orders');
      expect(cube.accessPolicy).toBeDefined();
      const policy = cube.accessPolicy![0];
      expect(policy.memberMasking).toBeDefined();
      expect(policy.memberMasking!.includesMembers).toContain('orders.secret_string');
      expect(policy.memberMasking!.includesMembers).toContain('orders.count');
    });
  });

  describe('SQL generation with masked members', () => {
    it('should replace dimension SQL with literal mask value', async () => {
      const { compiler, cubeEvaluator, joinGraph } = prepareYamlCompiler(
        createOrdersSchemaYaml()
      );
      await compiler.compile();

      const query = new PostgresQuery({ compiler, cubeEvaluator, joinGraph }, {
        dimensions: ['orders.status', 'orders.secret_number'],
        measures: [],
        filters: [],
        // Simulate that secret_number is masked
        maskedMembers: ['orders.secret_number'],
      } as any);

      const [sql] = query.buildSqlAndParams();
      expect(sql).toContain('-1');
      expect(sql).toContain('"orders".status');
      expect(sql).not.toContain('"orders".secret_number');
    });

    it('should replace dimension SQL with boolean mask value', async () => {
      const { compiler, cubeEvaluator, joinGraph } = prepareYamlCompiler(
        createOrdersSchemaYaml()
      );
      await compiler.compile();

      const query = new PostgresQuery({ compiler, cubeEvaluator, joinGraph }, {
        dimensions: ['orders.secret_boolean'],
        measures: [],
        filters: [],
        maskedMembers: ['orders.secret_boolean'],
      } as any);

      const [sql] = query.buildSqlAndParams();
      expect(sql).toContain('FALSE');
    });

    it('should replace dimension SQL with sql expression mask', async () => {
      const { compiler, cubeEvaluator, joinGraph } = prepareYamlCompiler(
        createOrdersSchemaYaml()
      );
      await compiler.compile();

      const query = new PostgresQuery({ compiler, cubeEvaluator, joinGraph }, {
        dimensions: ['orders.secret_string'],
        measures: [],
        filters: [],
        maskedMembers: ['orders.secret_string'],
      } as any);

      const [sql] = query.buildSqlAndParams();
      // The mask SQL expression CONCAT('***', RIGHT(col, 3)) is used instead of raw col reference
      expect(sql).toContain('CONCAT');
      expect(sql).toContain('***');
      // The masked column appears inside the mask expression (not as bare column select)
      expect(sql).toContain("CONCAT('***', RIGHT");
    });

    it('should replace measure SQL with literal mask, bypassing aggregation', async () => {
      const { compiler, cubeEvaluator, joinGraph } = prepareYamlCompiler(
        createOrdersSchemaYaml()
      );
      await compiler.compile();

      const query = new PostgresQuery({ compiler, cubeEvaluator, joinGraph }, {
        dimensions: ['orders.status'],
        measures: ['orders.count'],
        filters: [],
        maskedMembers: ['orders.count'],
      } as any);

      const [sql] = query.buildSqlAndParams();
      // The count measure should be replaced by the literal 12345
      expect(sql).toContain('12345');
      // Count aggregation should NOT be present for masked measure
      expect(sql).not.toMatch(/count\s*\(\s*(?!.*12345)/i);
    });

    it('should use NULL for member with no mask and no env var default', async () => {
      const { compiler, cubeEvaluator, joinGraph } = prepareYamlCompiler(
        createOrdersSchemaYaml()
      );
      await compiler.compile();

      const query = new PostgresQuery({ compiler, cubeEvaluator, joinGraph }, {
        dimensions: ['orders.no_mask_string'],
        measures: [],
        filters: [],
        maskedMembers: ['orders.no_mask_string'],
      } as any);

      const [sql] = query.buildSqlAndParams();
      expect(sql).toContain('NULL');
    });

    it('should not mask non-masked members', async () => {
      const { compiler, cubeEvaluator, joinGraph } = prepareYamlCompiler(
        createOrdersSchemaYaml()
      );
      await compiler.compile();

      const query = new PostgresQuery({ compiler, cubeEvaluator, joinGraph }, {
        dimensions: ['orders.status', 'orders.secret_string'],
        measures: [],
        filters: [],
        // Only secret_string is masked
        maskedMembers: ['orders.secret_string'],
      } as any);

      const [sql] = query.buildSqlAndParams();
      expect(sql).toContain('"orders".status');
      expect(sql).toContain('CONCAT');
    });
  });

  describe('maskValueToSql helper', () => {
    it('should convert number to SQL literal', async () => {
      const { compiler, cubeEvaluator, joinGraph } = prepareYamlCompiler(
        createOrdersSchemaYaml()
      );
      await compiler.compile();

      const query = new PostgresQuery({ compiler, cubeEvaluator, joinGraph }, {
        dimensions: ['orders.status'],
        measures: [],
        filters: [],
      } as any);

      expect((query as any).maskValueToSql(42)).toBe('42');
      expect((query as any).maskValueToSql(-1)).toBe('-1');
      expect((query as any).maskValueToSql(0)).toBe('0');
    });

    it('should convert boolean to SQL literal', async () => {
      const { compiler, cubeEvaluator, joinGraph } = prepareYamlCompiler(
        createOrdersSchemaYaml()
      );
      await compiler.compile();

      const query = new PostgresQuery({ compiler, cubeEvaluator, joinGraph }, {
        dimensions: ['orders.status'],
        measures: [],
        filters: [],
      } as any);

      expect((query as any).maskValueToSql(true)).toBe('TRUE');
      expect((query as any).maskValueToSql(false)).toBe('FALSE');
    });

    it('should convert string to SQL literal with escaping', async () => {
      const { compiler, cubeEvaluator, joinGraph } = prepareYamlCompiler(
        createOrdersSchemaYaml()
      );
      await compiler.compile();

      const query = new PostgresQuery({ compiler, cubeEvaluator, joinGraph }, {
        dimensions: ['orders.status'],
        measures: [],
        filters: [],
      } as any);

      expect((query as any).maskValueToSql('hello')).toBe("'hello'");
      expect((query as any).maskValueToSql("it's")).toBe("'it''s'");
    });
  });

  describe('JS-style schema with mask parameter', () => {
    it('should compile JS cube with mask parameters', async () => {
      const { compiler, cubeEvaluator } = prepareJsCompiler(`
        cube('Orders', {
          sql: 'SELECT * FROM orders',

          dimensions: {
            id: {
              sql: 'id',
              type: 'number',
              primaryKey: true
            },
            status: {
              sql: 'status',
              type: 'string'
            },
            secret_field: {
              sql: 'secret_field',
              type: 'string',
              mask: {
                sql: (CUBE) => \`CONCAT('***', RIGHT(\${CUBE}.secret_field, 3))\`
              }
            },
            numeric_secret: {
              sql: 'numeric_secret',
              type: 'number',
              mask: -99
            }
          },

          measures: {
            count: {
              type: 'count',
              mask: 9999
            }
          },

          accessPolicy: [
            {
              role: 'admin',
              memberLevel: {
                includes: ['status', 'count']
              },
              memberMasking: {
                includes: '*'
              }
            }
          ]
        })
      `);
      await compiler.compile();
      compiler.throwIfAnyErrors();

      const cube = cubeEvaluator.cubeFromPath('Orders');
      expect(cube.dimensions.secret_field.mask).toBeDefined();
      expect(cube.dimensions.numeric_secret.mask).toBe(-99);
      expect(cube.measures.count.mask).toBe(9999);

      const policy = cube.accessPolicy![0];
      expect(policy.memberMasking).toBeDefined();
      expect(policy.memberMasking!.includesMembers).toContain('Orders.secret_field');
      expect(policy.memberMasking!.includesMembers).toContain('Orders.count');
    });
  });

  describe('member_masking compilation with excludes', () => {
    it('should compile member_masking with includes and excludes', async () => {
      const { compiler, cubeEvaluator } = prepareYamlCompiler(
        createSchemaWithMaskingPolicy(`
      - role: viewer
        member_level:
          includes:
            - status
        member_masking:
          includes: '*'
          excludes:
            - revenue
`)
      );
      await compiler.compile();
      compiler.throwIfAnyErrors();

      const cube = cubeEvaluator.cubeFromPath('orders');
      const policy = cube.accessPolicy![0];
      expect(policy.memberMasking!.includesMembers).toContain('orders.secret_string');
      expect(policy.memberMasking!.excludesMembers).toContain('orders.revenue');
    });
  });

  describe('SQL generation with env var defaults', () => {
    it('should use env var default for string type mask', async () => {
      const originalEnv = process.env.CUBEJS_ACCESS_POLICY_MASK_STRING;
      process.env.CUBEJS_ACCESS_POLICY_MASK_STRING = '******';

      try {
        const { compiler, cubeEvaluator, joinGraph } = prepareYamlCompiler(
          createOrdersSchemaYaml()
        );
        await compiler.compile();

        const query = new PostgresQuery({ compiler, cubeEvaluator, joinGraph }, {
          dimensions: ['orders.no_mask_string'],
          measures: [],
          filters: [],
          maskedMembers: ['orders.no_mask_string'],
        } as any);

        const [sql] = query.buildSqlAndParams();
        expect(sql).toContain("'******'");
      } finally {
        if (originalEnv === undefined) {
          delete process.env.CUBEJS_ACCESS_POLICY_MASK_STRING;
        } else {
          process.env.CUBEJS_ACCESS_POLICY_MASK_STRING = originalEnv;
        }
      }
    });

    it('should use env var default for number type mask', async () => {
      const originalEnv = process.env.CUBEJS_ACCESS_POLICY_MASK_NUMBER;
      process.env.CUBEJS_ACCESS_POLICY_MASK_NUMBER = '0';

      try {
        const { compiler, cubeEvaluator, joinGraph } = prepareYamlCompiler(
          createOrdersSchemaYaml()
        );
        await compiler.compile();

        const query = new PostgresQuery({ compiler, cubeEvaluator, joinGraph }, {
          dimensions: [],
          measures: ['orders.revenue'],
          filters: [],
          maskedMembers: ['orders.revenue'],
        } as any);

        const [sql] = query.buildSqlAndParams();
        expect(sql).toContain('0');
      } finally {
        if (originalEnv === undefined) {
          delete process.env.CUBEJS_ACCESS_POLICY_MASK_NUMBER;
        } else {
          process.env.CUBEJS_ACCESS_POLICY_MASK_NUMBER = originalEnv;
        }
      }
    });
  });
});
