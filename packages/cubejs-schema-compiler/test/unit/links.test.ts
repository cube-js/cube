import { PostgresQuery } from '../../src';
import { prepareYamlCompiler } from './PrepareCompiler';

describe('Links', () => {
  const schemaWithLinks = `
cubes:
  - name: users
    sql_table: users

    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

      - name: full_name
        sql: full_name
        type: string
        links:
          - name: google_search
            label: Search on Google
            url: "{full_name}"
            icon: brand-google
            target: blank
          - name: send_email
            label: Write an email
            url: "{email}"
            icon: send

      - name: email
        sql: email
        type: string
`;

  it('should create synthetic link URL dimensions', async () => {
    const compilers = prepareYamlCompiler(schemaWithLinks);
    await compilers.compiler.compile();

    const googleDef = compilers.cubeEvaluator.dimensionByPath('users.full_name___link_google_search_url');
    expect(googleDef).toBeDefined();
    expect(googleDef.type).toBe('string');
    expect((googleDef as any).synthetic).toBe(true);

    const emailDef = compilers.cubeEvaluator.dimensionByPath('users.full_name___link_send_email_url');
    expect(emailDef).toBeDefined();
    expect(emailDef.type).toBe('string');
    expect((emailDef as any).synthetic).toBe(true);
  });

  it('synthetic link dimension exists and can be referenced', async () => {
    const compilers = prepareYamlCompiler(schemaWithLinks);
    await compilers.compiler.compile();

    const dimDef = compilers.cubeEvaluator.dimensionByPath('users.full_name___link_google_search_url');
    expect(dimDef).toBeDefined();
    expect(dimDef.type).toBe('string');
    expect(typeof dimDef.sql).toBe('function');
  });

  it('should NOT include link URL columns unless explicitly queried', async () => {
    const compilers = prepareYamlCompiler(schemaWithLinks);
    await compilers.compiler.compile();

    const query = new PostgresQuery(compilers, {
      measures: [],
      dimensions: ['users.full_name'],
    });

    const queryAndParams = query.buildSqlAndParams();
    const sql = queryAndParams[0];

    expect(sql).not.toContain('___link_');
  });

  it('should expose links metadata and synthetic flag in meta config', async () => {
    const compilers = prepareYamlCompiler(schemaWithLinks);
    await compilers.compiler.compile();

    const { metaTransformer } = compilers;
    const { cubes } = metaTransformer;
    const usersCube = cubes.find((c: any) => c.config.name === 'users');
    expect(usersCube).toBeDefined();

    const fullNameDim = usersCube!.config.dimensions.find(
      (d: any) => d.name === 'users.full_name'
    );
    expect(fullNameDim).toBeDefined();
    expect(fullNameDim!.links).toBeDefined();
    expect(fullNameDim!.links).toHaveLength(2);
    expect(fullNameDim!.links![0].label).toBe('Search on Google');
    expect(fullNameDim!.links![0].icon).toBe('brand-google');
    expect(fullNameDim!.links![0].target).toBe('blank');

    const syntheticDim = usersCube!.config.dimensions.find(
      (d: any) => d.name === 'users.full_name___link_google_search_url'
    );
    expect(syntheticDim).toBeDefined();
    expect(syntheticDim!.synthetic).toBe(true);
  });

  it('synthetic link dimensions should be public by default', async () => {
    const compilers = prepareYamlCompiler(schemaWithLinks);
    await compilers.compiler.compile();

    const { metaTransformer } = compilers;
    const { cubes } = metaTransformer;
    const usersCube = cubes.find((c: any) => c.config.name === 'users');
    expect(usersCube).toBeDefined();

    const syntheticDim = usersCube!.config.dimensions.find(
      (d: any) => d.name === 'users.full_name___link_google_search_url'
    );
    expect(syntheticDim).toBeDefined();
    expect(syntheticDim!.public).toBe(true);
  });

  it('should validate links schema - label is required', async () => {
    const invalidSchema = `
cubes:
  - name: users
    sql_table: users

    dimensions:
      - name: full_name
        sql: full_name
        type: string
        links:
          - name: test
            url: "{full_name}"
`;
    const compilers = prepareYamlCompiler(invalidSchema);

    try {
      await compilers.compiler.compile();
      fail('Should have thrown an error for missing label');
    } catch (e: any) {
      expect(e.message || e.toString()).toMatch(/label/i);
    }
  });

  it('should reject duplicate link names on same dimension', async () => {
    const schema = `
cubes:
  - name: users
    sql_table: users
    dimensions:
      - name: full_name
        sql: full_name
        type: string
        links:
          - name: dup
            label: First
            url: "{full_name}"
          - name: dup
            label: Second
            url: "{full_name}"
`;
    const compilers = prepareYamlCompiler(schema);
    try {
      await compilers.compiler.compile();
      fail('Should have thrown for duplicate link name');
    } catch (e: any) {
      expect(e.message || e.toString()).toMatch(/[Dd]uplicate.*dup/);
    }
  });

  it('should reject link that collides with user-defined dimension', async () => {
    const schema = `
cubes:
  - name: users
    sql_table: users
    dimensions:
      - name: full_name
        sql: full_name
        type: string
        links:
          - name: custom
            label: Link
            url: "{full_name}"
      - name: full_name___link_custom_url
        sql: "'manual'"
        type: string
`;
    const compilers = prepareYamlCompiler(schema);
    try {
      await compilers.compiler.compile();
      fail('Should have thrown for collision');
    } catch (e: any) {
      expect(e.message || e.toString()).toMatch(/conflict|collision|already/i);
    }
  });

  it('should reject multiple primary links on same dimension', async () => {
    const schema = `
cubes:
  - name: users
    sql_table: users
    dimensions:
      - name: full_name
        sql: full_name
        type: string
        links:
          - name: first
            label: First
            url: "{full_name}"
            primary: true
          - name: second
            label: Second
            url: "{full_name}"
            primary: true
`;
    const compilers = prepareYamlCompiler(schema);
    try {
      await compilers.compiler.compile();
      fail('Should have thrown for multiple primary links');
    } catch (e: any) {
      expect(e.message || e.toString()).toMatch(/primary/i);
    }
  });

  describe('dashboard links', () => {
    const schemaWithDashboardLink = `
cubes:
  - name: users
    sql_table: users

    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

      - name: full_name
        sql: full_name
        type: string
        links:
          - name: overview
            label: View dashboard
            dashboard: abc123
            icon: dashboard
`;

    it('should create synthetic dimension for dashboard link', async () => {
      const compilers = prepareYamlCompiler(schemaWithDashboardLink);
      await compilers.compiler.compile();

      const dimDef = compilers.cubeEvaluator.dimensionByPath('users.full_name___link_overview_url');
      expect(dimDef).toBeDefined();
      expect(dimDef.type).toBe('string');
      expect((dimDef as any).synthetic).toBe(true);
    });

    it('should expose dashboard in meta config', async () => {
      const compilers = prepareYamlCompiler(schemaWithDashboardLink);
      await compilers.compiler.compile();

      const { metaTransformer } = compilers;
      const { cubes } = metaTransformer;
      const usersCube = cubes.find((c: any) => c.config.name === 'users');
      expect(usersCube).toBeDefined();

      const fullNameDim = usersCube!.config.dimensions.find(
        (d: any) => d.name === 'users.full_name'
      );
      expect(fullNameDim).toBeDefined();
      expect(fullNameDim!.links![0].dashboard).toBe('abc123');
    });

    it('should not allow both url and dashboard on same link', async () => {
      const invalidSchema = `
cubes:
  - name: users
    sql_table: users

    dimensions:
      - name: full_name
        sql: full_name
        type: string
        links:
          - name: both
            label: Invalid
            url: "{full_name}"
            dashboard: abc123
`;
      const compilers = prepareYamlCompiler(invalidSchema);

      try {
        await compilers.compiler.compile();
        fail('Should have thrown a validation error');
      } catch (e: any) {
        expect(e.message || e.toString()).toMatch(/url.*dashboard|dashboard.*url/i);
      }
    });
  });

  describe('params', () => {
    const schemaWithParams = `
cubes:
  - name: users
    sql_table: users

    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

      - name: full_name
        sql: full_name
        type: string
        links:
          - name: profile
            label: View profile
            dashboard: dash123
            params:
              - key: user_id
                value: "{id}"
              - key: user_name
                value: "{full_name}"

      - name: country
        sql: country
        type: string
`;

    it('should create synthetic dimension with params', async () => {
      const compilers = prepareYamlCompiler(schemaWithParams);
      await compilers.compiler.compile();

      const dimDef = compilers.cubeEvaluator.dimensionByPath('users.full_name___link_profile_url');
      expect(dimDef).toBeDefined();
      expect(dimDef.type).toBe('string');
      expect((dimDef as any).synthetic).toBe(true);
      expect(typeof dimDef.sql).toBe('function');
    });

    it('should generate SQL with urlEncode for params', async () => {
      const compilers = prepareYamlCompiler(schemaWithParams);
      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        measures: [],
        dimensions: ['users.full_name___link_profile_url'],
      });

      const queryAndParams = query.buildSqlAndParams();
      const sql = queryAndParams[0];

      expect(sql).toContain('/dashboard/dash123');
      expect(sql).toContain('user_id=');
      expect(sql).toContain('name=');
      expect(sql).toContain('REPLACE');
    });

    it('should url-encode param values with special characters', async () => {
      const schemaWithSpecialChars = `
cubes:
  - name: items
    sql: >
      SELECT 'hello world & more' as name, 'a=b+c' as code

    dimensions:
      - name: name
        sql: name
        type: string
        links:
          - name: search
            label: Search
            dashboard: dash1
            params:
              - key: q
                value: "{name}"
              - key: filter
                value: "{code}"

      - name: code
        sql: code
        type: string
`;
      const compilers = prepareYamlCompiler(schemaWithSpecialChars);
      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        measures: [],
        dimensions: ['items.name___link_search_url'],
      });

      const queryAndParams = query.buildSqlAndParams();
      const sql = queryAndParams[0];

      // The SQL should wrap each param value in REPLACE chains for URL encoding
      // Encoding: % -> %25, & -> %26, = -> %3D, + -> %2B, space -> %20
      expect(sql).toContain("'%', '%25'");
      expect(sql).toContain("'&', '%26'");
      expect(sql).toContain("'=', '%3D'");
      expect(sql).toContain("'+', '%2B'");
      expect(sql).toContain("' ', '%20'");
      // Should have REPLACE for both params (name and code)
      const replaceCount = (sql.match(/REPLACE/g) || []).length;
      expect(replaceCount).toBeGreaterThanOrEqual(10);
    });
  });

  describe('access policy on view with links', () => {
    const schemaWithViewAndPolicy = `
cubes:
  - name: users
    sql_table: users

    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

      - name: full_name
        sql: full_name
        type: string
        links:
          - name: google_search
            label: Search on Google
            url: "{full_name}"
            icon: brand-google

      - name: email
        sql: email
        type: string

views:
  - name: users_view
    cubes:
      - join_path: users
        includes:
          - full_name
          - email
    access_policy:
      - group: "*"
        member_level:
          includes:
            - full_name
            - full_name___link_google_search_url
`;

    it('should include synthetic link dim when explicitly listed in access policy', async () => {
      const compilers = prepareYamlCompiler(schemaWithViewAndPolicy);
      await compilers.compiler.compile();

      const viewCube = compilers.cubeEvaluator.cubeFromPath('users_view');
      expect(viewCube).toBeDefined();

      const policy = viewCube.accessPolicy![0];
      expect(policy.memberLevel!.includesMembers).toContain('users_view.full_name');
      expect(policy.memberLevel!.includesMembers).toContain('users_view.full_name___link_google_search_url');
    });

    const schemaWithViewPolicyExcludeLink = `
cubes:
  - name: users
    sql_table: users

    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

      - name: full_name
        sql: full_name
        type: string
        links:
          - name: google_search
            label: Search on Google
            url: "{full_name}"
            icon: brand-google

      - name: email
        sql: email
        type: string

views:
  - name: users_view
    cubes:
      - join_path: users
        includes:
          - full_name
          - email
    access_policy:
      - group: "*"
        member_level:
          includes:
            - full_name
            - email
`;

    it('should exclude synthetic link dim when not listed in access policy includes', async () => {
      const compilers = prepareYamlCompiler(schemaWithViewPolicyExcludeLink);
      await compilers.compiler.compile();

      const viewCube = compilers.cubeEvaluator.cubeFromPath('users_view');
      expect(viewCube).toBeDefined();

      const policy = viewCube.accessPolicy![0];
      expect(policy.memberLevel!.includesMembers).toContain('users_view.full_name');
      expect(policy.memberLevel!.includesMembers).toContain('users_view.email');
      expect(policy.memberLevel!.includesMembers).not.toContain('users_view.full_name___link_google_search_url');
    });

    const schemaWithViewPolicyWildcard = `
cubes:
  - name: users
    sql_table: users

    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

      - name: full_name
        sql: full_name
        type: string
        links:
          - name: google_search
            label: Search on Google
            url: "{full_name}"
            icon: brand-google

      - name: email
        sql: email
        type: string

views:
  - name: users_view
    cubes:
      - join_path: users
        includes: "*"
    access_policy:
      - group: "*"
        member_level:
          includes: "*"
`;

    it('should include synthetic link dim when access policy uses wildcard includes', async () => {
      const compilers = prepareYamlCompiler(schemaWithViewPolicyWildcard);
      await compilers.compiler.compile();

      const viewCube = compilers.cubeEvaluator.cubeFromPath('users_view');
      expect(viewCube).toBeDefined();

      const policy = viewCube.accessPolicy![0];
      expect(policy.memberLevel!.includesMembers).toContain('users_view.full_name___link_google_search_url');
    });
  });
});
