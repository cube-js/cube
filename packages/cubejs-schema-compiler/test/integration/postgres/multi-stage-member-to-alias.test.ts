import { SnowflakeQuery } from '../../../src/adapter/SnowflakeQuery';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';

// Multi-stage ratio measure in a cube whose name is >= the 16-char alias truncation limit.
// The SQL API (cubesql) sends `memberToAlias` where every selected member truncates to the
// same 16-char prefix (deduped with _1/_2 suffixes). This previously produced
// `invalid identifier '"kibanasampledata"'` from the warehouse.
describe('multi_stage member_to_alias', () => {
  jest.setTimeout(60000);

  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: KibanaSampleDataEcommerce
    sql_table: public.kibana_sample_data_ecommerce
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
      - name: customer_gender
        sql: "{CUBE}.customer_gender"
        type: string
      - name: notes
        sql: "{CUBE}.notes"
        type: string
      - name: status
        sql: "{CUBE}.status"
        type: string
    measures:
      - name: sumPrice
        sql: "{CUBE}.price"
        type: sum
      - name: sumPriceTotal
        sql: "{sumPrice}"
        type: sum
        multi_stage: true
        group_by:
          - notes
          - status
      - name: pricePercent
        sql: "coalesce(100 * {sumPrice} / nullif({sumPriceTotal}, 0), 0)"
        type: number
        multi_stage: true
`);

  // What cubesql sends: the cube name alone is >= 16 chars, so every member truncates to the
  // same `kibanasampledata` prefix and is deduped with _N suffixes.
  const queryOptions = {
    measures: [
      'KibanaSampleDataEcommerce.pricePercent',
    ],
    dimensions: [
      'KibanaSampleDataEcommerce.customer_gender',
      'KibanaSampleDataEcommerce.notes',
      'KibanaSampleDataEcommerce.status',
    ],
    memberToAlias: {
      'KibanaSampleDataEcommerce.customer_gender': 'kibanasampledata',
      'KibanaSampleDataEcommerce.notes': 'kibanasampledata_1',
      'KibanaSampleDataEcommerce.status': 'kibanasampledata_2',
      'KibanaSampleDataEcommerce.pricePercent': 'kibanasampledata_3',
    },
    timezone: 'UTC',
    order: [],
  };

  it('grouped multi_stage ratio with colliding truncated memberToAlias', async () => {
    await compiler.compile();

    const query = new SnowflakeQuery({ joinGraph, cubeEvaluator, compiler }, queryOptions);

    const [sql] = query.buildSqlAndParams();

    // The leaf scan over the real table must project each selected dimension under its
    // memberToAlias, so every downstream multi-stage CTE that references the dimension by
    // that alias resolves. Before the fix the leaf projected the full internal name
    // (`kibana_sample_data_ecommerce__customer_gender`) while the stages referenced
    // `"kibanasampledata"`, producing `invalid identifier "kibanasampledata"`.
    expect(sql).toMatch(/\.customer_gender\s+"kibanasampledata"/);
    expect(sql).toMatch(/\.notes\s+"kibanasampledata_1"/);
    expect(sql).toMatch(/\.status\s+"kibanasampledata_2"/);

    // And the inverted aliasing (reference truncated alias, output full internal name) that
    // characterised the bug must not appear anywhere.
    expect(sql).not.toMatch(/"kibanasampledata(?:_\d+)?"\s+"kibana_sample_data_ecommerce__/);
  });

  // Same scenario through the native (Tesseract) SQL planner, which generates SQL itself
  // rather than via BaseQuery. It bakes memberToAlias into each member symbol and resolves
  // references through a shared references builder, so define/reference stay consistent.
  it('grouped multi_stage ratio with colliding truncated memberToAlias (Tesseract)', async () => {
    await compiler.compile();

    const query = new SnowflakeQuery(
      { joinGraph, cubeEvaluator, compiler },
      { ...queryOptions, useNativeSqlPlanner: true },
    );

    const [sql] = query.buildSqlAndParams();

    // Final output must expose all four members under their memberToAlias values.
    expect(sql).toMatch(/"kibanasampledata"/);
    expect(sql).toMatch(/"kibanasampledata_1"/);
    expect(sql).toMatch(/"kibanasampledata_2"/);
    expect(sql).toMatch(/"kibanasampledata_3"/);

    // Every qualified reference must resolve to a projected column; a reference to a truncated
    // alias that no sub-select defines is the failure mode we are guarding against.
    const referenced = new Set(
      [...sql.matchAll(/\b\w+\."(kibanasampledata(?:_\d+)?)"/g)].map((m) => m[1]),
    );
    const defined = new Set(
      [...sql.matchAll(/"(kibanasampledata(?:_\d+)?)"(?=\s*(?:,|\)|$|\sAS|\sFROM))/gim)].map((m) => m[1]),
    );
    const dangling = [...referenced].filter((r) => !defined.has(r));
    expect(dangling).toEqual([]);
  });
});
