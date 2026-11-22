import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './TrinoDbRunner';

describe('Trino multi-stage null-safe joins', () => {
  jest.setTimeout(300000);

  // End-of-period count: each entity counts once per week, in the city of its last
  // snapshot. Entity 1 switches city on the last day of the week; entities 4 and 5
  // have a NULL city, entity 6 an empty one.
  const compilers = prepareYamlCompiler(`
cubes:
  - name: snapshots
    sql: >
      SELECT
        concat('e', CAST(e AS varchar), '-', CAST(d AS varchar)) AS id,
        concat('e', CAST(e AS varchar)) AS entity_id,
        CAST(date_add('day', d, DATE '2026-08-31') AS timestamp) AS snapshot_date,
        CASE
          WHEN e = 1 AND d % 7 = 6 THEN 'A'
          WHEN e IN (4, 5) THEN NULL
          WHEN e = 6 THEN ''
          WHEN e % 2 = 0 THEN 'A'
          ELSE 'B'
        END AS city,
        CASE WHEN (e + d) % 3 = 0 THEN 0.1 ELSE 0.9 END AS score
      FROM UNNEST(sequence(1, 6)) AS t(e)
      CROSS JOIN UNNEST(sequence(0, 13)) AS s(d)

    dimensions:
      - name: id
        sql: id
        type: string
        primary_key: true
      - name: entity_id
        sql: entity_id
        type: string
      - name: city
        sql: city
        type: string
      - name: snapshot_date
        sql: snapshot_date
        type: time

    measures:
      - name: group_last_date
        type: number
        sql: "max({CUBE}.snapshot_date)"
        public: false
      - name: group_last_flag
        type: number
        sql: "max_by(CASE WHEN {CUBE}.score <= 0.30 THEN 1 ELSE 0 END, {CUBE}.snapshot_date)"
        public: false
      - name: entity_last_date
        multi_stage: true
        type: max
        sql: "{group_last_date}"
        add_group_by: [entity_id]
        reduce_by: [city]
        public: false
      - name: low_score_entities_eop
        multi_stage: true
        type: sum
        sql: "CASE WHEN {group_last_date} = {entity_last_date} THEN {group_last_flag} ELSE 0 END"
        add_group_by: [entity_id]
      - name: entities_eop
        multi_stage: true
        type: sum
        sql: "CASE WHEN {group_last_date} = {entity_last_date} THEN 1 ELSE 0 END"
        add_group_by: [entity_id]
  `);

  // Without the UNION ALL combine, multi-stage results are joined to a keys grid, as on
  // Presto and Athena.
  const buildSqlAndParams = async (unionFullKeyAggregate = true) => {
    await compilers.compile();
    return dbRunner.newTestQuery(compilers, {
      unionFullKeyAggregate,
      measures: ['snapshots.low_score_entities_eop', 'snapshots.entities_eop'],
      dimensions: ['snapshots.city'],
      timeDimensions: [{
        dimension: 'snapshots.snapshot_date',
        granularity: 'week',
        dateRange: ['2026-08-31', '2026-09-13'],
      }],
      timezone: 'UTC',
    }).buildSqlAndParams();
  };

  it.each([true, false])('plans every join with an equi-join criterion (union combine: %s)', async (unionFullKeyAggregate) => {
    const plan = await dbRunner.explain(await buildSqlAndParams(unionFullKeyAggregate));
    // A join without criteria is a nested loop over both sides; Trino shows a filter-only
    // inner join as a CrossJoin.
    const joins = plan.split('\n').filter(line => /\b(Left|Right|Inner|Full|Cross)?Join\[/.test(line));

    expect(joins.length).toBeGreaterThan(0);
    expect(joins.filter(line => !line.includes('criteria ='))).toEqual([]);
  });

  it.each([true, false])('keeps a NULL key apart from an empty one (union combine: %s)', async (unionFullKeyAggregate) => {
    const rows = await dbRunner.testQueries([await buildSqlAndParams(unionFullKeyAggregate)]);
    const byKey = (r: Record<string, unknown>) => `${r.snapshots__snapshot_date_week}|${r.snapshots__city}`;

    expect([...rows].sort((a, b) => byKey(a).localeCompare(byKey(b)))).toEqual([
      { snapshots__city: '', snapshots__snapshot_date_week: '2026-08-31 00:00:00.000', snapshots__low_score_entities_eop: 1, snapshots__entities_eop: 1 },
      { snapshots__city: 'A', snapshots__snapshot_date_week: '2026-08-31 00:00:00.000', snapshots__low_score_entities_eop: 0, snapshots__entities_eop: 2 },
      { snapshots__city: 'B', snapshots__snapshot_date_week: '2026-08-31 00:00:00.000', snapshots__low_score_entities_eop: 1, snapshots__entities_eop: 1 },
      { snapshots__city: null, snapshots__snapshot_date_week: '2026-08-31 00:00:00.000', snapshots__low_score_entities_eop: 0, snapshots__entities_eop: 2 },
      { snapshots__city: '', snapshots__snapshot_date_week: '2026-09-07 00:00:00.000', snapshots__low_score_entities_eop: 0, snapshots__entities_eop: 1 },
      { snapshots__city: 'A', snapshots__snapshot_date_week: '2026-09-07 00:00:00.000', snapshots__low_score_entities_eop: 1, snapshots__entities_eop: 2 },
      { snapshots__city: 'B', snapshots__snapshot_date_week: '2026-09-07 00:00:00.000', snapshots__low_score_entities_eop: 0, snapshots__entities_eop: 1 },
      { snapshots__city: null, snapshots__snapshot_date_week: '2026-09-07 00:00:00.000', snapshots__low_score_entities_eop: 1, snapshots__entities_eop: 2 },
    ]);
  });
});
