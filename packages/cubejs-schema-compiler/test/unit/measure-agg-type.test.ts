import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareJsCompiler, prepareYamlCompiler } from './PrepareCompiler';

describe('Measure agg_type', () => {
  const model = (measures: string, views = '') => `
cubes:
  - name: events
    sql: "SELECT * FROM events"

    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

      - name: status
        sql: status
        type: string

    measures:
${measures}
${views}`;

  const measure = (name: string, props: Record<string, string>) => [
    `      - name: ${name}`,
    ...Object.entries(props).map(([key, value]) => `        ${key}: ${value}`)
  ].join('\n');

  const measuresMeta = (compilers: any, cubeName = 'events') => Object.fromEntries(
    compilers.metaTransformer.cubes
      .find((cube: any) => cube.config.name === cubeName).config.measures
      .map((m: any) => [m.name, { type: m.type, aggType: m.aggType }])
  );

  describe('validation', () => {
    const allowed: [string, string][] = [
      ['number', 'sum'],
      ['number', 'avg'],
      ['number', 'min'],
      ['number', 'max'],
      ['number', 'count_distinct'],
      ['number', 'count_distinct_approx'],
      ['string', 'min'],
      ['string', 'max'],
      ['time', 'min'],
      ['time', 'max'],
      ['boolean', 'min'],
      ['boolean', 'max'],
    ];

    it.each(allowed)('accepts type: %s with agg_type: %s', async (type, aggType) => {
      const compilers = prepareYamlCompiler(model(measure('m', { sql: 'value', type, agg_type: aggType })));
      await compilers.compiler.compile();
    });

    it('accepts agg_type: count without sql', async () => {
      const compilers = prepareYamlCompiler(model(measure('m', { type: 'number', agg_type: 'count' })));
      await compilers.compiler.compile();
    });

    const rejected: [string, string][] = [
      ['string', 'sum'],
      ['time', 'avg'],
      ['time', 'count'],
      ['boolean', 'count_distinct'],
      ['number', 'number'],
      ['number', 'median'],
    ];

    it.each(rejected)('rejects type: %s with agg_type: %s', async (type, aggType) => {
      const compilers = prepareYamlCompiler(model(measure('m', { sql: 'value', type, agg_type: aggType })));
      await expect(compilers.compiler.compile()).rejects.toThrow(/aggType.*must be one of/);
    });

    it.each(['sum', 'max', 'count', 'count_distinct'])('rejects agg_type next to type: %s', async (type) => {
      const compilers = prepareYamlCompiler(model(measure('m', { sql: 'value', type, agg_type: 'max' })));
      await expect(compilers.compiler.compile()).rejects.toThrow(
        /"aggType" property can only be used with type: number, string, time or boolean/
      );
    });

    it('requires sql unless agg_type is count', async () => {
      const compilers = prepareYamlCompiler(model(measure('m', { type: 'time', agg_type: 'max' })));
      await expect(compilers.compiler.compile()).rejects.toThrow(/sql.*is required/);
    });

    it('accepts agg_type in a JavaScript model', async () => {
      const compilers = prepareJsCompiler(`
        cube('events', {
          sql: 'SELECT * FROM events',
          measures: {
            last_event_at: { sql: 'created_at', type: 'time', agg_type: 'max' },
            unique_users: { sql: 'user_id', type: 'number', aggType: 'countDistinct' },
          },
        });
      `);
      await compilers.compiler.compile();

      expect(measuresMeta(compilers)).toEqual({
        'events.last_event_at': { type: 'time', aggType: 'max' },
        'events.unique_users': { type: 'number', aggType: 'countDistinct' },
      });
    });
  });

  describe('meta', () => {
    const measures = [
      measure('legacy_max', { sql: 'created_at', type: 'max' }),
      measure('legacy_sum', { sql: 'amount', type: 'sum' }),
      measure('legacy_time', { sql: 'MAX(created_at)', type: 'time' }),
      measure('last_event_at', { sql: 'created_at', type: 'time', agg_type: 'max' }),
      measure('first_source', { sql: 'source', type: 'string', agg_type: 'min' }),
      measure('total_amount', { sql: 'amount', type: 'number', agg_type: 'sum' }),
      measure('unique_users', { sql: 'user_id', type: 'number', agg_type: 'count_distinct' }),
    ].join('\n\n');

    const views = `
views:
  - name: events_view
    cubes:
      - join_path: events
        includes: "*"
`;

    it('keeps aggregate types as they were', async () => {
      const compilers = prepareYamlCompiler(model(measures));
      await compilers.compiler.compile();

      expect(measuresMeta(compilers)).toMatchObject({
        'events.legacy_max': { type: 'number', aggType: 'max' },
        'events.legacy_sum': { type: 'number', aggType: 'sum' },
        'events.legacy_time': { type: 'time', aggType: 'time' },
      });
    });

    it('exposes the data type as type and agg_type as aggType', async () => {
      const compilers = prepareYamlCompiler(model(measures));
      await compilers.compiler.compile();

      expect(measuresMeta(compilers)).toMatchObject({
        'events.last_event_at': { type: 'time', aggType: 'max' },
        'events.first_source': { type: 'string', aggType: 'min' },
        'events.total_amount': { type: 'number', aggType: 'sum' },
        'events.unique_users': { type: 'number', aggType: 'countDistinct' },
      });
    });

    it('carries both through a view', async () => {
      const compilers = prepareYamlCompiler(model(measures, views));
      await compilers.compiler.compile();

      expect(measuresMeta(compilers, 'events_view')).toMatchObject({
        'events_view.legacy_max': { type: 'number', aggType: 'max' },
        'events_view.last_event_at': { type: 'time', aggType: 'max' },
        'events_view.first_source': { type: 'string', aggType: 'min' },
        'events_view.total_amount': { type: 'number', aggType: 'sum' },
      });
    });
  });

  describe('SQL', () => {
    const measures = [
      measure('last_event_at', { sql: 'created_at', type: 'time', agg_type: 'max' }),
      measure('total_amount', { sql: 'amount', type: 'number', agg_type: 'sum' }),
      measure('rows_count', { type: 'number', agg_type: 'count' }),
    ].join('\n\n');

    const views = `
views:
  - name: events_view
    cubes:
      - join_path: events
        includes: "*"
`;

    it('applies the aggregate to a cube measure', async () => {
      const compilers = prepareYamlCompiler(model(measures));
      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        measures: ['events.last_event_at', 'events.total_amount', 'events.rows_count'],
        dimensions: ['events.status'],
        timezone: 'UTC',
      });
      const [sql] = query.buildSqlAndParams();

      expect(sql).toMatch(/max\("events"\.created_at\)/i);
      expect(sql).toMatch(/sum\("events"\.amount\)/i);
      expect(sql).toMatch(/count\(/i);
    });

    it('applies the aggregate once through a view', async () => {
      const compilers = prepareYamlCompiler(model(measures, views));
      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        measures: ['events_view.last_event_at'],
        dimensions: ['events_view.status'],
        timezone: 'UTC',
      });
      const [sql] = query.buildSqlAndParams();

      expect(sql).toMatch(/max\("events"\.created_at\)/i);
      expect(sql).not.toMatch(/max\(\s*max\(/i);
    });
  });
});
