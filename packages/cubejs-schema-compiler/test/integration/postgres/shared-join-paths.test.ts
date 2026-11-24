import { getEnv } from '@cubejs-backend/shared';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// Shared join paths are planned by Tesseract; the legacy planner rejects them.
const tesseract = getEnv('nativeSqlPlanner');

(tesseract ? describe : describe.skip)('Views sharing dimensions through join_paths', () => {
  jest.setTimeout(200000);

  const compilers = prepareYamlCompiler(`
cubes:
  - name: dates
    sql: >
      SELECT * FROM (VALUES
        ('2024-01-01'::timestamp),
        ('2024-01-02'::timestamp),
        ('2024-01-03'::timestamp)
      ) AS t(date)
    dimensions:
      - name: date
        sql: date
        type: time
        primary_key: true

  - name: orders
    sql: >
      SELECT * FROM (VALUES
        (1, '2024-01-01'::timestamp),
        (2, '2024-01-01'::timestamp),
        (3, '2024-01-02'::timestamp)
      ) AS t(id, event_date)
    joins:
      - name: dates
        alias: event_day
        sql: "{CUBE}.event_date = {event_day.date}"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
    measures:
      - name: order_count
        type: count

  - name: refunds
    sql: >
      SELECT * FROM (VALUES
        (1, '2024-01-02'::timestamp),
        (2, '2024-01-03'::timestamp)
      ) AS t(id, event_date)
    joins:
      - name: dates
        alias: event_day
        sql: "{CUBE}.event_date = {event_day.date}"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
    measures:
      - name: refund_count
        type: count

  - name: people
    sql: >
      SELECT * FROM (VALUES (1, 'Ann'), (2, 'Bob'), (3, 'Cid')) AS t(id, name)
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: name
        sql: name
        type: string

  - name: complaints
    sql: >
      SELECT * FROM (VALUES (1, 1, 3), (2, 1, 3), (3, 2, 3)) AS t(id, complainant_id, handler_id)
    joins:
      - name: people
        alias: complainant
        sql: "{CUBE}.complainant_id = {complainant.id}"
        relationship: many_to_one
      - name: people
        alias: handler
        sql: "{CUBE}.handler_id = {handler.id}"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
    measures:
      - name: count
        type: count

  - name: attendances
    sql: >
      SELECT * FROM (VALUES (1, 1), (2, 2), (3, 2), (4, 3)) AS t(id, person_id)
    joins:
      - name: people
        sql: "{CUBE}.person_id = {people.id}"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
    measures:
      - name: count
        type: count

views:
  - name: activity
    cubes:
      - join_path: orders
        includes:
          - order_count
      - join_path: refunds
        includes:
          - refund_count
      - join_paths:
          - orders.event_day
          - refunds.event_day
        includes:
          - date

  - name: complaints_and_attendance
    cubes:
      - join_path: complaints
        prefix: true
        includes:
          - count
      - join_path: attendances
        prefix: true
        includes:
          - count
      - join_paths:
          - complaints.complainant
          - attendances.people
        alias: person
        prefix: true
        includes:
          - name
`);

  it('reads one aliased role from two facts', async () => dbRunner.runQueryTest({
    measures: ['activity.order_count', 'activity.refund_count'],
    timeDimensions: [{ dimension: 'activity.date', granularity: 'day' }],
    order: [{ id: 'activity.date' }],
    timezone: 'UTC',
  }, [
    { activity__date_day: '2024-01-01T00:00:00.000Z', activity__order_count: '2', activity__refund_count: null },
    { activity__date_day: '2024-01-02T00:00:00.000Z', activity__order_count: '1', activity__refund_count: '1' },
    { activity__date_day: '2024-01-03T00:00:00.000Z', activity__order_count: null, activity__refund_count: '1' },
  ], compilers));

  it('reads an aliased path on one fact and a plain one on another', async () => dbRunner.runQueryTest({
    measures: ['complaints_and_attendance.complaints_count', 'complaints_and_attendance.attendances_count'],
    dimensions: ['complaints_and_attendance.person_name'],
    order: [{ id: 'complaints_and_attendance.person_name' }],
    timezone: 'UTC',
  }, [
    {
      complaints_and_attendance__person_name: 'Ann',
      complaints_and_attendance__complaints_count: '2',
      complaints_and_attendance__attendances_count: '1',
    },
    {
      complaints_and_attendance__person_name: 'Bob',
      complaints_and_attendance__complaints_count: '1',
      complaints_and_attendance__attendances_count: '2',
    },
    {
      complaints_and_attendance__person_name: 'Cid',
      complaints_and_attendance__complaints_count: null,
      complaints_and_attendance__attendances_count: '1',
    },
  ], compilers));

  it('binds a single fact in place', async () => dbRunner.runQueryTest({
    measures: ['activity.refund_count'],
    timeDimensions: [{ dimension: 'activity.date', granularity: 'day' }],
    order: [{ id: 'activity.date' }],
    timezone: 'UTC',
  }, [
    { activity__date_day: '2024-01-02T00:00:00.000Z', activity__refund_count: '1' },
    { activity__date_day: '2024-01-03T00:00:00.000Z', activity__refund_count: '1' },
  ], compilers));
});
