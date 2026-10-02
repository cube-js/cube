import { getEnv } from '@cubejs-backend/shared';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// Join aliases are resolved by the Tesseract planner; the legacy planner is out of scope.
const tesseract = getEnv('nativeSqlPlanner');

(tesseract ? describe : describe.skip)('Join aliases', () => {
  jest.setTimeout(200000);

  const compilers = prepareYamlCompiler(`
cubes:
  - name: orders
    sql: >
      SELECT * FROM (VALUES
        (1, 1, 2, 'completed', 100, '2025-01-05'::timestamp, '2025-02-10'::timestamp),
        (2, 1, 2, 'pending',   50,  '2025-01-20'::timestamp, '2025-03-01'::timestamp),
        (3, 2, 3, 'completed', 70,  '2024-01-07'::timestamp, '2024-01-09'::timestamp),
        (4, 3, 2, 'pending',   30,  '2025-02-02'::timestamp, NULL::timestamp)
      ) AS t(id, customer_id, manager_id, status, amount, created_at, completed_at)
    joins:
      - name: users
        alias: customer
        sql: "{CUBE}.customer_id = {users}.id"
        relationship: many_to_one
      - name: users
        alias: manager
        sql: "{CUBE}.manager_id = {manager}.id"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: status
        sql: status
        type: string
      - name: created_at
        sql: created_at
        type: time
      - name: customer_city
        sql: "{customer.city}"
        type: string
    measures:
      - name: count
        type: count
      - name: total_amount
        sql: amount
        type: sum

  - name: users
    sql: >
      SELECT * FROM (VALUES
        (1, 'Berlin', 1, 10),
        (2, 'Paris',  2, 20),
        (3, 'Berlin', 2, 30)
      ) AS t(id, city, department_id, score)
    joins:
      - name: departments
        sql: "{CUBE}.department_id = {departments}.id"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: city
        sql: city
        type: string
      - name: department_name
        sql: "{departments.name}"
        type: string
    segments:
      - name: berliners
        sql: "{CUBE}.city = 'Berlin'"
    measures:
      - name: total_score
        sql: score
        type: sum

  - name: departments
    sql: >
      SELECT * FROM (VALUES (1, 'Sales'), (2, 'Ops')) AS t(id, name)
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: name
        sql: name
        type: string

  - name: employees
    sql: >
      SELECT * FROM (VALUES
        (1, 'Ann', NULL::int),
        (2, 'Bob', 1),
        (3, 'Cid', 1),
        (4, 'Dan', 2)
      ) AS t(id, name, supervisor_id)
    joins:
      - name: employees
        alias: supervisor
        sql: "{CUBE}.supervisor_id = {supervisor}.id"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: name
        sql: name
        type: string

views:
  - name: orders_view
    cubes:
      - join_path: orders
        includes:
          - count
      - join_path: orders.customer
        prefix: true
        includes:
          - city
`);

  it('joins one cube once per alias', async () => dbRunner.runQueryTest({
    measures: ['orders.count'],
    dimensions: ['orders.customer.city', 'orders.manager.city'],
    order: [{ id: 'orders.customer.city' }, { id: 'orders.manager.city' }],
    timezone: 'UTC',
  }, [
    { orders__customer__city: 'Berlin', orders__manager__city: 'Paris', orders__count: '3' },
    { orders__customer__city: 'Paris', orders__manager__city: 'Berlin', orders__count: '1' },
  ], compilers));

  it('joins a cube below an alias once per alias', async () => dbRunner.runQueryTest({
    measures: ['orders.count'],
    dimensions: ['orders.customer.departments.name', 'orders.manager.department_name'],
    order: [{ id: 'orders.customer.departments.name' }],
    timezone: 'UTC',
  }, [
    { orders__customer__departments__name: 'Ops', orders__manager__department_name: 'Ops', orders__count: '2' },
    { orders__customer__departments__name: 'Sales', orders__manager__department_name: 'Ops', orders__count: '2' },
  ], compilers));

  it('joins a cube to itself', async () => dbRunner.runQueryTest({
    dimensions: ['employees.name', 'employees.supervisor.name'],
    order: [{ id: 'employees.name' }],
    timezone: 'UTC',
  }, [
    { employees__name: 'Ann', employees__supervisor__name: null },
    { employees__name: 'Bob', employees__supervisor__name: 'Ann' },
    { employees__name: 'Cid', employees__supervisor__name: 'Ann' },
    { employees__name: 'Dan', employees__supervisor__name: 'Bob' },
  ], compilers));

  it('reads an alias in a member of the declaring cube', async () => dbRunner.runQueryTest({
    measures: ['orders.total_amount'],
    dimensions: ['orders.customer_city'],
    order: [{ id: 'orders.customer_city' }],
    timezone: 'UTC',
  }, [
    { orders__customer_city: 'Berlin', orders__total_amount: '180' },
    { orders__customer_city: 'Paris', orders__total_amount: '70' },
  ], compilers));

  it('aggregates a measure of an instance over its distinct rows', async () => dbRunner.runQueryTest({
    measures: ['orders.customer.total_score'],
    dimensions: ['orders.status'],
    order: [{ id: 'orders.status' }],
    timezone: 'UTC',
  }, [
    { orders__status: 'completed', orders__customer__total_score: '30' },
    { orders__status: 'pending', orders__customer__total_score: '40' },
  ], compilers));

  it('filters and segments an instance', async () => dbRunner.runQueryTest({
    measures: ['orders.total_amount'],
    dimensions: ['orders.customer.city'],
    filters: [{ member: 'orders.manager.city', operator: 'equals', values: ['Paris'] }],
    segments: ['orders.customer.berliners'],
    order: [{ id: 'orders.customer.city' }],
    timezone: 'UTC',
  }, [
    { orders__customer__city: 'Berlin', orders__total_amount: '180' },
  ], compilers));

  it('groups an instance time dimension by granularity', async () => dbRunner.runQueryTest({
    measures: ['orders.count'],
    timeDimensions: [{ dimension: 'orders.created_at', granularity: 'year' }],
    dimensions: ['orders.manager.city'],
    order: [{ id: 'orders.created_at' }, { id: 'orders.manager.city' }],
    timezone: 'UTC',
  }, [
    { orders__created_at_year: '2024-01-01T00:00:00.000Z', orders__manager__city: 'Berlin', orders__count: '1' },
    { orders__created_at_year: '2025-01-01T00:00:00.000Z', orders__manager__city: 'Paris', orders__count: '3' },
  ], compilers));

  it('includes members into a view through an alias', async () => dbRunner.runQueryTest({
    measures: ['orders_view.count'],
    dimensions: ['orders_view.customer_city'],
    order: [{ id: 'orders_view.customer_city' }],
    timezone: 'UTC',
  }, [
    { orders_view__customer_city: 'Berlin', orders_view__count: '3' },
    { orders_view__customer_city: 'Paris', orders_view__count: '1' },
  ], compilers));
});
