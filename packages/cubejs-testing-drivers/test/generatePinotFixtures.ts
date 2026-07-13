/* eslint-disable no-console */
/**
 * Generates the committed Apache Pinot fixtures under `fixtures/pinot/` from the
 * shared dataset (`src/dataset.ts`) — CSV data plus the per-table Pinot schema,
 * table-config and batch-ingestion jobspec. Run once and commit the output;
 * re-run only when `src/dataset.ts` changes:
 *
 *   yarn tsc && node dist/test/generatePinotFixtures.js
 *
 * Pinot cannot be seeded via SQL, so the testing-drivers harness ingests these
 * files through the controller (see src/helpers/seedPinot.ts). Tables carry the
 * fixed `_pinot` suffix so they line up with the Cube model (getSchemaPath).
 */
import fs from 'fs-extra';
import path from 'path';
import { Cast } from '../src/types/Cast';
import {
  Customers, Products, ECommerce, BigECommerce, RetailCalendar,
} from '../src/dataset';

const ROOT = path.resolve(process.cwd(), 'fixtures/pinot');
const SUFFIX = 'pinot';

const bareCast: Cast = {
  DATE_PREFIX: '',
  DATE_SUFFIX: '',
  SELECT_PREFIX: '',
  SELECT_SUFFIX: '',
  CREATE_TBL_PREFIX: '',
  CREATE_TBL_SUFFIX: '',
  CREATE_SUB_PREFIX: '',
  CREATE_SUB_SUFFIX: '',
  USE_SCHEMA: '',
};

type Row = { [col: string]: string | boolean | null };
type Parsed = { cols: string[], data: Row[] };

function splitTopLevel(str: string, sepChar: string): string[] {
  const out: string[] = [];
  let cur = '';
  let inStr = false;
  for (let i = 0; i < str.length; i++) {
    const ch = str[i];
    if (ch === '\'' && inStr && str[i + 1] === '\'') {
      cur += '\'\'';
      i += 1;
    } else if (ch === '\'') {
      inStr = !inStr;
      cur += ch;
    } else if (ch === sepChar && !inStr) {
      out.push(cur);
      cur = '';
    } else {
      cur += ch;
    }
  }
  out.push(cur);
  return out;
}

function splitUnionAll(sql: string): string[] {
  const rows: string[] = [];
  let cur = '';
  let inStr = false;
  const lower = sql.toLowerCase();
  for (let i = 0; i < sql.length; i++) {
    const ch = sql[i];
    if (ch === '\'' && inStr && sql[i + 1] === '\'') {
      cur += '\'\'';
      i += 1;
    } else if (ch === '\'') {
      inStr = !inStr;
      cur += ch;
    } else if (!inStr && lower.startsWith('union all', i)) {
      rows.push(cur);
      cur = '';
      i += 8;
    } else {
      cur += ch;
    }
  }
  rows.push(cur);
  return rows;
}

const stripSelect = (s: string) => s.replace(/^\s*select\s+/i, '').trim();

function parseItem(item: string): { value: string, alias: string | null } {
  const t = item.trim();
  const m = t.match(/\s+as\s+([a-z_][a-z0-9_]*)\s*$/i);
  if (m) return { value: t.slice(0, m.index).trim(), alias: m[1] };
  return { value: t, alias: null };
}

function parseLiteral(v: string): string | boolean | null {
  const t = v.trim();
  if (/^null$/i.test(t)) return null;
  if (/^true$/i.test(t)) return true;
  if (/^false$/i.test(t)) return false;
  if (t.startsWith('\'')) return t.slice(1, -1).replace(/''/g, '\'');
  return t;
}

function parseUnionSelect(sql: string): Parsed {
  const rows = splitUnionAll(sql).map(stripSelect);
  const cols: string[] = [];
  const data: Row[] = [];
  rows.forEach((row, idx) => {
    const items = splitTopLevel(row, ',').map(parseItem);
    if (idx === 0) items.forEach((it) => cols.push(it.alias as string));
    const rec: Row = {};
    items.forEach((it, i) => { rec[cols[i]] = parseLiteral(it.value); });
    data.push(rec);
  });
  return { cols, data };
}

function extractInner(sql: string): string {
  const fromIdx = sql.search(/\bfrom\s*\(/i);
  const open = sql.indexOf('(', fromIdx);
  const close = sql.lastIndexOf(')');
  return sql.slice(open + 1, close);
}

const toEpochMillis = (d: string) => String(Date.parse(`${d}T00:00:00.000Z`));

function csvField(v: string | boolean | null): string {
  if (v === null || v === undefined) return '';
  const s = String(v);
  return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

type Spec = {
  dim?: boolean,
  string?: string[],
  int?: string[],
  long?: string[],
  double?: string[],
  boolean?: string[],
  dateTime?: string[],
  timeColumn?: string,
  pk?: string[],
};

// Column classification per table — drives both CSV date conversion and the
// Pinot schema field specs.
const SPECS: { [table: string]: Spec } = {
  customers: { dim: true, string: ['customer_id', 'customer_name'], pk: ['customer_id'] },
  products: { dim: true, string: ['category', 'sub_category', 'product_name'], pk: ['category', 'sub_category', 'product_name'] },
  ecommerce: {
    string: ['order_id', 'customer_id', 'city', 'category', 'sub_category', 'product_name'],
    int: ['row_id'],
    long: ['quantity'],
    double: ['sales', 'discount', 'profit'],
    dateTime: ['order_date', 'completed_date'],
    timeColumn: 'order_date',
  },
  bigecommerce: {
    string: ['order_id', 'customer_id', 'city', 'category', 'sub_category', 'product_name'],
    int: ['id', 'row_id'],
    long: ['quantity'],
    double: ['sales', 'discount', 'profit'],
    boolean: ['is_returning'],
    dateTime: ['order_date', 'completed_date'],
    timeColumn: 'order_date',
  },
  retailcalendar: {
    dim: true,
    string: ['retail_year_name', 'retail_quarter_name', 'retail_month_name', 'retail_week_name'],
    dateTime: ['date_val', 'retail_year_begin_date', 'retail_quarter_begin_date', 'retail_month_begin_date',
      'retail_week_begin_date', 'retail_date_prev_month', 'retail_date_prev_quarter', 'retail_date_prev_year'],
    pk: ['date_val'],
  },
};

function buildSchema(table: string, spec: Spec): any {
  const name = `${table}_${SUFFIX}`;
  const schema: any = { schemaName: name };
  const dims = [
    ...(spec.string || []).map((n) => ({ name: n, dataType: 'STRING' })),
    ...(spec.int || []).map((n) => ({ name: n, dataType: 'INT' })),
    ...(spec.boolean || []).map((n) => ({ name: n, dataType: 'BOOLEAN' })),
  ];
  if (dims.length) schema.dimensionFieldSpecs = dims;
  const metrics = [
    ...(spec.long || []).map((n) => ({ name: n, dataType: 'LONG' })),
    ...(spec.double || []).map((n) => ({ name: n, dataType: 'DOUBLE' })),
  ];
  if (metrics.length) schema.metricFieldSpecs = metrics;
  if (spec.dateTime && spec.dateTime.length) {
    schema.dateTimeFieldSpecs = spec.dateTime.map((n) => ({
      name: n, dataType: 'TIMESTAMP', format: '1:MILLISECONDS:EPOCH', granularity: '1:MILLISECONDS',
    }));
  }
  if (spec.pk) schema.primaryKeyColumns = spec.pk;
  return schema;
}

function buildTableConfig(table: string, spec: Spec): any {
  const name = `${table}_${SUFFIX}`;
  const cfg: any = {
    tableName: name,
    tableType: 'OFFLINE',
    segmentsConfig: { schemaName: name, replication: '1' },
    tenants: { broker: 'DefaultTenant', server: 'DefaultTenant' },
    tableIndexConfig: { loadMode: 'MMAP', nullHandlingEnabled: true },
    metadata: {},
    ingestionConfig: { batchIngestionConfig: { segmentIngestionType: 'REFRESH', segmentIngestionFrequency: 'DAILY' } },
  };
  if (spec.timeColumn) cfg.segmentsConfig.timeColumnName = spec.timeColumn;
  if (spec.dim) { cfg.isDimTable = true; cfg.dimensionTableConfig = { disablePreload: false }; }
  return cfg;
}

function buildJobSpec(table: string): string {
  const name = `${table}_${SUFFIX}`;
  return `executionFrameworkSpec:
  name: 'standalone'
  segmentGenerationJobRunnerClassName: 'org.apache.pinot.plugin.ingestion.batch.standalone.SegmentGenerationJobRunner'
  segmentTarPushJobRunnerClassName: 'org.apache.pinot.plugin.ingestion.batch.standalone.SegmentTarPushJobRunner'
  segmentUriPushJobRunnerClassName: 'org.apache.pinot.plugin.ingestion.batch.standalone.SegmentUriPushJobRunner'
jobType: SegmentCreationAndTarPush
inputDirURI: '/tmp/data/test-resources/rawdata/${name}/'
includeFileNamePattern: 'glob:**/*.csv'
outputDirURI: '/tmp/data/segments/${name}/'
overwriteOutput: true
pinotFSSpecs:
  - scheme: file
    className: org.apache.pinot.spi.filesystem.LocalPinotFS
recordReaderSpec:
  dataFormat: 'csv'
  className: 'org.apache.pinot.plugin.inputformat.csv.CSVRecordReader'
  configClassName: 'org.apache.pinot.plugin.inputformat.csv.CSVRecordReaderConfig'
tableSpec:
  tableName: '${name}'
pinotClusterSpecs:
  - controllerURI: 'http://localhost:9000'
pushJobSpec:
  pushAttempts: 1
`;
}

function writeCsv(table: string, cols: string[], data: Row[], dateCols: string[]): void {
  const name = `${table}_${SUFFIX}`;
  const dir = path.join(ROOT, 'rawdata', name);
  fs.mkdirpSync(dir);
  const lines = [cols.join(',')];
  for (const rec of data) {
    lines.push(cols.map((c) => {
      let v = rec[c];
      if (dateCols.includes(c) && v !== null && v !== undefined) v = toEpochMillis(v as string);
      if (typeof v === 'boolean') v = v ? 'true' : 'false';
      return csvField(v);
    }).join(','));
  }
  fs.writeFileSync(path.join(dir, `${name}.csv`), `${lines.join('\n')}\n`);
}

function writeResources(table: string, spec: Spec): void {
  const name = `${table}_${SUFFIX}`;
  fs.writeFileSync(path.join(ROOT, `${name}.schema.json`), `${JSON.stringify(buildSchema(table, spec), null, 2)}\n`);
  fs.writeFileSync(path.join(ROOT, `${name}.table.json`), `${JSON.stringify(buildTableConfig(table, spec), null, 2)}\n`);
  fs.writeFileSync(path.join(ROOT, `${name}.jobspec.yml`), buildJobSpec(table));
}

function tableData(table: string): Parsed {
  switch (table) {
    case 'customers': return parseUnionSelect(Customers.select(bareCast));
    case 'products': return parseUnionSelect(Products.select(bareCast));
    case 'ecommerce': return parseUnionSelect(ECommerce.select(bareCast));
    case 'retailcalendar': return parseUnionSelect(RetailCalendar.select(bareCast));
    case 'bigecommerce': {
      const inner = parseUnionSelect(extractInner(BigECommerce.select(bareCast)));
      inner.data.forEach((r) => { r.id = r.row_id; });
      return { cols: ['id', ...inner.cols], data: inner.data };
    }
    default: throw new Error(`unknown table ${table}`);
  }
}

function main(): void {
  fs.mkdirpSync(ROOT);
  for (const table of Object.keys(SPECS)) {
    const spec = SPECS[table];
    const { cols, data } = tableData(table);
    writeCsv(table, cols, data, spec.dateTime || []);
    writeResources(table, spec);
    console.log(`${table}_${SUFFIX}: ${data.length} rows, cols=[${cols.join(', ')}]`);
  }
  console.log(`Pinot fixtures written to ${ROOT}`);
}

main();
