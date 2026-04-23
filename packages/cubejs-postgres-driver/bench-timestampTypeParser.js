/* eslint-disable */
// Quick micro-benchmark comparing the old moment-based parser to the new
// per-OID parsers. Run with: `node bench-timestampTypeParser.js` from this
// package directory (requires `yarn tsc` to have built the dist first).

const moment = require('moment');
const {
  dateTypeParser,
  timestampTypeParser,
  timestampTzTypeParser,
} = require('./dist/src/type-parsers');

const prevParser = (val) =>
  moment.utc(val).format(moment.HTML5_FMT.DATETIME_LOCAL_MS);

const cases = [
  { label: 'DATE',                    input: '2020-01-01',                    next: dateTypeParser },
  { label: 'TIMESTAMP (no frac)',     input: '2020-01-01 12:34:56',           next: timestampTypeParser },
  { label: 'TIMESTAMP (ms)',          input: '2020-01-01 12:34:56.789',       next: timestampTypeParser },
  { label: 'TIMESTAMP (us)',          input: '2020-01-01 12:34:56.123456',    next: timestampTypeParser },
  { label: 'TIMESTAMPTZ (+02)',       input: '2020-01-01 00:00:00+02',        next: timestampTzTypeParser },
  { label: 'TIMESTAMPTZ (ms +05:30)', input: '2020-06-15 08:15:30.250+05:30', next: timestampTzTypeParser },
  { label: 'TIMESTAMPTZ (us -03)',    input: '2020-06-15 08:15:30.123456-03', next: timestampTzTypeParser },
];

function sanityCheck() {
  for (const { label, input, next } of cases) {
    const prev = prevParser(input);
    const got = next(input);
    if (prev !== got) {
      console.error(`MISMATCH ${label}: moment=${prev} new=${got}`);
      process.exitCode = 1;
    }
  }
}

function bench(label, fn, val, iters) {
  for (let i = 0; i < 10000; i++) fn(val); // warmup
  const t0 = process.hrtime.bigint();
  for (let i = 0; i < iters; i++) fn(val);
  const t1 = process.hrtime.bigint();
  const ns = Number(t1 - t0);
  const perOp = ns / iters;
  const opsPerSec = (iters * 1e9) / ns;
  console.log(
    `  ${label.padEnd(8)} ${(ns / 1e6).toFixed(1).padStart(7)} ms total   ` +
    `${perOp.toFixed(0).padStart(5)} ns/op   ` +
    `${(opsPerSec / 1e6).toFixed(2).padStart(6)} Mops/s`
  );
}

function main() {
  sanityCheck();

  const iters = 2_000_000;
  console.log(`iters=${iters.toLocaleString()} per variant\n`);

  for (const { label, input, next } of cases) {
    console.log(`${label}   input=${JSON.stringify(input)}`);
    bench('moment', prevParser, input, iters);
    bench('new',    next,       input, iters);
    bench('moment', prevParser, input, iters);
    bench('new',    next,       input, iters);
    console.log('');
  }
}

main();
