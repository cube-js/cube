/* eslint-disable no-console, no-bitwise, no-continue, prefer-destructuring, @stylistic/padding-line-between-statements */
// Streams a .heapsnapshot (too big for JSON.parse), then prints a JSON summary:
// self size by category, NativeContext count and the size each one retains
// (dominator tree over non-weak edges, like DevTools' "Retained Size").
//
//   node --max-old-space-size=12000 benchmarks/heap-snapshot-summary.js file.heapsnapshot [--top=15] [--strings=30]
//
// --strings=N adds a duplicate-string report: string nodes grouped by value, the bytes held
// by the extra copies (the ceiling for interning), and the N values/retainers with the most.

const fs = require('fs');

const file = process.argv[2];
const topArg = process.argv.find((a) => a.startsWith('--top='));
const TOP = topArg ? parseInt(topArg.split('=')[1], 10) : 15;
const stringsArg = process.argv.find((a) => a.startsWith('--strings='));
const STRINGS_TOP = stringsArg ? parseInt(stringsArg.split('=')[1], 10) : 0;

const fd = fs.openSync(file, 'r');
const CHUNK = 64 * 1024 * 1024;
const buf = Buffer.allocUnsafe(CHUNK);
let bufLen = 0;
let pos = 0;
let eof = false;

const fill = () => {
  if (pos < bufLen || eof) return;
  bufLen = fs.readSync(fd, buf, 0, CHUNK, null);
  pos = 0;
  if (bufLen === 0) eof = true;
};

const peekByte = () => {
  fill();
  return eof ? -1 : buf[pos];
};

// Advance until just past `token` (ASCII).
const skipPast = (token) => {
  const t = Buffer.from(token);
  let matched = 0;
  for (;;) {
    const b = peekByte();
    if (b < 0) throw new Error(`token ${token} not found`);
    pos++;
    if (b === t[matched]) {
      matched++;
      if (matched === t.length) return;
    } else {
      matched = b === t[0] ? 1 : 0;
    }
  }
};

const readHeader = () => {
  const head = [];
  const t = Buffer.from('"nodes":[');
  let matched = 0;
  for (;;) {
    const b = peekByte();
    pos++;
    head.push(b);
    if (b === t[matched]) {
      matched++;
      if (matched === t.length) break;
    } else {
      matched = b === t[0] ? 1 : 0;
    }
  }
  const text = Buffer.from(head).toString('utf8');
  const json = text.slice(0, text.lastIndexOf('"nodes":[')).replace(/,\s*$/, '');
  return JSON.parse(`${json}}`).snapshot;
};

// Read a flat array of non-negative integers into `out`, stopping at ']'.
const readInts = (out) => {
  let n = 0;
  let cur = 0;
  let inNum = false;
  for (;;) {
    fill();
    if (eof) throw new Error('unexpected eof');
    const end = bufLen;
    while (pos < end) {
      const b = buf[pos++];
      if (b >= 48 && b <= 57) {
        cur = cur * 10 + (b - 48);
        inNum = true;
      } else {
        if (inNum) {
          out[n++] = cur;
          cur = 0;
          inNum = false;
        }
        if (b === 93) return n;
      }
    }
  }
};

const readStrings = () => {
  const strings = [];
  let parts = null;
  let inStr = false;
  let escaped = false;
  let hasEscape = false;
  let start = 0;
  for (;;) {
    fill();
    if (eof) throw new Error('unexpected eof in strings');
    const end = bufLen;
    if (inStr) start = pos;
    while (pos < end) {
      const b = buf[pos++];
      if (!inStr) {
        if (b === 34) {
          inStr = true;
          start = pos;
          parts = [];
          hasEscape = false;
        } else if (b === 93) {
          return strings;
        }
      } else if (escaped) {
        escaped = false;
      } else if (b === 92) {
        escaped = true;
        hasEscape = true;
      } else if (b === 34) {
        parts.push(Buffer.from(buf.subarray(start, pos - 1)));
        const raw = Buffer.concat(parts).toString('utf8');
        strings.push(hasEscape ? JSON.parse(`"${raw}"`) : raw);
        inStr = false;
        parts = null;
      }
    }
    if (inStr) parts.push(Buffer.from(buf.subarray(start, end)));
  }
};

const t0 = Date.now();
const header = readHeader();
const { meta } = header;
const nodeFields = meta.node_fields;
const edgeFields = meta.edge_fields;
const nodeTypes = meta.node_types[0];
const edgeTypes = meta.edge_types[0];
const NF = nodeFields.length;
const EF = edgeFields.length;
const N_TYPE = nodeFields.indexOf('type');
const N_NAME = nodeFields.indexOf('name');
const N_SIZE = nodeFields.indexOf('self_size');
const N_EDGES = nodeFields.indexOf('edge_count');
const E_TYPE = edgeFields.indexOf('type');
const E_TO = edgeFields.indexOf('to_node');
const E_NAME = edgeFields.indexOf('name_or_index');
const PROPERTY = edgeTypes.indexOf('property');
const ELEMENT = edgeTypes.indexOf('element');
const INTERNAL = edgeTypes.indexOf('internal');
const WEAK = edgeTypes.indexOf('weak');
const SHORTCUT = edgeTypes.indexOf('shortcut');

const nodes = new Uint32Array(header.node_count * NF);
if (readInts(nodes) !== nodes.length) throw new Error('node count mismatch');
skipPast('"edges":[');
const edges = new Uint32Array(header.edge_count * EF);
const edgeLen = readInts(edges);
if (edgeLen !== edges.length) throw new Error('edge count mismatch');
skipPast('"strings":[');
const strings = readStrings();
fs.closeSync(fd);

const nodeCount = nodes.length / NF;
const firstEdge = new Uint32Array(nodeCount + 1);
for (let i = 0, e = 0; i < nodeCount; i++) {
  firstEdge[i] = e;
  e += nodes[i * NF + N_EDGES] * EF;
  firstEdge[i + 1] = e;
}

const typeOf = (i) => nodeTypes[nodes[i * NF + N_TYPE]];
const nameOf = (i) => strings[nodes[i * NF + N_NAME]];
const sizeOf = (i) => nodes[i * NF + N_SIZE];

// --- self size by category (DevTools-like grouping) ---
const category = (i) => {
  const type = typeOf(i);
  if (type !== 'string' && type !== 'concatenated string' && type !== 'sliced string') {
    const name = nameOf(i);
    if (name.startsWith('system / ')) {
      return `(system) ${name.slice(9).replace(/ \/.*$/, '')}`;
    }
  }
  switch (type) {
    case 'object':
    case 'native':
      return nameOf(i);
    case 'closure':
      return '(closure)';
    case 'code':
      return '(compiled code)';
    case 'array':
      return '(array)';
    case 'hidden':
    case 'object shape':
      return '(system)';
    case 'string':
    case 'concatenated string':
    case 'sliced string':
      return '(string)';
    default:
      return `(${type})`;
  }
};

let totalSelf = 0;
const byCat = new Map();
const nativeContexts = [];
for (let i = 0; i < nodeCount; i++) {
  const size = sizeOf(i);
  totalSelf += size;
  const cat = category(i);
  const entry = byCat.get(cat) || { count: 0, self: 0 };
  entry.count++;
  entry.self += size;
  byCat.set(cat, entry);
  if (nameOf(i).startsWith('system / NativeContext')) nativeContexts.push(i);
}

// --- dominators (Cooper-Harvey-Kennedy), root = node 0, weak/shortcut edges ignored ---
const followEdge = (e) => {
  const t = edges[e + E_TYPE];
  return t !== WEAK && t !== SHORTCUT;
};
let reachable = 0;
const postOrder = new Uint32Array(nodeCount);
const postIndex = new Int32Array(nodeCount).fill(-1);
{
  const visited = new Uint8Array(nodeCount);
  const stackNode = new Uint32Array(nodeCount);
  const stackEdge = new Uint32Array(nodeCount);
  let sp = 0;
  let po = 0;
  stackNode[0] = 0;
  stackEdge[0] = firstEdge[0];
  visited[0] = 1;
  sp = 1;
  while (sp > 0) {
    const n = stackNode[sp - 1];
    const e = stackEdge[sp - 1];
    if (e < firstEdge[n + 1]) {
      stackEdge[sp - 1] = e + EF;
      if (followEdge(e)) {
        const to = edges[e + E_TO] / NF;
        if (!visited[to]) {
          visited[to] = 1;
          stackNode[sp] = to;
          stackEdge[sp] = firstEdge[to];
          sp++;
        }
      }
    } else {
      postIndex[n] = po;
      postOrder[po++] = n;
      sp--;
    }
  }
  reachable = po;
}

// Predecessors in post-order index space (CSR).
const predCount = new Uint32Array(reachable + 1);
for (let n = 0; n < nodeCount; n++) {
  if (postIndex[n] < 0) continue;
  for (let e = firstEdge[n]; e < firstEdge[n + 1]; e += EF) {
    if (!followEdge(e)) continue;
    const to = postIndex[edges[e + E_TO] / NF];
    predCount[to + 1]++;
  }
}
for (let i = 1; i <= reachable; i++) predCount[i] += predCount[i - 1];
const preds = new Uint32Array(predCount[reachable]);
{
  const fillPos = predCount.slice(0, reachable);
  for (let n = 0; n < nodeCount; n++) {
    const from = postIndex[n];
    if (from < 0) continue;
    for (let e = firstEdge[n]; e < firstEdge[n + 1]; e += EF) {
      if (!followEdge(e)) continue;
      const to = postIndex[edges[e + E_TO] / NF];
      preds[fillPos[to]++] = from;
    }
  }
}

const rootPo = reachable - 1;
const UNDEF = 0xffffffff;
const idom = new Uint32Array(reachable).fill(UNDEF);
idom[rootPo] = rootPo;
let changed = true;
let passes = 0;
while (changed) {
  changed = false;
  passes++;
  for (let b = rootPo - 1; b >= 0; b--) {
    let newIdom = UNDEF;
    for (let p = predCount[b]; p < predCount[b + 1]; p++) {
      const pred = preds[p];
      if (idom[pred] === UNDEF) continue;
      if (newIdom === UNDEF) {
        newIdom = pred;
      } else {
        let f1 = pred;
        let f2 = newIdom;
        while (f1 !== f2) {
          while (f1 < f2) f1 = idom[f1];
          while (f2 < f1) f2 = idom[f2];
        }
        newIdom = f1;
      }
    }
    if (newIdom !== UNDEF && idom[b] !== newIdom) {
      idom[b] = newIdom;
      changed = true;
    }
  }
}

const retained = new Float64Array(reachable);
for (let b = 0; b < reachable; b++) retained[b] += sizeOf(postOrder[b]);
for (let b = 0; b < rootPo; b++) {
  if (idom[b] !== UNDEF) retained[idom[b]] += retained[b];
}

// --- duplicated strings: same value held by several string nodes ---
let stringReport;
if (STRINGS_TOP > 0) {
  const isString = (i) => {
    const type = typeOf(i);
    return type === 'string' || type === 'concatenated string' || type === 'sliced string';
  };
  // First non-weak retainer of every string: "<holder>.<edge>" names where it was stored.
  const retainer = new Int32Array(nodeCount).fill(-1);
  const retainerEdge = new Int32Array(nodeCount).fill(-1);
  for (let n = 0; n < nodeCount; n++) {
    for (let e = firstEdge[n]; e < firstEdge[n + 1]; e += EF) {
      if (!followEdge(e)) continue;
      const to = edges[e + E_TO] / NF;
      if (retainer[to] < 0) {
        retainer[to] = n;
        retainerEdge[to] = e;
      }
    }
  }
  const edgeLabel = (e) => {
    const t = edges[e + E_TYPE];
    if (t === ELEMENT) return '[]';
    if (t === PROPERTY || t === INTERNAL || edgeTypes[t] === 'context') return `.${strings[edges[e + E_NAME]]}`;
    return `(${edgeTypes[t]})`;
  };
  const retainerLabel = (i) => {
    const r = retainer[i];
    if (r < 0) return '(none)';
    const rt = typeOf(r);
    let holder = rt === 'object' || rt === 'native' ? nameOf(r) : `(${rt})`;
    if (holder.startsWith('system / ')) holder = holder.replace(/ \/.*$/, '');
    return `${holder}${edgeLabel(retainerEdge[i])}`;
  };

  // "holder.edge <- holder.edge <- ..." up to `depth` first-retainer hops, to find who keeps a string.
  const chain = (i, depth = 6) => {
    const parts = [];
    let cur = i;
    for (let d = 0; d < depth && retainer[cur] >= 0 && retainer[cur] !== 0; d++) {
      parts.push(retainerLabel(cur));
      cur = retainer[cur];
    }
    return parts.join(' <- ');
  };

  // Cons strings ("a" + b) not yet flattened: a tree of 32-byte nodes per value.
  const consByRetainer = new Map();
  let consBytes = 0;
  for (let i = 0; i < nodeCount; i++) {
    if (typeOf(i) !== 'concatenated string' || postIndex[i] < 0) continue;
    consBytes += sizeOf(i);
    const r = retainer[i];
    if (r >= 0 && typeOf(r) === 'concatenated string') continue;
    const label = retainerLabel(i);
    const entry = consByRetainer.get(label) || { roots: 0, retained: 0, sample: i };
    entry.roots++;
    entry.retained += retained[postIndex[i]];
    consByRetainer.set(label, entry);
  }

  const byValue = new Map();
  let stringNodes = 0;
  let stringBytes = 0;
  for (let i = 0; i < nodeCount; i++) {
    if (!isString(i) || postIndex[i] < 0) continue;
    const type = typeOf(i);
    // A cons/sliced string's own size is its header, and its name is the flattened value:
    // group only flat strings, so each value's bytes are counted once.
    if (type !== 'string') continue;
    stringNodes++;
    const size = sizeOf(i);
    stringBytes += size;
    const value = nameOf(i);
    let entry = byValue.get(value);
    if (!entry) {
      entry = { count: 0, bytes: 0, size, sample: i };
      byValue.set(value, entry);
    }
    entry.count++;
    entry.bytes += size;
  }
  let dupValues = 0;
  let dupExtraCopies = 0;
  let dupExtraBytes = 0;
  const byRetainer = new Map();
  for (const [, entry] of byValue) {
    if (entry.count < 2) continue;
    dupValues++;
    dupExtraCopies += entry.count - 1;
    dupExtraBytes += entry.bytes - entry.size;
  }
  // Retainer of each extra copy (second pass over nodes, only for duplicated values).
  const seenOnce = new Set();
  for (let i = 0; i < nodeCount; i++) {
    if (typeOf(i) !== 'string' || postIndex[i] < 0) continue;
    const value = nameOf(i);
    const entry = byValue.get(value);
    if (entry.count < 2) continue;
    if (!seenOnce.has(value)) {
      seenOnce.add(value);
      continue;
    }
    const label = retainerLabel(i);
    const r = byRetainer.get(label) || { copies: 0, bytes: 0, sample: i };
    r.copies++;
    r.bytes += sizeOf(i);
    byRetainer.set(label, r);
  }
  const clip = (v) => (v.length > 80 ? `${v.slice(0, 77)}...` : v);
  stringReport = {
    stringNodes,
    stringMb: +(stringBytes / 1024 / 1024).toFixed(2),
    distinctValues: byValue.size,
    duplicatedValues: dupValues,
    extraCopies: dupExtraCopies,
    extraCopiesMb: +(dupExtraBytes / 1024 / 1024).toFixed(2),
    topValues: [...byValue.entries()]
      .filter(([, e]) => e.count > 1)
      .sort((a, b) => (b[1].bytes - b[1].size) - (a[1].bytes - a[1].size))
      .slice(0, STRINGS_TOP)
      .map(([v, e]) => ({ value: clip(v), copies: e.count, extraKb: +((e.bytes - e.size) / 1024).toFixed(1), retainer: retainerLabel(e.sample) })),
    topRetainersOfExtraCopies: [...byRetainer.entries()]
      .sort((a, b) => b[1].bytes - a[1].bytes)
      .slice(0, STRINGS_TOP)
      .map(([name, r]) => ({ retainer: name, copies: r.copies, mb: +(r.bytes / 1024 / 1024).toFixed(2), chain: chain(r.sample), sample: clip(nameOf(r.sample)) })),
    consStringMb: +(consBytes / 1024 / 1024).toFixed(2),
    topConsStringRoots: [...consByRetainer.entries()]
      .sort((a, b) => b[1].retained - a[1].retained)
      .slice(0, STRINGS_TOP)
      .map(([name, e]) => ({ retainer: name, roots: e.roots, retainedMb: +(e.retained / 1024 / 1024).toFixed(2), chain: chain(e.sample), sample: clip(nameOf(e.sample)) })),
  };
}

const MB = 1024 * 1024;
const round = (x) => +(x / MB).toFixed(2);
const contexts = nativeContexts
  .map((n) => ({ retainedMb: postIndex[n] >= 0 ? round(retained[postIndex[n]]) : 0 }))
  .sort((a, b) => b.retainedMb - a.retainedMb);
const ctxRetained = contexts.map((c) => c.retainedMb);
const sortedAsc = [...ctxRetained].sort((a, b) => a - b);

// What one NativeContext (the second largest: a per-compile realm when there are several) dominates.
let contextBreakdown;
if (nativeContexts.length > 1) {
  const ranked = nativeContexts
    .filter((n) => postIndex[n] >= 0)
    .sort((a, b) => retained[postIndex[b]] - retained[postIndex[a]]);
  const ctxPo = postIndex[ranked[1]];
  const inCtx = new Uint8Array(reachable);
  const cats = new Map();
  for (let b = rootPo; b >= 0; b--) {
    if (b === ctxPo || (b !== rootPo && idom[b] !== UNDEF && inCtx[idom[b]])) {
      inCtx[b] = 1;
      const cat = category(postOrder[b]);
      cats.set(cat, (cats.get(cat) || 0) + sizeOf(postOrder[b]));
    }
  }
  contextBreakdown = [...cats.entries()].sort((a, b) => b[1] - a[1]).slice(0, 10)
    .map(([name, self]) => ({ name, selfMb: +(self / 1024 / 1024).toFixed(3) }));
}

const top = [...byCat.entries()]
  .sort((a, b) => b[1].self - a[1].self)
  .slice(0, TOP)
  .map(([name, v]) => ({ name, count: v.count, selfMb: round(v.self) }));

console.log(JSON.stringify({
  file,
  nodes: nodeCount,
  edges: edgeLen / EF,
  totalSelfMb: round(totalSelf),
  reachableSelfMb: round(retained[rootPo]),
  nativeContexts: nativeContexts.length,
  nativeContextRetainedMb: {
    largest: ctxRetained[0],
    medianOfOthers: sortedAsc.length > 1 ? sortedAsc[Math.floor((sortedAsc.length - 1) / 2)] : null,
    sumExcludingLargest: +ctxRetained.slice(1).reduce((a, b) => a + b, 0).toFixed(2),
    all: ctxRetained.length <= 40 ? ctxRetained : undefined,
  },
  contextBreakdown,
  top,
  strings: stringReport,
  dominatorPasses: passes,
  seconds: (Date.now() - t0) / 1000,
}, null, 1));
