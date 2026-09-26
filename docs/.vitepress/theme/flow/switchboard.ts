// Data and geometry helpers for the homepage switchboard (HeroSwitchboard.vue).

export type Pt = [x: number, y: number];
export type Rand = () => number;
export type Op = 'insert' | 'update' | 'delete';

export interface Field {
  k: string;
  v: string;
  enrich?: boolean;
}

export interface TableDef {
  name: string;
  weight: number;
  sinks: number[];
  keyCol: string;
  keys: [min: number, max: number];
  ops?: Op[];
  id: (k: number) => string;
  sample: (k: number, r: Rand) => { raw: Field[]; doc: Field[] };
}

export const SINKS = [
  { title: 'search', product: 'meilisearch', start: 48211 },
  { title: 'vector', product: 'pgvector', start: 9736 },
  { title: 'stream', product: 'kafka', start: 131902 },
  { title: 'http', product: 'webhook', start: 2417 },
];

const pick = <T>(r: Rand, xs: T[]) => xs[Math.floor(r() * xs.length)];

const STATUSES = ['pending', 'paid', 'shipped', 'refunded'];
const NAMES = ['Ada L.', 'Grace H.', 'Linus T.', 'Barbara L.', 'Ken T.'];
const TITLES = ['Trail Pack', 'Roo Bottle', 'Pouch 2L', 'Hop Boots', 'Outback Tent'];
const SLUGS = ['slots', 'backfill', 'sinks', 'mappings', 'testing'];
const KINDS = ['checkout', 'signup', 'refund', 'login'];
const TIERS = ['bronze', 'silver', 'gold'];
const EMAILS = ['ada', 'grace', 'linus', 'ken'];

export const TABLES: TableDef[] = [
  {
    name: 'orders', weight: 4, sinks: [0, 3], keyCol: 'id', keys: [4000, 4999],
    id: k => `ord_${k}`,
    sample: (k, r) => {
      const s = Math.floor(r() * STATUSES.length);
      const c = Math.floor(r() * NAMES.length);
      return {
        raw: [{ k: 'id', v: `${k}` }, { k: 'status_code', v: `${s + 1}` }, { k: 'customer_id', v: `${80 + c}` }],
        doc: [{ k: 'id', v: `"ord_${k}"` }, { k: 'status', v: `"${STATUSES[s]}"` }, { k: 'customer', v: `"${NAMES[c]}"`, enrich: true }],
      };
    },
  },
  {
    name: 'products', weight: 2, sinks: [0, 1], keyCol: 'sku', keys: [100, 399],
    id: k => `WB-${k}`,
    sample: (k, r) => {
      const t = pick(r, TITLES);
      return {
        raw: [{ k: 'sku', v: `'WB-${k}'` }, { k: 'title', v: `'${t}'` }, { k: 'body', v: 'text 2.1kB' }],
        doc: [{ k: 'id', v: `"WB-${k}"` }, { k: 'title', v: `"${t}"` }, { k: 'embedding', v: 'float[1536]', enrich: true }],
      };
    },
  },
  {
    name: 'documents', weight: 1, sinks: [1], keyCol: 'id', keys: [900, 999],
    id: k => `doc_${k}`,
    sample: (k, r) => {
      const n = 3 + Math.floor(r() * 6);
      return {
        raw: [{ k: 'id', v: `${k}` }, { k: 'path', v: `'/kb/${pick(r, SLUGS)}.md'` }, { k: 'content', v: 'text 8.4kB' }],
        doc: [{ k: 'id', v: `"doc_${k}"` }, { k: 'chunk', v: `"${1 + Math.floor(r() * n)}/${n}"` }, { k: 'vector', v: 'float[1536]', enrich: true }],
      };
    },
  },
  {
    name: 'events', weight: 3, sinks: [2], keyCol: 'seq', keys: [55000, 59999], ops: ['insert'],
    id: k => `${k}`,
    sample: (k, r) => {
      const kind = pick(r, KINDS);
      return {
        raw: [{ k: 'seq', v: `${k}` }, { k: 'kind', v: `'${kind}'` }, { k: 'payload', v: 'jsonb 312B' }],
        doc: [{ k: 'key', v: `"${k}"` }, { k: 'type', v: `"${kind}"` }, { k: 'tenant', v: '"acme"', enrich: true }],
      };
    },
  },
  {
    name: 'customers', weight: 1, sinks: [0], keyCol: 'id', keys: [80, 84],
    id: k => `cus_${k}`,
    sample: (k, r) => {
      const t = Math.floor(r() * TIERS.length);
      const e = pick(r, EMAILS);
      return {
        raw: [{ k: 'id', v: `${k}` }, { k: 'tier', v: `${t + 1}` }, { k: 'email', v: `'${e}@acme.io'` }],
        doc: [{ k: 'id', v: `"cus_${k}"` }, { k: 'tier', v: `"${TIERS[t]}"` }, { k: 'email', v: `"${e[0]}***@acme.io"`, enrich: true }],
      };
    },
  },
];

export const OP_GLYPH: Record<Op, string> = { insert: '+', update: '~', delete: '-' };

export function pickTable(r: Rand): TableDef {
  const total = TABLES.reduce((s, t) => s + t.weight, 0);
  let x = r() * total;
  for (const t of TABLES) {
    x -= t.weight;
    if (x < 0) return t;
  }
  return TABLES[0];
}

export function pickOp(t: TableDef, r: Rand): Op {
  if (t.ops) return pick(r, t.ops);
  const x = r();
  return x < 0.3 ? 'insert' : x < 0.9 ? 'update' : 'delete';
}

// record panel rows: key column padded, value after
export function fieldLine(f: Field | undefined) {
  return f ? `${f.k.padEnd(12)}${f.v}` : '';
}

// deletes decode to the key columns only and ship as a delete by id
export function recordLines(t: TableDef, op: Op, k: number, r: Rand) {
  if (op === 'delete') {
    return {
      raw: [{ k: t.keyCol, v: `${k}` }] as Field[],
      doc: [{ k: 'delete', v: `"${t.id(k)}"` }] as Field[],
    };
  }
  return t.sample(k, r);
}

export function formatLsn(v: number) {
  const hi = Math.floor(v / 0x100000000);
  const lo = v % 0x100000000;
  return `${hi.toString(16).toUpperCase()}/${lo.toString(16).toUpperCase().padStart(8, '0')}`;
}

// small deterministic PRNG so every visit plays the same opening
export function mulberry32(seed: number): Rand {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// routes are orthogonal polylines, so segment length is |dx| + |dy|
export function pathLength(p: Pt[]) {
  let len = 0;
  for (let i = 1; i < p.length; i++) {
    len += Math.abs(p[i][0] - p[i - 1][0]) + Math.abs(p[i][1] - p[i - 1][1]);
  }
  return len;
}

export function pointAt(p: Pt[], d: number): Pt {
  let rest = Math.max(0, d);
  for (let i = 1; i < p.length; i++) {
    const [ax, ay] = p[i - 1];
    const [bx, by] = p[i];
    const seg = Math.abs(bx - ax) + Math.abs(by - ay);
    if (rest <= seg) {
      const f = seg === 0 ? 0 : rest / seg;
      return [ax + (bx - ax) * f, ay + (by - ay) * f];
    }
    rest -= seg;
  }
  return p[p.length - 1];
}
