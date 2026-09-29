<script setup lang="ts">
import { nextTick, onMounted, onUnmounted, reactive, ref } from 'vue';
import { formatCount } from './flow/format';
import FlowChip from './flow/FlowChip.vue';
import { cubicBezier, type Timeline } from 'animejs';
import { phaseTimeline, type Cue, type Tween } from './flow/timeline';
import { useTimelineLoop } from './flow/useTimelineLoop';
import {
  OP_GLYPH, SINKS, fieldLine, formatLsn, mulberry32, pathLength,
  pickOp, pickTable, pointAt, recordLines, type Field, type Op, type Pt, type TableDef,
} from './flow/switchboard';

// The hero "image": a slow-motion model of the engine, played as one
// wave per cycle so only one thing moves at a time. A transaction is
// written to the WAL tape and commits; its changes cross into wallaby as
// a train of packets and park in the batch lane (decode); the first
// change is followed in the record panel as its raw row becomes a
// document (transform); the batch fans out to the sinks each table maps
// to (deliver); an ack returns to postgres, the slot's flush position
// advances and the tape markers turn blue. Then a short rest.
// Each cycle is one anime.js timeline (flow/timeline.ts): tweens move the
// packets and scramble the record text, cues change the readouts. DOM
// holds the text; one canvas, redrawn on each timeline update, holds the
// rails and packets. Telemetry is fake, so the whole widget is hidden
// from assistive tech.

// cycle timeline, ms
const WRITE_GAP = 110; // between tape lines
const IN_MS = 700; // postgres to lane slot
const TRAIN_GAP = 90; // between packets in a train
const HOLD_MS = 900; // parked while the transform shows
const OUT_MS = 900; // lane slot to sink
const OUT_GAP = 60;
const SETTLE_MS = 150; // pause after the commit, and after the last landing
const ACK_MS = 550;
const SCRAMBLE_MS = 420;
const RIPPLE_MS = 450;
const REST_MS = 700;

const SLOTS = 8;
const TRAIL = 6; // comet tail samples
const TRAIL_STEP = 17; // ms between tail samples
const SLOW_MO = 40; // displayed rate and lag are scaled back to real time
const TAPE_MAX = 16;
const GLYPHS = '0123456789ABCDEF#*+=<>/';
const easeInOut = cubicBezier(0.42, 0, 0.58, 1);

interface TapeLine {
  id: number;
  kind: 'begin' | 'change' | 'commit';
  text: string;
  glyph?: string;
  key?: string;
  lsn: number;
}

interface Change {
  table: TableDef;
  op: Op;
  key: number;
  lsn: number;
  raw: Field[];
  doc: Field[];
}

interface Packet {
  c: Change;
  slot: number;
  leg: 'wait' | 'in' | 'parked' | 'out' | 'gone';
  u: number; // linear progress along the current leg
}

// ---- deterministic opening state (SSR hydration) ----

const BASE_LSN = 0x16b3762a94;
let lineId = 0;

function seedTape(): TapeLine[] {
  const rows: [TapeLine['kind'], string, string?, string?][] = [
    ['begin', 'begin 88207'], ['change', 'orders', '~', '4811'], ['change', 'events', '+', '55118'], ['commit', 'commit'],
    ['begin', 'begin 88208'], ['change', 'products', '~', '219'], ['commit', 'commit'],
    ['begin', 'begin 88209'], ['change', 'events', '+', '55119'], ['change', 'orders', '-', '4702'],
    ['change', 'customers', '~', '82'], ['commit', 'commit'],
  ];
  return rows.map(([kind, text, glyph, key], i) => ({
    id: lineId++, kind, text, glyph, key, lsn: BASE_LSN - (rows.length - i) * 0x2c0,
  }));
}

const tape = ref<TapeLine[]>(seedTape());
const walLsn = ref(BASE_LSN);
const flushLsn = ref(BASE_LSN);
const pgFlash = ref(false);
const stage = ref(0); // 0 idle, 1 decode, 2 transform, 3 deliver
const rec = reactive({
  lines: ['~ UPDATE orders 4811', 'id          "ord_4811"', 'status      "shipped"', 'customer    "Grace H."'],
  blue: [false, false, false, true],
  stale: true,
});
const sinks = reactive(SINKS.map((s, i) => ({
  ...s,
  count: s.start,
  flash: false,
  fresh: false,
  meter: [3, 5, 2, 6, 4, 1, 5, 3, 6, 2, 4, 5].map(m => (m + i * 2) % 7 + 1),
})));
const batchCount = ref(0);
const flushing = ref(false);
const rate = ref(41);
const lag = ref(72);
const hoverSink = ref(-1);
const mode = ref<'wide' | 'tall'>('wide');

const root = ref<HTMLElement>();
const pgChip = ref<{ $el: HTMLElement }>();
const coreChip = ref<{ $el: HTMLElement }>();
const laneEl = ref<HTMLElement>();
const sinkEls: HTMLElement[] = [];
const canvas = ref<HTMLCanvasElement>();

function setSinkRef(el: unknown, i: number) {
  const node = (el as { $el?: HTMLElement } | null)?.$el;
  if (node) sinkEls[i] = node;
}

// ---- simulation ----

let rnd = mulberry32(0x5eed);
let lsnNum = BASE_LSN;
let xid = 88209;
let packets: Packet[] = [];
let ackU = 0; // ack progress back up to postgres, drawn while between 0 and 1
let ripples: { sink: number; at: number; t: number }[] = [];

function addTape(line: Omit<TapeLine, 'id'>) {
  tape.value.push({ id: lineId++, ...line });
  if (tape.value.length > TAPE_MAX) tape.value.splice(0, tape.value.length - TAPE_MAX);
}

function newChange(): Change {
  const table = pickTable(rnd);
  const op = pickOp(table, rnd);
  const key = table.keys[0] + Math.floor(rnd() * (table.keys[1] - table.keys[0] + 1));
  const { raw, doc } = recordLines(table, op, key, rnd);
  return { table, op, key, lsn: 0, raw, doc };
}

function writeChange(c: Change) {
  lsnNum += 0x80 + Math.floor(rnd() * 0x600);
  c.lsn = lsnNum;
  walLsn.value = lsnNum;
  addTape({ kind: 'change', text: c.table.name, glyph: OP_GLYPH[c.op], key: `${c.key}`, lsn: lsnNum });
}

// a highlight switched on at offset and off again ms later
function pulse(offset: number, set: (on: boolean) => void, ms = 320): Cue[] {
  return [[offset, () => set(true)], [offset + ms, () => set(false)]];
}

// the record panel's lines scramble from whatever they show into `to`
function scramble(offset: number, to: string[]): Tween[] {
  return to.map((line, i): Tween => {
    let from: string | undefined;
    let res: number[] = [];
    return [offset, SCRAMBLE_MS, v => {
      if (from === undefined) {
        from = rec.lines[i];
        const n = Math.max(from.length, line.length);
        res = Array.from({ length: n }, (_, c) => (c / n) * 0.5 + rnd() * 0.5);
      }
      rec.lines[i] = v >= 1 ? line : scrambled(from, line, res, v);
    }];
  });
}

function scrambled(from: string, to: string, res: number[], p: number) {
  let out = '';
  for (let c = 0; c < res.length; c++) {
    const a = to[c] ?? ' ';
    const b = from[c] ?? ' ';
    out += p >= res[c] ? a : a === ' ' && b === ' ' ? ' ' : GLYPHS[Math.floor(Math.random() * GLYPHS.length)];
  }
  return out.trimEnd();
}

// a packet's progress along one leg of its trip; easing is applied when
// drawing, so the comet tail can sample earlier points on the leg
function travel(p: Packet, leg: 'in' | 'out') {
  return (v: number) => {
    p.leg = v < 1 ? leg : leg === 'in' ? 'parked' : 'gone';
    p.u = v;
  };
}

// One wave per cycle, as back-to-back phases, so the stages always read
// in the same order and rhythm and only one thing moves at a time.
function startCycle(lead: number): Timeline {
  const changes = Array.from({ length: 2 + Math.floor(rnd() * 3) }, newChange);
  const n = changes.length;
  const head = changes[0];
  const title = `${OP_GLYPH[head.op]} ${head.op.toUpperCase()} ${head.table.name} ${head.key}`;
  packets = changes.map((c, j) => ({ c, slot: j, leg: 'wait', u: 0 }));
  // one per copy landing on a sink, timed from the start of deliver
  ripples = changes.flatMap((c, j) => c.table.sinks.map(sink => ({ sink, at: j * OUT_GAP + OUT_MS, t: 0 })));
  ackU = 0;

  // each sink flashes from its first landing to its last, then logs the batch
  const sinkCues: Cue[] = [];
  SINKS.forEach((_, s) => {
    const hits = ripples.filter(r => r.sink === s).map(r => r.at);
    if (!hits.length) return;
    const first = hits[0];
    const last = hits[hits.length - 1];
    const sink = sinks[s];
    sinkCues.push(
      ...pulse(first, on => (sink.flash = on), last - first + 320),
      [last, () => (sink.meter = [...sink.meter.slice(1), hits.length])],
      ...pulse(last, on => (sink.fresh = on), 600),
    );
  });

  const tl: Timeline = phaseTimeline([
    { name: 'lead', ms: lead },
    // begin, one tape line per change
    {
      name: 'write',
      ms: WRITE_GAP * (n + 1),
      run: () => addTape({ kind: 'begin', text: `begin ${++xid}`, lsn: lsnNum }),
      at: changes.map((c, j): Cue => [WRITE_GAP * (j + 1), () => writeChange(c)]),
    },
    // changes only leave postgres once the transaction commits
    {
      name: 'commit',
      ms: SETTLE_MS,
      run: () => {
        lsnNum += 0x30;
        walLsn.value = lsnNum;
        addTape({ kind: 'commit', text: 'commit', lsn: lsnNum });
      },
    },
    // the train crosses into wallaby and parks in the lane
    {
      name: 'decode',
      ms: (n - 1) * TRAIN_GAP + IN_MS,
      at: [
        [IN_MS * 0.4, () => {
          stage.value = 1;
          rec.stale = false;
          rec.blue = [false, false, false, false];
        }],
        ...packets.map((_, j): Cue => [j * TRAIN_GAP + IN_MS, () => (batchCount.value += 1)]),
      ],
      tweens: [
        ...packets.map((p, j): Tween => [j * TRAIN_GAP, IN_MS, travel(p, 'in')]),
        ...scramble(IN_MS * 0.4, [title, ...[0, 1, 2].map(i => fieldLine(head.raw[i]))]),
      ],
    },
    // the batch holds while the followed record changes shape
    {
      name: 'transform',
      ms: HOLD_MS,
      run: () => {
        stage.value = 2;
        rec.blue = [false, ...[0, 1, 2].map(i => !!head.doc[i]?.enrich)];
      },
      tweens: scramble(0, [title, ...[0, 1, 2].map(i => fieldLine(head.doc[i]))]),
    },
    // the batch fans out, each copy landing on its sink
    {
      name: 'deliver',
      ms: (n - 1) * OUT_GAP + OUT_MS + SETTLE_MS,
      run: () => {
        stage.value = 3;
        batchCount.value = 0;
      },
      at: [
        ...pulse(0, on => (flushing.value = on), 260),
        ...ripples.map((r): Cue => [r.at, () => (sinks[r.sink].count += 1)]),
        ...sinkCues,
      ],
      tweens: [
        ...packets.map((p, j): Tween => [j * OUT_GAP, OUT_MS, travel(p, 'out')]),
        ...ripples.map((r): Tween => [r.at, RIPPLE_MS, v => (r.t = v)]),
      ],
    },
    // back to postgres; lag runs from the commit to here
    {
      name: 'ack',
      ms: ACK_MS,
      run: () => (lag.value = Math.round((tl.labels.ack - tl.labels.commit) / SLOW_MO)),
      tweens: [[0, ACK_MS, v => (ackU = v)]],
    },
    // the slot confirms, the tape markers turn blue
    {
      name: 'rest',
      ms: REST_MS,
      run: () => {
        stage.value = 0;
        rec.stale = true;
        flushLsn.value = lsnNum;
        rate.value = Math.round(0.7 * rate.value + 0.3 * (n / (tl.duration / 1000)) * SLOW_MO);
      },
      at: pulse(0, on => (pgFlash.value = on)),
    },
  ], { onUpdate: draw });
  return tl;
}

// ---- geometry, measured from the DOM ----

interface Geo {
  w: number;
  h: number;
  inRoute: Pt[];
  ackRoute: Pt[];
  laneLeft: number;
  laneRight: number;
  laneY: number;
  tails: Pt[][]; // per sink, from the lane's right edge onward
}

let geo: Geo | null = null;
let dpr = 1;
let ctx: CanvasRenderingContext2D | null = null;
const col = { amber: '', blue: '', rail: '', slot: '', dark: true };

function box(el: Element, o: DOMRect) {
  const r = el.getBoundingClientRect();
  return {
    l: Math.round(r.left - o.left),
    t: Math.round(r.top - o.top),
    r: Math.round(r.right - o.left),
    b: Math.round(r.bottom - o.top),
  };
}

async function layout() {
  const el = root.value;
  if (!el) return;
  const want = el.clientWidth < 500 ? 'tall' : 'wide';
  if (want !== mode.value) {
    mode.value = want;
    await nextTick();
  }
  const o = el.getBoundingClientRect();
  const w = Math.round(o.width);
  const h = Math.round(o.height);
  const pg = box(pgChip.value!.$el, o);
  const core = box(coreChip.value!.$el, o);
  const ln = box(laneEl.value!, o);
  const sk = sinkEls.map(s => box(s, o));
  const laneY = Math.round((ln.t + ln.b) / 2);

  let inRoute: Pt[];
  let tails: Pt[][];
  if (mode.value === 'wide') {
    const busX = Math.round((core.r + sk[0].l) / 2);
    inRoute = [[pg.r, laneY], [ln.l, laneY]];
    tails = sk.map(s => {
      const cy = Math.round((s.t + s.b) / 2);
      return [[busX, laneY], [busX, cy], [s.l, cy]];
    });
  } else {
    // gutters either side of the inset core and sink grid
    const gL = Math.round(core.l / 2);
    const gR = Math.round((core.r + w) / 2);
    const busY = Math.round((core.b + Math.min(...sk.map(s => s.t))) / 2);
    inRoute = [[gL, pg.b], [gL, laneY], [ln.l, laneY]];
    tails = sk.map(s => {
      const cy = Math.round((s.t + s.b) / 2);
      return s.l > w / 2
        ? [[gR, laneY], [gR, cy], [s.r, cy]]
        : [[gR, laneY], [gR, busY], [gL, busY], [gL, cy], [s.l, cy]];
    });
  }
  geo = {
    w, h, inRoute, ackRoute: [...inRoute].reverse(), tails, laneY, laneLeft: ln.l, laneRight: ln.r,
  };

  dpr = Math.min(2, window.devicePixelRatio || 1);
  const c = canvas.value!;
  c.width = Math.round(w * dpr);
  c.height = Math.round(h * dpr);
  c.style.width = `${w}px`;
  c.style.height = `${h}px`;
  ctx = c.getContext('2d');
  ctx?.setTransform(dpr, 0, 0, dpr, 0, 0);
  if (!loop.isRunning()) drawStatic();
}

function readColors() {
  const cs = getComputedStyle(root.value!);
  col.amber = cs.getPropertyValue('--vp-c-brand-1').trim();
  col.blue = cs.getPropertyValue('--wb-accent-blue').trim();
  col.rail = cs.getPropertyValue('--vp-c-divider').trim();
  col.slot = cs.getPropertyValue('--vp-c-border').trim();
  col.dark = document.documentElement.classList.contains('dark');
}

// train position i parks at slot i, counted from the lane's right end
function slotX(i: number) {
  const w = geo!.laneRight - geo!.laneLeft;
  return geo!.laneLeft + Math.round(w - 8 - i * ((w - 16) / (SLOTS - 1)));
}

function inPath(slot: number): Pt[] {
  return [...geo!.inRoute, [slotX(slot), geo!.laneY]];
}

function outPath(slot: number, sink: number): Pt[] {
  return [[slotX(slot), geo!.laneY], ...geo!.tails[sink]];
}

// ---- drawing ----

function hline(c: CanvasRenderingContext2D, x1: number, x2: number, y: number) {
  c.fillRect(Math.min(x1, x2), y, Math.abs(x2 - x1) + 1, 1);
}

function vline(c: CanvasRenderingContext2D, x: number, y1: number, y2: number) {
  c.fillRect(x, Math.min(y1, y2), 1, Math.abs(y2 - y1) + 1);
}

function polyline(c: CanvasRenderingContext2D, p: Pt[]) {
  for (let i = 1; i < p.length; i++) {
    if (p[i][1] === p[i - 1][1]) hline(c, p[i - 1][0], p[i][0], p[i][1]);
    else vline(c, p[i][0], p[i - 1][1], p[i][1]);
  }
}

function outline(c: CanvasRenderingContext2D, x: number, y: number, s: number) {
  hline(c, x, x + s - 1, y);
  hline(c, x, x + s - 1, y + s - 1);
  vline(c, x, y, y + s - 1);
  vline(c, x + s - 1, y, y + s - 1);
}

function px(v: number) {
  return Math.round(v * dpr) / dpr;
}

function square(c: CanvasRenderingContext2D, [x, y]: Pt, color: string, size = 5) {
  const half = (size - 1) / 2;
  c.fillStyle = color;
  c.fillRect(px(Math.round(x) - half), px(Math.round(y) - half), size, size);
}

// reticle: four corner brackets around the followed packet
function reticle(c: CanvasRenderingContext2D, [x, y]: Pt) {
  const cx = Math.round(x);
  const cy = Math.round(y);
  const r = 7;
  const arm = 3;
  c.fillStyle = col.amber;
  for (const [sx, sy] of [[-1, -1], [1, -1], [-1, 1], [1, 1]]) {
    const ex = cx + sx * r;
    const ey = cy + sy * r;
    hline(c, ex, ex - sx * arm, ey);
    vline(c, ex, ey, ey - sy * arm);
  }
}

// a packet eased along a leg lasting `ms`: head plus a tail drawn where it
// was a few steps earlier, so the tail shortens as the leg eases in and out
function comet(c: CanvasRenderingContext2D, path: Pt[], u: number, ms: number, color: string, size = 5) {
  const len = pathLength(path);
  for (let j = TRAIL; j >= 1; j--) {
    const back = u - (j * TRAIL_STEP) / ms;
    if (back < 0) continue;
    c.globalAlpha = 0.5 * (1 - j / (TRAIL + 1));
    square(c, pointAt(path, easeInOut(back) * len), color, j > 2 ? 3 : Math.min(size, 5));
  }
  c.globalAlpha = 1;
  c.shadowBlur = col.dark ? 6 * dpr : 0;
  c.shadowColor = color;
  const head = pointAt(path, easeInOut(u) * len);
  square(c, head, color, size);
  c.shadowBlur = 0;
  return head;
}

function drawRails(c: CanvasRenderingContext2D) {
  const g = geo!;
  c.clearRect(0, 0, g.w, g.h);
  c.fillStyle = col.rail;
  polyline(c, g.inRoute);
  hline(c, g.laneLeft, g.laneRight, g.laneY);
  for (const t of g.tails) polyline(c, [[g.laneRight, g.laneY], ...t]);

  if (hoverSink.value >= 0) {
    c.fillStyle = col.amber;
    c.globalAlpha = 0.55;
    polyline(c, g.inRoute);
    hline(c, g.laneLeft, g.laneRight, g.laneY);
    polyline(c, [[g.laneRight, g.laneY], ...g.tails[hoverSink.value]]);
    c.globalAlpha = 1;
  }

  c.fillStyle = col.slot;
  for (let i = 0; i < SLOTS; i++) outline(c, slotX(i) - 4, g.laneY - 4, 9);
}

function draw() {
  if (!geo || !ctx) return;
  const g = geo;
  const c = ctx;
  drawRails(c);

  for (const rp of ripples) {
    if (rp.t <= 0 || rp.t >= 1) continue;
    const [x, y] = g.tails[rp.sink][g.tails[rp.sink].length - 1];
    const s = Math.round(5 + rp.t * 16) | 1;
    c.globalAlpha = 1 - rp.t;
    c.fillStyle = col.blue;
    outline(c, Math.round(x) - (s - 1) / 2, Math.round(y) - (s - 1) / 2, s);
  }
  c.globalAlpha = 1;

  packets.forEach((p, j) => {
    let at: Pt | null = null;
    if (p.leg === 'in') {
      at = comet(c, inPath(p.slot), p.u, IN_MS, col.amber);
    } else if (p.leg === 'parked') {
      at = [slotX(p.slot), g.laneY];
      c.shadowBlur = col.dark ? 6 * dpr : 0;
      c.shadowColor = col.amber;
      square(c, at, col.amber);
      c.shadowBlur = 0;
    } else if (p.leg === 'out') {
      const heads = p.c.table.sinks.map(s => comet(c, outPath(p.slot, s), p.u, OUT_MS, col.amber));
      at = heads[0];
    }
    if (j === 0 && at) reticle(c, at);
  });

  if (ackU > 0 && ackU < 1) comet(c, g.ackRoute, ackU, ACK_MS, col.blue, 3);
}

// reduced motion, or before the loop starts: rails plus a parked batch
function drawStatic() {
  if (!geo || !ctx) return;
  drawRails(ctx);
  for (let i = 0; i < 3; i++) square(ctx, [slotX(i), geo.laneY], col.amber);
}

// ---- lifecycle ----

// plays while on screen in a visible tab; pausing freezes the cycle mid-wave
const loop = useTimelineLoop(root, first => startCycle(first ? 400 : 0));

let resizeObserver: ResizeObserver | undefined;
let themeObserver: MutationObserver | undefined;

onMounted(() => {
  rnd = mulberry32(0x5eed);
  readColors();
  layout();
  document.fonts?.ready.then(layout);
  resizeObserver = new ResizeObserver(() => layout());
  resizeObserver.observe(root.value!);
  themeObserver = new MutationObserver(() => {
    readColors();
    if (!loop.isRunning()) drawStatic();
  });
  themeObserver.observe(document.documentElement, { attributes: true, attributeFilter: ['class'] });
});

onUnmounted(() => {
  resizeObserver?.disconnect();
  themeObserver?.disconnect();
});
</script>

<template>
  <div ref="root" class="wb-sb" :class="`is-${mode}`" aria-hidden="true">
    <FlowChip ref="pgChip" class="wb-sb-pg" :flash="pgFlash">
      <div class="wb-chip-title">postgres</div>
      <div class="wb-sb-row">
        <span>wal</span><span class="wb-chip-val">{{ formatLsn(walLsn) }}</span>
      </div>
      <div class="wb-sb-row">
        <span>flush</span><span class="wb-chip-val">{{ formatLsn(flushLsn) }}</span>
      </div>
      <TransitionGroup tag="ol" name="wb-tape" class="wb-sb-tape">
        <li
          v-for="l in tape"
          :key="l.id"
          :class="[`is-${l.kind}`, { 'is-acked': l.kind === 'change' && l.lsn <= flushLsn }]"
        >
          <template v-if="l.kind === 'change'">
            <i class="wb-sb-sq" />{{ l.glyph }} {{ l.text }} <span class="wb-sb-dim">{{ l.key }}</span>
          </template>
          <template v-else>{{ l.text }}</template>
        </li>
      </TransitionGroup>
    </FlowChip>

    <FlowChip ref="coreChip" class="wb-sb-core" :class="{ 'is-processing': stage > 0 }">
      <div class="wb-chip-title wb-sb-row">
        <span class="wb-sb-amber">wallaby</span>
        <span class="wb-sb-state">► streaming</span>
      </div>
      <div class="wb-sb-row">
        <span>slot wallaby_cdc</span><span>lag {{ lag }}ms</span>
      </div>
      <div class="wb-sb-row wb-sb-stages">
        <span>
          <span :class="{ 'is-on': stage === 1 }">decode</span> ▸
          <span :class="{ 'is-on': stage === 2 }">transform</span> ▸
          <span :class="{ 'is-on': stage === 3 }">deliver</span>
        </span>
      </div>
      <div class="wb-sb-rec" :class="{ 'is-stale': rec.stale }">
        <div
          v-for="(line, i) in rec.lines"
          :key="i"
          :class="{ 'is-head': i === 0, 'is-blue': rec.blue[i] }"
        >{{ line || ' ' }}</div>
      </div>
      <div ref="laneEl" class="wb-sb-lane" />
      <div class="wb-sb-row">
        <span :class="{ 'wb-sb-amber': flushing }">batch {{ batchCount }}/{{ SLOTS }}</span>
        <span>{{ rate }} changes/s</span>
      </div>
    </FlowChip>

    <div class="wb-sb-sinks">
      <FlowChip
        v-for="(s, i) in sinks"
        :key="s.title"
        :ref="el => setSinkRef(el, i)"
        class="wb-sb-sink"
        :flash="s.flash"
        :lit="hoverSink === i"
        @mouseenter="hoverSink = i"
        @mouseleave="hoverSink = -1"
      >
        <div class="wb-sb-row wb-sb-sink-head">
          <span class="wb-chip-title">{{ s.title }}</span>
          <span class="wb-chip-val">{{ formatCount(s.count) }}</span>
        </div>
        <div class="wb-sb-row">{{ s.product }}</div>
        <div class="wb-sb-meter">
          <i
            v-for="(m, j) in s.meter"
            :key="j"
            :class="{ 'is-new': s.fresh && j === s.meter.length - 1 }"
            :style="{ height: `${1 + m}px` }"
          />
        </div>
      </FlowChip>
    </div>

    <canvas ref="canvas" class="wb-sb-canvas" />
  </div>
</template>

<style scoped>
.wb-sb {
  position: relative;
  width: 100%;
  max-width: 560px;
  font-family: var(--vp-font-family-mono);
  text-align: left;
  user-select: none;
}

.wb-sb.is-wide {
  display: grid;
  grid-template-columns: minmax(0, 136fr) 20px minmax(0, 214fr) 30px minmax(0, 128fr);
  height: 316px;
}

.is-wide .wb-sb-pg {
  grid-column: 1;
}

.is-wide .wb-sb-core {
  grid-column: 3;
  align-self: center;
}

.is-wide .wb-sb-sinks {
  grid-column: 5;
  display: flex;
  flex-direction: column;
  justify-content: center;
  gap: 8px;
}

.wb-sb.is-tall {
  display: flex;
  flex-direction: column;
}

.is-tall .wb-sb-core {
  margin: 24px 22px 0;
}

.is-tall .wb-sb-sinks {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 8px;
  margin: 32px 22px 0;
}

.is-tall .wb-sb-tape {
  flex: none;
  height: calc(3 * 17px);
}

.wb-sb-canvas {
  position: absolute;
  top: 0;
  left: 0;
  pointer-events: none;
}

.wb-chip {
  padding: 9px 12px 10px;
}

.wb-sb-pg {
  display: flex;
  flex-direction: column;
  min-height: 0;
}

.wb-sb-core {
  border-color: var(--vp-c-brand-1);
}

.wb-sb-core.is-processing {
  border-color: var(--vp-c-brand-2);
}

.wb-sb-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 8px;
  margin-top: 1px;
  font-size: 11px;
  line-height: 17px;
  white-space: nowrap;
  color: var(--vp-c-text-3);
}

.wb-sb-row.wb-chip-title {
  margin-top: 0;
  font-size: 14px;
  line-height: 20px;
}

.wb-sb-amber,
.wb-sb-state {
  color: var(--vp-c-brand-1);
}

.wb-sb-state {
  font-size: 11px;
}

.wb-sb-stages .is-on {
  color: var(--vp-c-brand-1);
}

/* WAL tape: newest at the bottom, older lines scroll up into a fade */
.wb-sb-tape {
  flex: 1;
  min-height: 0;
  margin: 8px 0 0;
  padding: 0;
  list-style: none;
  display: flex;
  flex-direction: column;
  justify-content: flex-end;
  overflow: hidden;
  font-size: 11px;
  line-height: 17px;
  white-space: nowrap;
  color: var(--vp-c-text-1);
  mask-image: linear-gradient(to bottom, transparent, #000 45%);
}

.wb-sb-tape li {
  margin: 0;
  flex: none;
  transition: color 0.8s;
}

.wb-sb-tape .is-begin,
.wb-sb-tape .is-commit {
  color: var(--vp-c-text-3);
}

.wb-sb-tape .is-acked {
  color: var(--vp-c-text-2);
}

.wb-sb-dim {
  color: var(--vp-c-text-3);
}

.wb-sb-sq {
  display: inline-block;
  width: 5px;
  height: 5px;
  margin-right: 6px;
  vertical-align: 2px;
  background-color: var(--vp-c-brand-1);
}

.is-acked .wb-sb-sq {
  animation: wb-sb-ack 1.6s forwards;
}

@keyframes wb-sb-ack {
  0% {
    background-color: var(--wb-accent-blue);
  }
  100% {
    background-color: var(--vp-c-border);
  }
}

.wb-tape-move {
  transition: transform 0.2s steps(4);
}

.wb-tape-enter-active {
  transition: opacity 0.2s;
}

.wb-tape-enter-from {
  opacity: 0;
}

/* record panel: the followed change, raw row then document */
.wb-sb-rec {
  margin-top: 8px;
  padding: 4px 8px;
  border: 1px solid var(--vp-c-divider);
  background-color: var(--vp-c-bg);
  font-size: 11px;
  line-height: 17px;
  white-space: pre;
  overflow: hidden;
  color: var(--vp-c-text-1);
}

.wb-sb-rec div {
  transition: color 0.4s;
}

.wb-sb-rec .is-head {
  color: var(--vp-c-text-2);
}

.wb-sb-rec .is-blue {
  color: var(--wb-accent-blue);
}

.wb-sb-rec.is-stale div {
  color: var(--vp-c-text-3);
}

.wb-sb-lane {
  height: 15px;
  margin: 10px 0 6px;
}

.wb-sb-sink-head .wb-chip-title {
  font-size: 14px;
}

.wb-sb-meter {
  display: flex;
  align-items: flex-end;
  gap: 2px;
  height: 9px;
  margin-top: 4px;
}

.wb-sb-meter i {
  width: 5px;
  background-color: var(--vp-c-border);
  transition: background-color 0.4s;
}

.wb-sb-meter i.is-new {
  background-color: var(--wb-accent-blue);
  transition: none;
}
</style>
