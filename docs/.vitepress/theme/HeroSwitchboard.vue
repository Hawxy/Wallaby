<script setup lang="ts">
import { nextTick, onMounted, onUnmounted, reactive, ref } from 'vue';
import { formatCount } from './flow/format';
import FlowChip from './flow/FlowChip.vue';
import { createTimeline } from './flow/timeline';
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
// DOM holds the text; one canvas, redrawn every frame, holds the rails
// and packets. Telemetry is fake, so the whole widget is hidden from
// assistive tech.

// cycle timeline, ms
const WRITE_GAP = 110; // between tape lines
const IN_MS = 700; // postgres to lane slot
const TRAIN_GAP = 90; // between packets in a train
const HOLD_MS = 900; // parked while the transform shows
const OUT_MS = 900; // lane slot to sink
const OUT_GAP = 60;
const SETTLE_MS = 150; // pause after the commit, and after the last landing
const ACK_MS = 550;
const REST_MS = 700;

const SLOTS = 8;
const TRAIL = 6; // comet tail samples, one per 18ms of travel
const SLOW_MO = 40; // displayed rate and lag are scaled back to real time
const TAPE_MAX = 16;
const GLYPHS = '0123456789ABCDEF#*+=<>/';

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
  leaveAt: number; // sim time it leaves postgres
  flushAt: number; // sim time it leaves its slot, Infinity until the flush
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
let now = 0;
const timeline = createTimeline();
let lsnNum = BASE_LSN;
let xid = 88209;
let packets: Packet[] = [];
let ackAt = -1; // sim time the ack leaves the lane, -1 when none
let ripples: { x: number; y: number; t0: number }[] = [];
// flash tokens: sinks 0-3, postgres 4, batch label 5
const flashTok = [0, 0, 0, 0, 0, 0];

function after(ms: number, fn: () => void) {
  timeline.schedule(now + ms, fn);
}

function flash(i: number, set: (on: boolean) => void, ms = 320) {
  const tok = ++flashTok[i];
  set(true);
  after(ms, () => {
    if (flashTok[i] === tok) set(false);
  });
}

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

function land(sink: number, perSink: number[], left: number[]) {
  const s = sinks[sink];
  s.count += 1;
  flash(sink, on => (s.flash = on));
  const end = geo!.tails[sink][geo!.tails[sink].length - 1];
  ripples.push({ x: end[0], y: end[1], t0: now });
  if (--left[sink] === 0) {
    s.meter = [...s.meter.slice(1), perSink[sink]];
    s.fresh = true;
    after(600, () => (s.fresh = false));
  }
}

// One wave per cycle, as back-to-back phases, so the stages always read
// in the same order and rhythm and only one thing moves at a time.
function startCycle() {
  const changes = Array.from({ length: 2 + Math.floor(rnd() * 3) }, newChange);
  const n = changes.length;
  const head = changes[0];
  const title = `${OP_GLYPH[head.op]} ${head.op.toUpperCase()} ${head.table.name} ${head.key}`;
  const perSink = [0, 0, 0, 0];
  for (const c of changes) for (const s of c.table.sinks) perSink[s] += 1;
  const left = [...perSink];
  let committedAt = 0;

  const total = timeline.play(now, [
    // write: begin, one tape line per change
    {
      ms: WRITE_GAP * (n + 1),
      run: () => addTape({ kind: 'begin', text: `begin ${++xid}`, lsn: lsnNum }),
      at: changes.map((c, j): [number, () => void] => [WRITE_GAP * (j + 1), () => writeChange(c)]),
    },
    // commit: changes only leave postgres once the transaction commits
    {
      ms: SETTLE_MS,
      run: () => {
        lsnNum += 0x30;
        walLsn.value = lsnNum;
        committedAt = now;
        addTape({ kind: 'commit', text: 'commit', lsn: lsnNum });
      },
    },
    // decode: the train crosses into wallaby and parks in the lane
    {
      ms: (n - 1) * TRAIN_GAP + IN_MS,
      run: () => {
        packets = changes.map((c, j) => ({ c, slot: j, leaveAt: now + j * TRAIN_GAP, flushAt: Infinity }));
      },
      at: [
        [IN_MS * 0.4, () => {
          stage.value = 1;
          rec.stale = false;
          rec.blue = [false, false, false, false];
          scrambleTo([title, ...[0, 1, 2].map(i => fieldLine(head.raw[i]))]);
        }],
        ...changes.map((_, j): [number, () => void] => [j * TRAIN_GAP + IN_MS, () => (batchCount.value += 1)]),
      ],
    },
    // transform: the batch holds while the followed record changes shape
    {
      ms: HOLD_MS,
      run: () => {
        stage.value = 2;
        rec.blue = [false, ...[0, 1, 2].map(i => !!head.doc[i]?.enrich)];
        scrambleTo([title, ...[0, 1, 2].map(i => fieldLine(head.doc[i]))]);
      },
    },
    // deliver: the batch fans out, each copy landing on its sink
    {
      ms: (n - 1) * OUT_GAP + OUT_MS + SETTLE_MS,
      run: () => {
        stage.value = 3;
        batchCount.value = 0;
        flash(5, on => (flushing.value = on), 260);
        packets.forEach((p, j) => (p.flushAt = now + j * OUT_GAP));
      },
      at: changes.flatMap((c, j) => c.table.sinks.map((s): [number, () => void] =>
        [j * OUT_GAP + OUT_MS, () => land(s, perSink, left)])),
    },
    // ack: back to postgres
    {
      ms: ACK_MS,
      run: () => {
        ackAt = now;
        lag.value = Math.round((now - committedAt) / SLOW_MO);
      },
    },
    // rest: the slot confirms, the tape markers turn blue
    {
      ms: REST_MS,
      run: () => {
        ackAt = -1;
        packets = [];
        stage.value = 0;
        rec.stale = true;
        flushLsn.value = lsnNum;
        flash(4, on => (pgFlash.value = on));
        rate.value = Math.round(0.7 * rate.value + 0.3 * (n / (total / 1000)) * SLOW_MO);
      },
    },
    { ms: 0, run: startCycle },
  ]);
}

// ---- record panel: character scramble between raw row and document ----

const scr: ({ from: string; to: string; t0: number; res: number[] } | null)[] = [null, null, null, null];

function scrambleTo(lines: string[]) {
  lines.forEach((to, i) => {
    const from = rec.lines[i];
    if (from === to) return;
    const n = Math.max(from.length, to.length);
    scr[i] = { from, to, t0: now, res: Array.from({ length: n }, (_, c) => (c / n) * 0.5 + rnd() * 0.5) };
  });
}

function tickScramble() {
  scr.forEach((s, i) => {
    if (!s) return;
    const p = (now - s.t0) / 420;
    if (p >= 1) {
      rec.lines[i] = s.to;
      scr[i] = null;
      return;
    }
    let out = '';
    for (let c = 0; c < s.res.length; c++) {
      const a = s.to[c] ?? ' ';
      const b = s.from[c] ?? ' ';
      out += p >= s.res[c] ? a : a === ' ' && b === ' ' ? ' ' : GLYPHS[Math.floor(Math.random() * GLYPHS.length)];
    }
    rec.lines[i] = out.trimEnd();
  });
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
  if (!running) drawStatic();
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

function ease(t: number) {
  const u = Math.min(1, Math.max(0, t));
  return u < 0.5 ? 4 * u * u * u : 1 - (-2 * u + 2) ** 3 / 2;
}

// where a tween along a path is at sim time t
function along(path: Pt[], start: number, dur: number, t: number): Pt {
  return pointAt(path, ease((t - start) / dur) * pathLength(path));
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

function frame(c: CanvasRenderingContext2D, x: number, y: number, s: number) {
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

// a moving packet: head plus a tail sampled from where it just was, so
// the tail shortens as the tween eases in and out
function comet(c: CanvasRenderingContext2D, path: Pt[], start: number, dur: number, color: string, size = 5) {
  for (let j = TRAIL; j >= 1; j--) {
    const t = now - j * 18;
    if (t < start) continue;
    c.globalAlpha = 0.5 * (1 - j / (TRAIL + 1));
    square(c, along(path, start, dur, t), color, j > 2 ? 3 : Math.min(size, 5));
  }
  c.globalAlpha = 1;
  c.shadowBlur = col.dark ? 6 * dpr : 0;
  c.shadowColor = color;
  const head = along(path, start, dur, now);
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
  for (let i = 0; i < SLOTS; i++) frame(c, slotX(i) - 4, g.laneY - 4, 9);
}

function draw() {
  const g = geo!;
  const c = ctx!;
  drawRails(c);

  ripples = ripples.filter(r => now - r.t0 < 450);
  for (const rp of ripples) {
    const t = (now - rp.t0) / 450;
    const s = Math.round(5 + t * 16) | 1;
    c.globalAlpha = 1 - t;
    c.fillStyle = col.blue;
    frame(c, Math.round(rp.x) - (s - 1) / 2, Math.round(rp.y) - (s - 1) / 2, s);
  }
  c.globalAlpha = 1;

  packets.forEach((p, j) => {
    let at: Pt | null = null;
    if (now < p.leaveAt) return;
    if (now < p.leaveAt + IN_MS) {
      at = comet(c, inPath(p.slot), p.leaveAt, IN_MS, col.amber);
    } else if (now < p.flushAt) {
      at = [slotX(p.slot), g.laneY];
      c.shadowBlur = col.dark ? 6 * dpr : 0;
      c.shadowColor = col.amber;
      square(c, at, col.amber);
      c.shadowBlur = 0;
    } else if (now < p.flushAt + OUT_MS) {
      const heads = p.c.table.sinks.map(s => comet(c, outPath(p.slot, s), p.flushAt, OUT_MS, col.amber));
      at = heads[0];
    }
    if (j === 0 && at) reticle(c, at);
  });

  if (ackAt >= 0) comet(c, g.ackRoute, ackAt, ACK_MS, col.blue, 3);
}

// reduced motion, or before the loop starts: rails plus a parked batch
function drawStatic() {
  if (!geo || !ctx) return;
  drawRails(ctx);
  for (let i = 0; i < 3; i++) square(ctx, [slotX(i), geo.laneY], col.amber);
}

// ---- loop lifecycle ----

let raf = 0;
let lastTs = 0;
let running = false;
let started = false;
let inView = false;
let reduced = false;

function tick(ts: number) {
  raf = requestAnimationFrame(tick);
  const dtMs = Math.min(50, ts - lastTs);
  lastTs = ts;
  if (!geo || !ctx || dtMs <= 0) return;
  now += dtMs;
  timeline.runDue(now);
  tickScramble();
  draw();
}

function sync() {
  const run = inView && !document.hidden && !reduced;
  if (run && !running) {
    running = true;
    if (!started) {
      started = true;
      after(400, startCycle);
    }
    lastTs = performance.now();
    raf = requestAnimationFrame(tick);
  } else if (!run && running) {
    running = false;
    cancelAnimationFrame(raf);
  }
}

let resizeObserver: ResizeObserver | undefined;
let intersectionObserver: IntersectionObserver | undefined;
let themeObserver: MutationObserver | undefined;

onMounted(() => {
  reduced = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  rnd = mulberry32(0x5eed);
  readColors();
  layout();
  document.fonts?.ready.then(layout);
  resizeObserver = new ResizeObserver(() => layout());
  resizeObserver.observe(root.value!);
  intersectionObserver = new IntersectionObserver(([entry]) => {
    inView = entry.isIntersecting;
    sync();
  });
  intersectionObserver.observe(root.value!);
  themeObserver = new MutationObserver(() => {
    readColors();
    if (!running) drawStatic();
  });
  themeObserver.observe(document.documentElement, { attributes: true, attributeFilter: ['class'] });
  document.addEventListener('visibilitychange', sync);
});

onUnmounted(() => {
  cancelAnimationFrame(raf);
  running = false;
  resizeObserver?.disconnect();
  intersectionObserver?.disconnect();
  themeObserver?.disconnect();
  document.removeEventListener('visibilitychange', sync);
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
  height: 312px;
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
