// Placement for the How It Works step callout: the caption card takes the
// width of one of the diagram's stage columns and sits at the nearest
// height in any of them that doesn't cover the step's own stages or pulses.
// Covering the next step's stages costs most, then stages later in the
// walkthrough, then ones already passed or dimmed; a card that cuts
// through a stage the scenario uses costs extra. A short, straight leader
// joins the card to the closest active stage without crossing any other.

import { CANVAS, edges, groups, nodes } from './internals';

const LINE_PX = 17;
const PAD_X = 10;
const PAD_Y = 7;
// 12px mono
const CHAR_PX = 7.2;
const GRID = 8;
const MARGIN = 4;
const CUT_COST = 20000;
const DIAGONAL_COST = 15000;
const CROSSING_COST = 100000;

// the three stage columns: postgres, the leader, and its jobs + schema
const LANES = ['tables', 'stream', 'backfill'].map(id => {
  const n = nodes.find(n => n.id === id)!;
  return { x: n.x, w: n.w };
});

type Rect = { x: number; y: number; w: number; h: number };

export type Callout = Rect & {
  /** leader from the card to the stage it describes */
  leader: [number, number, number, number];
};

export type CalloutStep = {
  text: string;
  /** stages the caption talks about; the leader points at one of these */
  anchors: string[];
  /** stages and wires the card must not cover */
  avoidNodes: string[];
  avoidEdges: string[];
  /** stages the next step lights */
  next: string[];
  /** stages earlier steps lit */
  passed: Set<string>;
  /** stages the scenario uses at some step */
  involved: Set<string>;
};

function wrappedLines(text: string, width: number) {
  const perLine = Math.floor((width - 2 * PAD_X - 2) / CHAR_PX);
  let count = 1;
  let used = 0;
  for (const word of text.split(' ')) {
    if (used === 0) used = word.length;
    else if (used + 1 + word.length <= perLine) used += 1 + word.length;
    else {
      count++;
      used = word.length;
    }
  }
  return count;
}

function overlap(a: Rect, b: Rect) {
  const w = Math.min(a.x + a.w, b.x + b.w) - Math.max(a.x, b.x);
  const h = Math.min(a.y + a.h, b.y + b.h) - Math.max(a.y, b.y);
  return w > 0 && h > 0 ? w * h : 0;
}

function inflate(r: Rect, by: number): Rect {
  return { x: r.x - by, y: r.y - by, w: r.w + 2 * by, h: r.h + 2 * by };
}

function gap(a: Rect, b: Rect) {
  const dx = Math.max(0, a.x - (b.x + b.w), b.x - (a.x + a.w));
  const dy = Math.max(0, a.y - (b.y + b.h), b.y - (a.y + a.h));
  return Math.hypot(dx, dy);
}

// on each axis: the middle of the shared span, or the facing edges
function leader(card: Rect, node: Rect): Callout['leader'] {
  const axis = (a0: number, a1: number, b0: number, b1: number) => {
    const lo = Math.max(a0, b0);
    const hi = Math.min(a1, b1);
    if (lo <= hi) return [(lo + hi) / 2, (lo + hi) / 2];
    return a1 < b0 ? [a1, b0] : [a0, b1];
  };
  const [x1, x2] = axis(card.x, card.x + card.w, node.x, node.x + node.w);
  const [y1, y2] = axis(card.y, card.y + card.h, node.y, node.y + node.h);
  return [x1, y1, x2, y2];
}

const nodeRects = new Map<string, Rect>(nodes.map(n => [n.id, n]));

const edgeRects = edges.map(e => ({
  id: e.id,
  segments: e.points.slice(1).map(([x, y], i) => {
    const [px, py] = e.points[i];
    return inflate({ x: Math.min(x, px), y: Math.min(y, py), w: Math.abs(x - px), h: Math.abs(y - py) }, 3);
  }),
}));

const labelRects: Rect[] = [
  ...edges.filter(e => e.label).map(e => {
    const len = e.label!.length * 6.6 + 6;
    return e.vertical ? { x: e.lx!, y: e.ly!, w: 14, h: len } : { x: e.lx!, y: e.ly!, w: len, h: 14 };
  }),
  ...groups.map(g => ({ x: g.x + 12, y: g.y - 9, w: g.label.length * 7.2 + 12, h: 18 })),
];

function nodeWeight(id: string, step: CalloutStep) {
  if (step.avoidNodes.includes(id)) return 1000;
  if (step.next.includes(id)) return 50;
  if (step.passed.has(id)) return 2;
  return step.involved.has(id) ? 4 : 1;
}

export function placeCallout(step: CalloutStep): Callout {
  const anchors = step.anchors.map(id => nodeRects.get(id)!);
  const stages = nodes.map(n => ({
    box: nodeRects.get(n.id)!,
    r: inflate(nodeRects.get(n.id)!, 6),
    weight: nodeWeight(n.id, step),
    used: step.involved.has(n.id),
  }));
  // wires and labels: the step's own pulses are off limits
  const obstacles: [Rect, number][] = [
    ...edgeRects.flatMap(e => e.segments.map(s => [s, step.avoidEdges.includes(e.id) ? 1000 : 0.5] as [Rect, number])),
    ...labelRects.map(r => [r, 3] as [Rect, number]),
  ];

  let best: Rect = { ...LANES[0], y: MARGIN, h: 0 };
  let bestScore = Infinity;
  for (const { x, w } of LANES) {
    const h = wrappedLines(step.text, w) * LINE_PX + 2 * PAD_Y + 2;
    for (let y = MARGIN; y + h <= CANVAS.h - MARGIN; y += GRID) {
      const r = { x, y, w, h };
      const near = Math.min(...anchors.map(a => gap(r, a)));
      let score = near * near * 1.5;
      for (const { box, r: o, weight, used } of stages) {
        const covered = overlap(r, o);
        score += covered * weight;
        if (used && covered > 0 && (r.y > box.y || r.y + h < box.y + box.h)) score += CUT_COST;
      }
      for (const [o, weight] of obstacles) {
        score += overlap(r, o) * weight;
        if (score >= bestScore) break;
      }
      if (score < bestScore) score += leaderCost(r, anchors);
      if (score < bestScore) {
        bestScore = score;
        best = r;
      }
    }
  }

  return { ...best, leader: leader(best, closestOf(best, anchors)) };
}

function closestOf(card: Rect, anchors: Rect[]) {
  return anchors.reduce((a, b) => (gap(card, a) <= gap(card, b) ? a : b));
}

function leaderCost(card: Rect, anchors: Rect[]) {
  const target = closestOf(card, anchors);
  const [x1, y1, x2, y2] = leader(card, target);
  const line = { x: Math.min(x1, x2), y: Math.min(y1, y2), w: Math.abs(x2 - x1) || 1, h: Math.abs(y2 - y1) || 1 };
  for (const r of nodeRects.values()) {
    if (r !== target && overlap(line, r) > 0) return CROSSING_COST;
  }
  return x1 !== x2 && y1 !== y2 ? DIAGONAL_COST : 0;
}
