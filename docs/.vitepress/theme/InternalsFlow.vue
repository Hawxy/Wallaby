<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue';
import { lsn, tickLsn } from './lsn';
import { formatCount } from './flow/format';
import FlowChip from './flow/FlowChip.vue';
import {
  CANVAS, nodes, edges, groups, scenarios,
  type IntEdge, type IntNode,
} from './flow/internals';

// The "How It Works" internals diagram: every data flow in the engine on
// one canvas. Four scenario walkthroughs animate over it step by step,
// each stage is clickable for a detail panel, and the whole thing can go
// full screen. Amber packets are live WAL changes, blue ones are
// snapshot reads - the same color language as the other diagrams.

const SINK_IDS = new Set(['meili', 'http', 'kafka']);
const STEP_MS = 2600;
const LOOP_PAUSE_MS = 2400;

// deterministic starts - SSR hydration
const flushed = ref(lsn.value);
const applied = ref(lsn.value);
const counts = ref<Record<string, number>>({ meili: 23481, http: 9210, kafka: 41203 });
let inFlightLsn = lsn.value;

const scenarioId = ref('live');
const stepIndex = ref(-1); // -1 = idle, before the first step
const playing = ref(true);
// the walkthrough only runs while the diagram is on screen
const inView = ref(false);
const reduced = ref(false);
const selected = ref<IntNode | null>(null);
const isFullscreen = ref(false);
const isOverlay = ref(false); // fallback when the Fullscreen API is unavailable
const scale = ref(1);
const root = ref<HTMLElement>();
const frame = ref<HTMLElement>();

const scenario = computed(() => scenarios.find(s => s.id === scenarioId.value)!);
const steps = computed(() => scenario.value.steps);
const step = computed(() => (stepIndex.value >= 0 ? steps.value[stepIndex.value] : undefined));
const stepBlue = computed(() => step.value?.blue ?? false);
const caption = computed(() => step.value?.caption ?? scenario.value.blurb);

const activeNodes = computed(() => new Set(step.value?.nodes ?? []));
const activeEdges = computed(() => new Set(step.value?.edges ?? []));
const warnNodes = computed(() => new Set(step.value?.warn ?? []));

// everything the walkthrough has already touched keeps a faint tint,
// colored by whether it was visited by live (amber) or snapshot (blue)
// traffic. When idle under reduced motion the whole scenario shows lit.
const visited = computed(() => {
  const upTo = stepIndex.value >= 0
    ? stepIndex.value + 1
    : reduced.value ? steps.value.length : 0;
  const nodeTint = new Map<string, boolean>();
  const edgeTint = new Map<string, boolean>();
  for (const s of steps.value.slice(0, upTo)) {
    for (const n of s.nodes ?? []) nodeTint.set(n, s.blue ?? false);
    for (const e of s.edges ?? []) edgeTint.set(e, s.blue ?? false);
  }
  return { nodeTint, edgeTint };
});

function d(edge: IntEdge) {
  return edge.points
    .map(([x, y], i) => `${i === 0 ? 'M' : 'L'} ${x} ${y}`)
    .join(' ');
}

// pulses ride pathLength-normalized edges, so a fixed dash fraction would
// shrink with the edge - size the dash in canvas pixels instead (clamped
// so short hops still flash and long rails don't smear)
function pulseStyle(edgeId: string) {
  const edge = edges.find(e => e.id === edgeId)!;
  let length = 0;
  for (let i = 1; i < edge.points.length; i++) {
    length += Math.abs(edge.points[i][0] - edge.points[i - 1][0])
      + Math.abs(edge.points[i][1] - edge.points[i - 1][1]);
  }
  const dashPx = Math.min(48, Math.max(16, length * 0.25));
  const dash = Math.min(0.75, dashPx / length);
  return { strokeDasharray: `${dash} 2`, '--wb-pulse-dash': `${dash}` };
}

function runFx(fx: string) {
  if (fx === 'tick') {
    tickLsn();
    inFlightLsn = lsn.value;
  } else if (fx === 'deliver') {
    for (const id of SINK_IDS) counts.value[id] += 6 + Math.floor(Math.random() * 34);
  } else if (fx === 'deliver-partial') {
    for (const id of SINK_IDS) {
      if (id !== 'http') counts.value[id] += 6 + Math.floor(Math.random() * 34);
    }
  } else if (fx === 'flush') {
    flushed.value = inFlightLsn;
    applied.value = inFlightLsn;
  }
}

let timer: ReturnType<typeof setTimeout> | undefined;

function applyStep(i: number, withFx: boolean) {
  stepIndex.value = i;
  const s = steps.value[i];
  if (withFx && s?.fx) runFx(s.fx);
}

function schedule(delay: number) {
  if (timer) clearTimeout(timer);
  if (!playing.value || !inView.value) return;
  timer = setTimeout(() => {
    const atEnd = stepIndex.value >= steps.value.length - 1;
    applyStep(atEnd ? 0 : stepIndex.value + 1, true);
    schedule(stepIndex.value >= steps.value.length - 1 ? STEP_MS + LOOP_PAUSE_MS : STEP_MS);
  }, delay);
}

function selectScenario(id: string) {
  if (scenarioId.value === id) return;
  scenarioId.value = id;
  stepIndex.value = -1;
  if (playing.value) schedule(900);
}

function togglePlay() {
  playing.value = !playing.value;
  if (playing.value) schedule(400);
  else if (timer) clearTimeout(timer);
}

function stepBy(delta: number) {
  playing.value = false;
  if (timer) clearTimeout(timer);
  const next = Math.min(steps.value.length - 1, Math.max(0, stepIndex.value + delta));
  applyStep(next, false);
}

function selectNode(n: IntNode) {
  selected.value = selected.value?.id === n.id ? null : n;
}

// --- fullscreen -------------------------------------------------------

function toggleFullscreen() {
  if (isOverlay.value) {
    isOverlay.value = false;
    return;
  }
  if (document.fullscreenElement) {
    document.exitFullscreen();
    return;
  }
  const el = root.value;
  if (el?.requestFullscreen) {
    el.requestFullscreen().catch(() => (isOverlay.value = true));
  } else {
    isOverlay.value = true;
  }
}

function onFullscreenChange() {
  isFullscreen.value = !!document.fullscreenElement;
  requestAnimationFrame(recomputeScale);
}

function onKeydown(e: KeyboardEvent) {
  if (e.key === 'Escape' && isOverlay.value) isOverlay.value = false;
}

const expanded = computed(() => isFullscreen.value || isOverlay.value);

// --- scaling: the canvas is fixed-size and scales to its container ----

function recomputeScale() {
  const w = frame.value?.clientWidth ?? CANVAS.w;
  if (expanded.value) {
    const h = window.innerHeight - 220; // controls + caption + padding
    scale.value = Math.max(0.5, Math.min(1.5, w / CANVAS.w, h / CANVAS.h));
  } else {
    // below this the labels stop being readable - hold and let it scroll
    scale.value = Math.max(0.62, Math.min(1, w / CANVAS.w));
  }
}

const stageWrapStyle = computed(() => ({
  width: `${CANVAS.w * scale.value}px`,
  height: `${CANVAS.h * scale.value}px`,
}));
const stageStyle = computed(() => ({ transform: `scale(${scale.value})` }));

let resizeObserver: ResizeObserver | undefined;
let intersectionObserver: IntersectionObserver | undefined;

onMounted(() => {
  reduced.value = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  if (reduced.value) playing.value = false;
  document.addEventListener('fullscreenchange', onFullscreenChange);
  document.addEventListener('keydown', onKeydown);
  resizeObserver = new ResizeObserver(recomputeScale);
  if (frame.value) resizeObserver.observe(frame.value);
  recomputeScale();
  // the observer fires immediately with the initial visibility, which is
  // also what kicks off the first cycle
  intersectionObserver = new IntersectionObserver(([entry]) => {
    inView.value = entry.isIntersecting;
    if (!entry.isIntersecting) {
      if (timer) clearTimeout(timer);
    } else if (playing.value) {
      schedule(700);
    }
  }, { threshold: 0.15 });
  if (root.value) intersectionObserver.observe(root.value);
});

onUnmounted(() => {
  if (timer) clearTimeout(timer);
  document.removeEventListener('fullscreenchange', onFullscreenChange);
  document.removeEventListener('keydown', onKeydown);
  resizeObserver?.disconnect();
  intersectionObserver?.disconnect();
});
</script>

<template>
  <div
    ref="root"
    class="wb-int"
    :class="{ 'is-overlay': isOverlay, 'is-expanded': expanded }"
    role="group"
    aria-label="Interactive diagram of Wallaby's internals: postgres WAL, publication and replication slot feed the leader's decode, materialize, transform and dispatch stages; backfill and dependent fan-out feed the same pipeline; sinks acknowledge back to the slot and checkpoint."
  >
    <div class="wb-int-controls">
      <div class="wb-int-tabs" role="tablist" aria-label="data flow">
        <button
          v-for="s in scenarios"
          :key="s.id"
          class="wb-int-tab"
          :class="{ 'is-on': s.id === scenarioId }"
          role="tab"
          :aria-selected="s.id === scenarioId"
          @click="selectScenario(s.id)"
        >{{ s.label }}</button>
      </div>
      <div class="wb-int-buttons">
        <button class="wb-int-btn" aria-label="previous step" @click="stepBy(-1)">‹</button>
        <button class="wb-int-btn is-play" :aria-label="playing ? 'pause' : 'play'" @click="togglePlay">
          {{ playing ? 'pause' : 'play' }}
        </button>
        <button class="wb-int-btn" aria-label="next step" @click="stepBy(1)">›</button>
        <button class="wb-int-btn is-fs" @click="toggleFullscreen">
          {{ expanded ? 'exit' : 'fullscreen' }}
        </button>
      </div>
    </div>

    <div ref="frame" class="wb-int-frame">
      <div class="wb-int-stage-wrap" :style="stageWrapStyle">
        <div class="wb-int-stage" :style="stageStyle">
          <div
            v-for="g in groups"
            :key="g.id"
            class="wb-int-group"
            :style="{ left: g.x + 'px', top: g.y + 'px', width: g.w + 'px', height: g.h + 'px' }"
          >
            <span class="wb-int-group-label">{{ g.label }}</span>
          </div>

          <svg class="wb-int-wires" :viewBox="`0 0 ${CANVAS.w} ${CANVAS.h}`" aria-hidden="true">
            <defs>
              <marker id="wb-int-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M 0 0 L 8 4 L 0 8 z" class="wb-int-arrow" />
              </marker>
              <marker id="wb-int-arrow-amber" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M 0 0 L 8 4 L 0 8 z" class="wb-int-arrow is-amber" />
              </marker>
              <marker id="wb-int-arrow-blue" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M 0 0 L 8 4 L 0 8 z" class="wb-int-arrow is-blue" />
              </marker>
              <marker id="wb-int-arrow-amber-soft" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M 0 0 L 8 4 L 0 8 z" class="wb-int-arrow is-amber-soft" />
              </marker>
              <marker id="wb-int-arrow-blue-soft" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M 0 0 L 8 4 L 0 8 z" class="wb-int-arrow is-blue-soft" />
              </marker>
            </defs>
            <path
              v-for="e in edges"
              :key="e.id"
              :d="d(e)"
              class="wb-int-wire"
              :class="{
                'is-dashed': e.dashed,
                'is-active': activeEdges.has(e.id),
                'is-blue': activeEdges.has(e.id) ? stepBlue : visited.edgeTint.get(e.id),
                'is-visited': !activeEdges.has(e.id) && visited.edgeTint.has(e.id),
              }"
            />
            <path
              v-for="id in activeEdges"
              :key="scenarioId + ':' + stepIndex + ':' + id"
              :d="d(edges.find(e => e.id === id)!)"
              pathLength="1"
              class="wb-int-pulse"
              :class="{ 'is-blue': stepBlue }"
              :style="pulseStyle(id)"
            />
          </svg>

          <span
            v-for="e in edges.filter(e => e.label)"
            :key="e.id + '-label'"
            class="wb-int-wire-label"
            :class="{ 'is-vertical': e.vertical }"
            :style="{ left: e.lx + 'px', top: e.ly + 'px' }"
          >{{ e.label }}</span>

          <button
            v-for="n in nodes"
            :key="n.id"
            class="wb-int-node"
            :class="{
              'is-selected': selected?.id === n.id,
              'is-visited': !activeNodes.has(n.id) && visited.nodeTint.has(n.id),
              'is-blue-visited': !activeNodes.has(n.id) && visited.nodeTint.get(n.id),
              'is-warn': warnNodes.has(n.id),
            }"
            :style="{ left: n.x + 'px', top: n.y + 'px', width: n.w + 'px' }"
            :aria-label="n.title + ' - show details'"
            :aria-pressed="selected?.id === n.id"
            @click="selectNode(n)"
          >
            <FlowChip
              :lit="activeNodes.has(n.id) && !stepBlue"
              :flash="activeNodes.has(n.id) && stepBlue"
            >
              <div class="wb-chip-title">{{ n.title }}</div>
              <template v-if="n.id === 'wal'">
                <div class="wb-chip-sub">wal @ <span class="wb-chip-val">{{ lsn }}</span></div>
              </template>
              <template v-else-if="n.id === 'slot'">
                <div class="wb-chip-sub">{{ n.subs[0] }}</div>
                <div class="wb-chip-sub">flushed @ <span class="wb-chip-val">{{ flushed }}</span></div>
              </template>
              <template v-else-if="n.id === 'checkpoint'">
                <div class="wb-chip-sub">applied @ <span class="wb-chip-val">{{ applied }}</span></div>
              </template>
              <template v-else-if="SINK_IDS.has(n.id)">
                <div class="wb-chip-sub">
                  <span class="wb-chip-val">{{ formatCount(counts[n.id]) }}</span> delivered
                </div>
              </template>
              <template v-else>
                <div v-for="s in n.subs" :key="s" class="wb-chip-sub">{{ s }}</div>
              </template>
            </FlowChip>
          </button>

        </div>
      </div>
    </div>

    <div class="wb-int-caption">
      <span class="wb-int-prompt">$</span>
      <span v-if="stepIndex >= 0" class="wb-int-count">[{{ stepIndex + 1 }}/{{ steps.length }}]</span>
      <span class="wb-int-caption-text">{{ caption }}</span>
      <span class="wb-int-legend" aria-hidden="true">
        <span class="wb-int-swatch is-amber"></span>live
        <span class="wb-int-swatch is-blue"></span>snapshot
      </span>
    </div>

    <div v-if="selected" class="wb-int-detail">
      <div class="wb-int-detail-head">
        <span class="wb-int-detail-title">{{ selected.title }}</span>
        <button class="wb-int-btn" aria-label="close details" @click="selected = null">✕</button>
      </div>
      <p class="wb-int-detail-body">{{ selected.detail }}</p>
      <div v-if="selected.links.length" class="wb-int-detail-links">
        <a v-for="l in selected.links" :key="l.href" :href="l.href">{{ l.text }} →</a>
      </div>
    </div>
    <div v-else class="wb-int-hint">click a stage for details</div>
  </div>
</template>

<style scoped>
.wb-int {
  margin: 32px 0;
  font-family: var(--vp-font-family-mono);
  --wb-chip-decay: 0.7s;
}

.wb-int.is-overlay {
  position: fixed;
  inset: 0;
  z-index: 200;
  margin: 0;
  padding: 24px;
  overflow: auto;
  background: var(--vp-c-bg);
}

.wb-int:fullscreen {
  padding: 24px 32px;
  overflow: auto;
  background: var(--vp-c-bg);
}

/* expanded on a wide screen: the detail panel moves beside the canvas
   instead of below it, so stage details never sit off-screen */
@media (min-width: 1100px) {
  .wb-int.is-expanded {
    display: grid;
    /* the canvas column mirrors the scale formula (1.5x cap, width fit,
       height fit), and the centered grid keeps the controls, caption,
       and side panel hugging the diagram instead of the viewport edges
       on ultrawide screens */
    grid-template-columns:
      min(1056px, calc(100vw - 410px), calc(0.8224 * (100vh - 220px)))
      320px;
    grid-template-rows: auto 1fr auto;
    grid-template-areas:
      'controls controls'
      'frame    side'
      'caption  side';
    column-gap: 24px;
    justify-content: center;
  }

  .wb-int.is-expanded .wb-int-controls {
    grid-area: controls;
  }

  .wb-int.is-expanded .wb-int-frame {
    grid-area: frame;
  }

  .wb-int.is-expanded .wb-int-caption {
    grid-area: caption;
  }

  .wb-int.is-expanded .wb-int-detail,
  .wb-int.is-expanded .wb-int-hint {
    grid-area: side;
    align-self: start;
    margin-top: 0;
    max-height: 100%;
    overflow-y: auto;
  }
}

/* --- controls --------------------------------------------------------- */

.wb-int-controls {
  display: flex;
  flex-wrap: wrap;
  gap: 8px 16px;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 12px;
}

/* the flow tabs read as one segmented bar: joined borders, square inner
   corners, one row that scrolls rather than wraps when space runs out */
.wb-int-tabs {
  display: flex;
  min-width: 0;
  max-width: 100%;
  overflow-x: auto;
  scrollbar-width: none;
}

.wb-int-tabs::-webkit-scrollbar {
  display: none;
}

.wb-int-tab,
.wb-int-btn {
  padding: 3px 10px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 2px;
  background: var(--vp-code-block-bg);
  color: var(--vp-c-text-2);
  font-family: inherit;
  font-size: 12px;
  line-height: 18px;
  cursor: pointer;
  transition: color 0.2s, border-color 0.2s, box-shadow 0.2s;
}

.wb-int-tab {
  flex-shrink: 0;
  position: relative;
  margin-left: -1px;
  border-radius: 0;
}

.wb-int-tab:first-child {
  margin-left: 0;
  border-radius: 2px 0 0 2px;
}

.wb-int-tab:last-child {
  border-radius: 0 2px 2px 0;
}

.wb-int-tab:hover,
.wb-int-btn:hover {
  border-color: var(--vp-c-brand-1);
  color: var(--vp-c-text-1);
  z-index: 1;
}

.wb-int-tab.is-on {
  border-color: var(--vp-c-brand-1);
  color: var(--vp-c-brand-1);
  box-shadow: var(--wb-glow-amber);
  z-index: 1;
}

/* transport cluster: ‹ play › joined, fullscreen set apart; fixed slot
   widths so the toggling labels don't shift the row */
.wb-int-buttons {
  display: flex;
  align-items: center;
  /* when the row wraps, the transport cluster right-aligns instead of
     dangling under the tabs */
  margin-left: auto;
}

.wb-int-buttons .wb-int-btn {
  position: relative;
  margin-left: -1px;
  border-radius: 0;
}

.wb-int-buttons .wb-int-btn:first-child {
  margin-left: 0;
  border-radius: 2px 0 0 2px;
}

.wb-int-buttons .wb-int-btn.is-play {
  min-width: 58px;
  text-align: center;
}

.wb-int-buttons .wb-int-btn.is-fs {
  margin-left: 12px;
  border-radius: 2px;
  min-width: 96px;
  text-align: center;
}

.wb-int-buttons .wb-int-btn:nth-last-child(2) {
  border-radius: 0 2px 2px 0;
}

/* --- stage ------------------------------------------------------------ */

.wb-int-frame {
  overflow-x: auto;
}

.wb-int-stage-wrap {
  margin: 0 auto;
}

.wb-int-stage {
  position: relative;
  width: 704px;
  height: 856px;
  transform-origin: top left;
}

.wb-int-group {
  position: absolute;
  border: 1px solid var(--vp-c-divider);
  border-radius: 2px;
}

.wb-int-group-label {
  position: absolute;
  top: -9px;
  left: 12px;
  padding: 0 6px;
  background: var(--vp-c-bg);
  font-size: 11px;
  line-height: 18px;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: var(--vp-c-text-3);
  white-space: nowrap;
}

.wb-int-wires {
  position: absolute;
  inset: 0;
  width: 100%;
  height: 100%;
  pointer-events: none;
}

.wb-int-wire {
  fill: none;
  stroke: var(--vp-c-divider);
  stroke-width: 1;
  transition: stroke 0.4s;
  marker-end: url(#wb-int-arrow);
}

.wb-int-wire.is-active {
  marker-end: url(#wb-int-arrow-amber);
}

.wb-int-wire.is-active.is-blue {
  marker-end: url(#wb-int-arrow-blue);
}

.wb-int-wire.is-visited {
  marker-end: url(#wb-int-arrow-amber-soft);
}

.wb-int-wire.is-visited.is-blue {
  marker-end: url(#wb-int-arrow-blue-soft);
}

.wb-int-arrow {
  fill: var(--vp-c-border);
}

.wb-int-arrow.is-amber {
  fill: var(--vp-c-brand-1);
}

.wb-int-arrow.is-blue {
  fill: var(--wb-accent-blue);
}

/* trail arrowheads keep the same soft tint as their visited wires */
.wb-int-arrow.is-amber-soft {
  fill: color-mix(in srgb, var(--vp-c-brand-1) 45%, var(--vp-c-divider));
}

.wb-int-arrow.is-blue-soft {
  fill: color-mix(in srgb, var(--wb-accent-blue) 45%, var(--vp-c-divider));
}

.wb-int-wire.is-dashed {
  stroke-dasharray: 4 4;
}

.wb-int-wire.is-visited {
  stroke: color-mix(in srgb, var(--vp-c-brand-1) 45%, var(--vp-c-divider));
}

.wb-int-wire.is-visited.is-blue {
  stroke: color-mix(in srgb, var(--wb-accent-blue) 45%, var(--vp-c-divider));
}

.wb-int-wire.is-active {
  stroke: var(--vp-c-brand-1);
  stroke-width: 1.5;
  transition: stroke 0.15s;
}

.wb-int-wire.is-active.is-blue {
  stroke: var(--wb-accent-blue);
}

.wb-int-wire-label {
  position: absolute;
  font-size: 11px;
  line-height: 14px;
  color: var(--vp-c-text-3);
  white-space: nowrap;
  pointer-events: none;
}

.wb-int-wire-label.is-vertical {
  writing-mode: vertical-rl;
  transform: rotate(180deg);
}

/* --- nodes ------------------------------------------------------------ */

.wb-int-node {
  position: absolute;
  padding: 0;
  border: none;
  background: none;
  text-align: left;
  font-family: inherit;
  cursor: pointer;
}

.wb-int-node .wb-chip {
  width: 100%;
}

.wb-int-node:hover .wb-chip {
  border-color: var(--vp-c-border);
}

.wb-int-node.is-visited .wb-chip {
  border-color: color-mix(in srgb, var(--vp-c-brand-1) 45%, var(--vp-c-divider));
}

.wb-int-node.is-visited.is-blue-visited .wb-chip {
  border-color: color-mix(in srgb, var(--wb-accent-blue) 45%, var(--vp-c-divider));
}

.wb-int-node.is-selected .wb-chip {
  border-color: var(--wb-accent-blue);
  box-shadow: var(--wb-glow-blue);
}

.wb-int-node.is-warn .wb-chip {
  border-style: dashed;
  border-color: var(--vp-c-brand-1);
}

/* --- pulses ------------------------------------------------------------
   Data movement is a bright dash riding the wire itself (pathLength
   normalizes every edge to 1, so one keyframe set fits all), ending in
   the arrowhead instead of colliding with it. */

.wb-int-pulse {
  fill: none;
  stroke: var(--vp-c-brand-2);
  stroke-width: 2.5;
  stroke-linecap: round;
  /* dash length is set inline per edge (--wb-pulse-dash); the gap of 2
     always exceeds path (1) + dash, so only one dash is visible */
  stroke-dashoffset: var(--wb-pulse-dash, 0.25);
  filter: drop-shadow(0 0 3px var(--vp-c-brand-2));
  opacity: 0;
}

.wb-int-pulse.is-blue {
  stroke: var(--wb-accent-blue);
  filter: drop-shadow(0 0 3px var(--wb-accent-blue));
}

@media (prefers-reduced-motion: no-preference) {
  .wb-int-pulse {
    animation: wb-int-pulse-travel 1.1s linear;
  }
}

@keyframes wb-int-pulse-travel {
  0% {
    stroke-dashoffset: var(--wb-pulse-dash, 0.25);
    opacity: 1;
  }
  100% {
    stroke-dashoffset: -1;
    opacity: 1;
  }
}

/* --- caption + detail panel ------------------------------------------- */

.wb-int-caption {
  display: flex;
  align-items: baseline;
  gap: 8px;
  margin-top: 12px;
  min-height: 38px;
  font-size: 12px;
  line-height: 18px;
  color: var(--vp-c-text-2);
}

.wb-int-prompt {
  color: var(--vp-c-brand-1);
}

.wb-int-count {
  color: var(--vp-c-text-3);
  flex-shrink: 0;
}

.wb-int-caption-text {
  flex: 1;
}

.wb-int-legend {
  display: flex;
  align-items: center;
  gap: 5px;
  flex-shrink: 0;
  font-size: 11px;
  color: var(--vp-c-text-3);
}

.wb-int-swatch {
  width: 5px;
  height: 5px;
}

.wb-int-swatch.is-amber {
  background: var(--vp-c-brand-1);
}

.wb-int-swatch.is-blue {
  background: var(--wb-accent-blue);
  margin-left: 6px;
}

.wb-int-detail {
  margin-top: 8px;
  padding: 12px 16px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 2px;
  background: var(--vp-code-block-bg);
}

.wb-int-detail-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 8px;
}

.wb-int-detail-title {
  font-size: 13px;
  color: var(--vp-c-brand-1);
}

.wb-int-detail-body {
  margin: 8px 0 0;
  font-family: var(--vp-font-family-base);
  font-size: 13px;
  line-height: 20px;
  color: var(--vp-c-text-2);
}

.wb-int-detail-links {
  display: flex;
  flex-wrap: wrap;
  gap: 6px 16px;
  margin-top: 8px;
}

.wb-int-detail-links a {
  font-size: 12px;
  color: var(--wb-accent-blue);
  text-decoration: underline;
}

.wb-int-hint {
  margin-top: 8px;
  font-size: 11px;
  color: var(--vp-c-text-3);
}
</style>
