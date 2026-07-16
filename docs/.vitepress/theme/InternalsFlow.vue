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

function runFx(fx: string) {
  if (fx === 'tick') {
    tickLsn();
    inFlightLsn = lsn.value;
  } else if (fx === 'deliver') {
    for (const id of SINK_IDS) counts.value[id] += 6 + Math.floor(Math.random() * 34);
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
  if (!playing.value) return;
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

onMounted(() => {
  reduced.value = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  if (reduced.value) playing.value = false;
  document.addEventListener('fullscreenchange', onFullscreenChange);
  document.addEventListener('keydown', onKeydown);
  resizeObserver = new ResizeObserver(recomputeScale);
  if (frame.value) resizeObserver.observe(frame.value);
  recomputeScale();
  if (playing.value) schedule(900);
});

onUnmounted(() => {
  if (timer) clearTimeout(timer);
  document.removeEventListener('fullscreenchange', onFullscreenChange);
  document.removeEventListener('keydown', onKeydown);
  resizeObserver?.disconnect();
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

          <div
            v-for="id in activeEdges"
            :key="scenarioId + ':' + stepIndex + ':' + id"
            class="wb-int-packet"
            :class="{ 'is-blue': stepBlue }"
            :style="{ offsetPath: `path('${d(edges.find(e => e.id === id)!)}')` }"
          ></div>
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

/* --- controls --------------------------------------------------------- */

.wb-int-controls {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 12px;
}

.wb-int-tabs {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
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

.wb-int-tab:hover,
.wb-int-btn:hover {
  border-color: var(--vp-c-brand-1);
  color: var(--vp-c-text-1);
}

.wb-int-tab.is-on {
  border-color: var(--vp-c-brand-1);
  color: var(--vp-c-brand-1);
  box-shadow: var(--wb-glow-amber);
}

.wb-int-buttons {
  display: flex;
  gap: 6px;
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

/* --- packets ---------------------------------------------------------- */

.wb-int-packet {
  position: absolute;
  top: 0;
  left: 0;
  width: 5px;
  height: 5px;
  background: var(--vp-c-brand-1);
  offset-rotate: 0deg;
  opacity: 0;
}

.wb-int-packet.is-blue {
  background: var(--wb-accent-blue);
}

@media (prefers-reduced-motion: no-preference) {
  .wb-int-packet {
    animation: wb-int-travel 1.1s linear;
  }
}

@keyframes wb-int-travel {
  0% {
    offset-distance: 0%;
    opacity: 1;
  }
  100% {
    offset-distance: 100%;
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
