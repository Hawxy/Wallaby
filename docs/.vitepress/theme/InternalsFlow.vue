<script setup lang="ts">
import { computed, nextTick, onMounted, onUnmounted, ref } from 'vue';
import { lsn, tickLsn } from './lsn';
import { formatCount } from './flow/format';
import FlowChip from './flow/FlowChip.vue';
import { phaseTimeline, type Tween } from './flow/timeline';
import { useTimelineLoop } from './flow/useTimelineLoop';
import { placeCallout, type Callout } from './flow/callout';
import {
  CANVAS, nodes, edges, groups, scenarios,
  type IntEdge, type IntNode,
} from './flow/internals';

// The "How It Works" internals diagram: every data flow in the engine on
// one canvas. Five scenario walkthroughs animate over it step by step,
// dimming the stages a scenario never touches, with each step's caption
// in a callout beside its stages; a step strip scrubs through them, each
// stage is clickable for a detail panel, and the whole
// thing can go full screen. Amber packets are live WAL changes, blue ones
// are snapshot reads - the same color language as the other diagrams.
// #flow-<scenario>-step-<n> links straight to a step (slug-shaped, so
// VitePress leaves markdown links to it alone).

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
const strip = ref<HTMLElement>();

const scenario = computed(() => scenarios.find(s => s.id === scenarioId.value)!);
const steps = computed(() => scenario.value.steps);
const step = computed(() => (stepIndex.value >= 0 ? steps.value[stepIndex.value] : undefined));
const stepBlue = computed(() => step.value?.blue ?? false);
const caption = computed(() => step.value?.caption ?? scenario.value.blurb);

const activeNodes = computed(() => new Set(step.value?.nodes ?? []));
const activeEdges = computed(() => new Set(step.value?.edges ?? []));
const warnNodes = computed(() => new Set(step.value?.warn ?? []));

// everything the scenario touches at any step; the rest of the map dims
const involved = computed(() => ({
  nodes: new Set(steps.value.flatMap(s => [...(s.nodes ?? []), ...(s.warn ?? [])])),
  edges: new Set(steps.value.flatMap(s => s.edges ?? [])),
}));

// the current step's caption sits beside its stages once the canvas is big
// enough to read it there; smaller, it stays on the line below
const calloutCache = new Map<string, Callout>();
const callout = computed(() => {
  const s = step.value;
  if (!s || scale.value < 0.8) return undefined;
  const key = `${scenarioId.value}:${stepIndex.value}`;
  let placed = calloutCache.get(key);
  if (!placed) {
    const anchors = s.nodes?.length ? s.nodes : (s.warn ?? []);
    const next = steps.value[stepIndex.value + 1];
    placed = placeCallout({
      next: [...(next?.nodes ?? []), ...(next?.warn ?? [])],
      passed: new Set(steps.value.slice(0, stepIndex.value).flatMap(p => p.nodes ?? [])),
      text: `[${stepIndex.value + 1}/${steps.value.length}] ${s.caption}`,
      anchors,
      avoidNodes: [...anchors, ...(s.warn ?? [])],
      avoidEdges: s.edges ?? [],
      involved: involved.value.nodes,
    });
    calloutCache.set(key, placed);
  }
  return placed;
});

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

function applyStep(i: number, withFx: boolean) {
  stepIndex.value = i;
  const s = steps.value[i];
  if (withFx && s?.fx) runFx(s.fx);
}

// --- playback ---------------------------------------------------------

// where the next walkthrough picks up, and how long it waits first
let resumeAt = 0;
let leadMs = 700;

// the current step's progress fills its segment of the step strip; set
// directly so the per-frame update doesn't re-render the diagram
function setProgress(v: number) {
  strip.value?.style.setProperty('--wb-step-progress', String(v));
}

// the rest of the scenario from `resumeAt`, one phase per step, holding
// on the last step before the walkthrough loops
function walkthrough() {
  const list = steps.value;
  const from = resumeAt;
  const lead = leadMs;
  resumeAt = 0;
  leadMs = 0;
  return phaseTimeline([
    { name: 'lead', ms: lead },
    ...list.slice(from).map((_, j) => {
      const k = from + j;
      const fill: Tween = [0, STEP_MS, setProgress];
      return {
        name: `step-${k + 1}`,
        ms: k === list.length - 1 ? STEP_MS + LOOP_PAUSE_MS : STEP_MS,
        run: () => {
          setProgress(0);
          applyStep(k, true);
        },
        tweens: [fill],
      };
    }),
  ]);
}

const loop = useTimelineLoop(root, walkthrough, { active: playing, threshold: 0.15 });

function restartAt(from: number, lead: number) {
  resumeAt = from;
  leadMs = lead;
  loop.reset();
}

function selectScenario(id: string) {
  if (scenarioId.value === id) return;
  scenarioId.value = id;
  stepIndex.value = -1;
  setProgress(0);
  restartAt(0, 900);
  writeHash();
}

function togglePlay() {
  playing.value = !playing.value;
}

// jumping to a step pauses on it; play continues from the next one
function goTo(i: number) {
  const target = Math.min(steps.value.length - 1, Math.max(0, i));
  playing.value = false;
  applyStep(target, false);
  setProgress(1);
  restartAt(target + 1, 400);
  writeHash();
  return target;
}

function onStripKey(e: KeyboardEvent) {
  const delta = e.key === 'ArrowRight' ? 1 : e.key === 'ArrowLeft' ? -1 : 0;
  if (!delta) return;
  e.preventDefault();
  const i = goTo(stepIndex.value + delta);
  nextTick(() => (strip.value?.children[i] as HTMLElement | undefined)?.focus());
}

function selectNode(n: IntNode) {
  selected.value = selected.value?.id === n.id ? null : n;
}

// --- deep links -------------------------------------------------------

// a link with a step holds on that step; one without plays the scenario
function readHash() {
  const m = /^#flow-([a-z]+)(?:-step-(\d+))?$/.exec(location.hash);
  const target = m && scenarios.find(s => s.id === m[1]);
  if (!m || !target) return;
  scenarioId.value = target.id;
  if (m[2]) {
    goTo(+m[2] - 1);
  } else {
    stepIndex.value = -1;
    setProgress(0);
    playing.value = !reduced.value;
    restartAt(0, 900);
  }
  requestAnimationFrame(() => root.value?.scrollIntoView({ block: 'start' }));
}

function writeHash() {
  const stepPart = stepIndex.value >= 0 ? `-step-${stepIndex.value + 1}` : '';
  history.replaceState(history.state, '', `#flow-${scenarioId.value}${stepPart}`);
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
  // offsetWidth includes any scrollbar, so the scale can't flip-flop as
  // a scrollbar it caused comes and goes
  const w = frame.value?.offsetWidth ?? CANVAS.w;
  if (expanded.value) {
    const h = window.innerHeight - 248; // controls + steps + caption + padding
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
  window.addEventListener('hashchange', readHash);
  resizeObserver = new ResizeObserver(recomputeScale);
  if (frame.value) resizeObserver.observe(frame.value);
  recomputeScale();
  readHash();
});

onUnmounted(() => {
  document.removeEventListener('fullscreenchange', onFullscreenChange);
  document.removeEventListener('keydown', onKeydown);
  window.removeEventListener('hashchange', readHash);
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
        <button class="wb-int-btn is-play" :aria-label="playing ? 'pause' : 'play'" @click="togglePlay">
          {{ playing ? 'pause' : 'play' }}
        </button>
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
                'is-dim': !involved.edges.has(e.id),
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
            :class="{ 'is-vertical': e.vertical, 'is-dim': !involved.edges.has(e.id) }"
            :style="{ left: e.lx + 'px', top: e.ly + 'px' }"
          >{{ e.label }}</span>

          <button
            v-for="n in nodes"
            :key="n.id"
            class="wb-int-node"
            :class="{
              'is-dim': !involved.nodes.has(n.id),
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

          <Transition name="wb-int-callout">
            <div
              v-if="callout"
              :key="scenarioId + ':' + stepIndex"
              class="wb-int-callout-layer"
              :class="{ 'is-blue': stepBlue }"
            >
              <svg class="wb-int-leader" :viewBox="`0 0 ${CANVAS.w} ${CANVAS.h}`" aria-hidden="true">
                <line :x1="callout.leader[0]" :y1="callout.leader[1]" :x2="callout.leader[2]" :y2="callout.leader[3]" />
                <circle :cx="callout.leader[2]" :cy="callout.leader[3]" r="2" />
              </svg>
              <div
                class="wb-int-callout"
                :style="{ left: callout.x + 'px', top: callout.y + 'px', width: callout.w + 'px' }"
              >
                <span class="wb-int-count">[{{ stepIndex + 1 }}/{{ steps.length }}]</span> {{ step?.caption }}
              </div>
            </div>
          </Transition>
        </div>
      </div>
    </div>

    <div
      ref="strip"
      class="wb-int-steps"
      role="group"
      aria-label="walkthrough steps"
      @keydown="onStripKey"
    >
      <button
        v-for="(s, i) in steps"
        :key="scenarioId + ':' + i"
        class="wb-int-step"
        :class="{
          'is-current': i === stepIndex,
          'is-done': i < stepIndex,
          'is-blue': s.blue,
        }"
        :title="s.caption"
        :aria-label="`step ${i + 1}: ${s.caption}`"
        :aria-current="i === stepIndex ? 'step' : undefined"
        @click="goTo(i)"
      ><i /></button>
    </div>

    <div class="wb-int-caption">
      <span class="wb-int-prompt">$</span>
      <span v-if="stepIndex >= 0 && !callout" class="wb-int-count">[{{ stepIndex + 1 }}/{{ steps.length }}]</span>
      <span class="wb-int-caption-text">{{ callout ? scenario.blurb : caption }}</span>
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
  scroll-margin-top: calc(var(--vp-nav-height) + 24px);
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
      min(1056px, calc(100vw - 410px), calc(0.8224 * (100vh - 248px)))
      320px;
    grid-template-rows: auto 1fr auto auto;
    grid-template-areas:
      'controls controls'
      'frame    side'
      'steps    side'
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

  .wb-int.is-expanded .wb-int-steps {
    grid-area: steps;
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

/* transport: play and fullscreen, with fixed widths so the toggling
   labels don't shift the row */
.wb-int-buttons {
  display: flex;
  align-items: center;
  gap: 8px;
  /* when the row wraps, the buttons right-align instead of dangling
     under the tabs */
  margin-left: auto;
}

.wb-int-btn.is-play {
  min-width: 58px;
  text-align: center;
}

.wb-int-btn.is-fs {
  min-width: 96px;
  text-align: center;
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
  transition: stroke 0.4s, opacity 0.3s;
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

/* labels knock out whatever wire runs beneath them, the same way group
   labels sit on their borders */
.wb-int-wire-label {
  position: absolute;
  padding-inline: 3px;
  background: var(--vp-c-bg);
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

/* stages, wires and labels outside the current scenario step back */
.wb-int-wire.is-dim,
.wb-int-wire-label.is-dim,
.wb-int-node.is-dim {
  opacity: 0.3;
}

.wb-int-wire-label,
.wb-int-node {
  transition: opacity 0.3s;
}

.wb-int-node.is-dim:hover {
  opacity: 0.8;
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

/* --- step callout ------------------------------------------------------
   The caption card beside the current step's stages, tinted by the
   step's color; it never takes clicks meant for the stages beneath. */

.wb-int-callout-layer {
  --wb-callout: var(--vp-c-brand-1);
  position: absolute;
  inset: 0;
  z-index: 2;
  pointer-events: none;
}

.wb-int-callout-layer.is-blue {
  --wb-callout: var(--wb-accent-blue);
}

.wb-int-leader {
  position: absolute;
  inset: 0;
  width: 100%;
  height: 100%;
}

.wb-int-leader line {
  stroke: var(--wb-callout);
  stroke-width: 1;
  stroke-dasharray: 2 2;
}

.wb-int-leader circle {
  fill: var(--wb-callout);
}

.wb-int-callout {
  position: absolute;
  box-sizing: border-box;
  padding: 7px 10px;
  border: 1px solid color-mix(in srgb, var(--wb-callout) 55%, var(--vp-c-divider));
  border-radius: 2px;
  background: color-mix(in srgb, var(--wb-callout) 8%, var(--vp-c-bg));
  box-shadow: 0 4px 16px rgb(0 0 0 / 0.18);
  font-size: 12px;
  line-height: 17px;
  color: var(--vp-c-text-1);
}

.wb-int-callout-enter-active {
  transition: opacity 0.25s;
}

.wb-int-callout-leave-active {
  transition: opacity 0.15s;
}

.wb-int-callout-enter-from,
.wb-int-callout-leave-to {
  opacity: 0;
}

/* --- step strip -------------------------------------------------------
   One segment per step: done steps keep a soft tint, the current one
   fills as it plays (--wb-step-progress, set per frame). */

.wb-int-steps {
  display: flex;
  gap: 3px;
  margin-top: 12px;
}

.wb-int-step {
  flex: 1;
  height: 16px;
  padding: 0;
  border: none;
  background: none;
  cursor: pointer;
}

.wb-int-step i {
  position: relative;
  display: block;
  height: 3px;
  background: var(--vp-c-divider);
  transition: background-color 0.2s;
}

.wb-int-step:hover i {
  background: var(--vp-c-text-3);
}

.wb-int-step:focus-visible {
  outline: 2px solid var(--vp-c-brand-1);
  outline-offset: 1px;
}

/* no fade in: the base colour under the current step's fill would show through */
.wb-int-step.is-done i {
  background: color-mix(in srgb, var(--vp-c-brand-1) 45%, var(--vp-c-divider));
  transition: none;
}

.wb-int-step.is-done.is-blue i {
  background: color-mix(in srgb, var(--wb-accent-blue) 45%, var(--vp-c-divider));
}

.wb-int-step.is-current i::after {
  content: '';
  position: absolute;
  inset: 0 auto 0 0;
  width: calc(var(--wb-step-progress, 1) * 100%);
  background: var(--vp-c-brand-1);
}

.wb-int-step.is-current.is-blue i::after {
  background: var(--wb-accent-blue);
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
