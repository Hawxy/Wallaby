<script setup lang="ts">
import { ref, onMounted, onUnmounted } from 'vue';
import { withBase } from 'vitepress';
import { lsn, tickLsn } from './lsn';

// "Choose your configuration" picker: postgres fans out over a bus into
// two lanes — the capture lane (providers box → sinks box) and the
// provision-only lane (external slots → pgoutput consumer) — with the
// setup buttons embedded in the chips. A packet flows down a different
// path each cycle.
const providers = [
  {
    title: 'efcore provider',
    sub: 'relational storage',
    label: 'EFCore Setup →',
    link: '/providers/entity-framework-core',
  },
  {
    title: 'marten provider',
    sub: 'document storage',
    label: 'Marten Setup →',
    link: '/providers/marten',
  },
];

const sinks = [
  {
    title: 'meilisearch',
    sub: 'search index',
    label: 'Meilisearch →',
    link: '/sinks/meilisearch',
  },
  {
    title: 'custom',
    sub: 'your own delivery target',
    label: 'Custom Sinks →',
    link: '/sinks/custom',
  },
];

// 0/1 = provider path, 2 = external slots path; rotates each cycle
const target = ref(0);
// chip processing the packet (amber): '' | 'p0' | 'p1' | 'ext'
const lit = ref('');
// chip whose data just updated (blue): '' | 'src' | 's0' | 's1' | 'consumer'
const flash = ref('');
// which connector segment the packet is on
const pulse = ref('');

let kickTimer: ReturnType<typeof setTimeout> | undefined;
let cycleTimer: ReturnType<typeof setInterval> | undefined;
let stepTimers: ReturnType<typeof setTimeout>[] = [];
// providers alternate which sink they deliver to across rounds
let round = 0;

// stem (0.5s) → bus toward a lane (0.35s) → drop into the box (0.5s) →
// chip processes (amber ~0.75s) → drop to the lane's destination →
// destination flashes blue as the delivery lands
function runCycle() {
  const t = target.value;
  const provider = t < 2;
  const sink = (t + round) % 2;
  stepTimers = ([
    [0, () => { tickLsn(); flash.value = 'src'; pulse.value = 'stem'; }],
    [400, () => (flash.value = '')],
    [500, () => (pulse.value = provider ? 'bus-left' : 'bus-right')],
    [850, () => (pulse.value = provider ? 'drop-left' : 'drop-right')],
    [1350, () => { pulse.value = ''; lit.value = provider ? 'p' + t : 'ext'; }],
    [2100, () => { lit.value = ''; pulse.value = provider ? 'drop-sinks' : 'drop-consumer'; }],
    [2600, () => { pulse.value = ''; flash.value = provider ? 's' + sink : 'consumer'; }],
    [3000, () => (flash.value = '')],
  ] as [number, () => void][]).map(([ms, fn]) => setTimeout(fn, ms));
}

onMounted(() => {
  if (!window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
    // first packet after a short settle instead of a 3.6s empty stare
    kickTimer = setTimeout(runCycle, 500);
    cycleTimer = setInterval(() => {
      target.value = (target.value + 1) % 3;
      if (target.value === 0) round += 1;
      runCycle();
    }, 3600);
  }
});

onUnmounted(() => {
  if (kickTimer) clearTimeout(kickTimer);
  if (cycleTimer) clearInterval(cycleTimer);
  stepTimers.forEach(clearTimeout);
});
</script>

<template>
  <div class="wb-config">
    <!-- the source chip and connectors are decoration (fake telemetry);
         the boxes below carry the real content and links -->
    <div class="wb-config-top" aria-hidden="true">
      <div class="wb-config-chip is-src" :class="{ 'is-flash': flash === 'src' }">
        <div class="wb-config-title">postgres</div>
        <div class="wb-config-sub">
          wal @ <span class="wb-config-val">{{ lsn }}</span>
        </div>
      </div>
      <div class="wb-config-stem" :class="{ 'is-pulsing': pulse === 'stem' }"></div>
      <div
        class="wb-config-bus"
        :class="{ 'is-left': pulse === 'bus-left', 'is-right': pulse === 'bus-right' }"
      ></div>
    </div>

    <!-- one shared grid so lane rows stay the same height: drops on
         row 1 and 3, providers/external on row 2, sinks/consumer on
         row 4 -->
    <div class="wb-config-grid">
      <div
        class="wb-config-drop"
        :class="{ 'is-pulsing': pulse === 'drop-left' }"
        aria-hidden="true"
      ></div>
      <div
        class="wb-config-drop"
        :class="{ 'is-pulsing': pulse === 'drop-right' }"
        aria-hidden="true"
      ></div>

      <div class="wb-config-group is-providers">
        <div class="wb-config-group-label">providers</div>
        <div class="wb-config-group-grid">
          <div
            v-for="(p, i) in providers"
            :key="p.title"
            class="wb-config-chip is-option"
            :class="{ 'is-lit': lit === 'p' + i }"
          >
            <div class="wb-config-title">{{ p.title }}</div>
            <div class="wb-config-sub">{{ p.sub }}</div>
            <a class="wb-btn" :href="withBase(p.link)">{{ p.label }}</a>
          </div>
        </div>
      </div>

      <div class="wb-config-chip is-option is-ext" :class="{ 'is-lit': lit === 'ext' }">
        <div class="wb-config-title">external slots</div>
        <div class="wb-config-sub">provision publications + slots for an external consumer</div>
        <a class="wb-btn" :href="withBase('/external-slots')">External Slots →</a>
      </div>

      <div
        class="wb-config-drop"
        :class="{ 'is-pulsing': pulse === 'drop-sinks' }"
        aria-hidden="true"
      ></div>
      <div
        class="wb-config-drop"
        :class="{ 'is-pulsing': pulse === 'drop-consumer' }"
        aria-hidden="true"
      ></div>

      <div class="wb-config-group is-sinks">
        <div class="wb-config-group-label">sinks</div>
        <div class="wb-config-group-grid">
          <div
            v-for="(s, i) in sinks"
            :key="s.title"
            class="wb-config-chip is-option"
            :class="{ 'is-flash': flash === 's' + i }"
          >
            <div class="wb-config-title">{{ s.title }}</div>
            <div class="wb-config-sub">{{ s.sub }}</div>
            <a class="wb-btn" :href="withBase(s.link)">{{ s.label }}</a>
          </div>
        </div>
      </div>

      <div
        class="wb-config-chip is-option is-consumer"
        :class="{ 'is-flash': flash === 'consumer' }"
      >
        <div class="wb-config-title">pgoutput consumer</div>
        <div class="wb-config-sub">Airbyte / Fivetran / etc</div>
        <div class="wb-config-sub">reads the slot directly</div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.wb-config {
  max-width: 680px;
  margin: 32px auto;
  font-family: var(--vp-font-family-mono);
}

.wb-config-top {
  display: flex;
  flex-direction: column;
  align-items: center;
}

.wb-config-chip {
  padding: 10px 16px 11px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 2px;
  background-color: var(--vp-code-block-bg);
  transition: border-color 0.4s;
}

.wb-config-chip.is-src {
  width: 200px;
}

/* amber while the chip "processes" the packet */
.wb-config-chip.is-lit {
  border-color: var(--vp-c-brand-1);
  transition: border-color 0.2s;
}

/* blue as the chip's data updates — the LSN moves, a delivery lands */
.wb-config-chip.is-flash {
  border-color: var(--wb-accent-blue);
  transition: border-color 0.2s;
}

.wb-config-chip.is-option {
  display: flex;
  flex-direction: column;
}

.wb-config-title {
  font-size: 14px;
  line-height: 20px;
  color: var(--vp-c-text-1);
}

.wb-config-sub {
  margin-top: 2px;
  font-size: 12px;
  line-height: 18px;
  color: var(--vp-c-text-3);
}

.wb-config-sub:last-of-type {
  margin-bottom: 12px;
}

.wb-config-val {
  color: var(--wb-accent-blue);
}

/* embedded buttons: compact form of the global .wb-btn, pinned to the
   chip bottom so siblings line up */
.wb-config-chip .wb-btn {
  display: block;
  margin-top: auto;
  padding: 5px 10px;
  font-size: 12px;
  text-align: center;
}

/* group boxes: a frame around the provider and sink chips */
.wb-config-group {
  padding: 10px 12px 12px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 2px;
}

.wb-config-group-label {
  margin-bottom: 8px;
  font-size: 11px;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--vp-c-text-3);
}

.wb-config-group-grid {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 12px;
}

/* stem: hairline from postgres down to the bus */
.wb-config-stem {
  position: relative;
  width: 1px;
  height: 24px;
  background-color: var(--vp-c-divider);
}

.wb-config-stem::before,
.wb-config-drop::before {
  content: '';
  position: absolute;
  top: 0;
  left: -2px;
  width: 5px;
  height: 5px;
  background-color: var(--vp-c-brand-1);
  opacity: 0;
}

/* bus: horizontal rail from the capture lane's center to the external
   lane's center (half a column in from each side of the 2fr/1fr grid) */
.wb-config-bus {
  position: relative;
  align-self: stretch;
  height: 1px;
  margin-left: calc((100% - 16px) / 3);
  margin-right: calc((100% - 16px) / 6);
  background-color: var(--vp-c-divider);
}

/* dot rests where the stem meets the bus (bus-local: a third in,
   nudged for the grid gap) */
.wb-config-bus::before {
  content: '';
  position: absolute;
  top: -2px;
  left: calc(33.333% + 0.67px);
  width: 5px;
  height: 5px;
  background-color: var(--vp-c-brand-1);
  opacity: 0;
}

/* capture lane gets two thirds, provision lane one third; grid rows
   keep the two lanes' boxes the same height */
.wb-config-grid {
  display: grid;
  grid-template-columns: 2fr 1fr;
  column-gap: 16px;
}

/* drops: hairlines between the bus, boxes, and destinations */
.wb-config-drop {
  position: relative;
  justify-self: center;
  width: 1px;
  height: 24px;
  background-color: var(--vp-c-divider);
}

@media (prefers-reduced-motion: no-preference) {
  .wb-config-stem.is-pulsing::before,
  .wb-config-drop.is-pulsing::before {
    animation: wb-config-drop-anim 0.5s linear;
  }

  .wb-config-bus.is-left::before {
    animation: wb-config-cross-left 0.35s linear;
  }

  .wb-config-bus.is-right::before {
    animation: wb-config-cross-right 0.35s linear;
  }
}

@keyframes wb-config-drop-anim {
  0% {
    top: 0;
    opacity: 1;
  }
  100% {
    top: calc(100% - 5px);
    opacity: 1;
  }
}

@keyframes wb-config-cross-left {
  0% {
    left: calc(33.333% + 0.67px);
    opacity: 1;
  }
  100% {
    left: -2px;
    opacity: 1;
  }
}

@keyframes wb-config-cross-right {
  0% {
    left: calc(33.333% + 0.67px);
    opacity: 1;
  }
  100% {
    left: calc(100% - 3px);
    opacity: 1;
  }
}

/* narrow screens: the lanes don't fit side by side — stack the boxes
   lane by lane and let the lit borders alone carry the motion */
@media (max-width: 639px) {
  .wb-config-top,
  .wb-config-drop {
    display: none;
  }

  .wb-config-grid,
  .wb-config-group-grid {
    grid-template-columns: 1fr;
  }

  .wb-config-grid {
    gap: 12px;
  }

  .wb-config-group.is-providers {
    order: 1;
  }

  .wb-config-group.is-sinks {
    order: 2;
  }

  .wb-config-chip.is-ext {
    order: 3;
  }

  .wb-config-chip.is-consumer {
    order: 4;
  }
}
</style>
