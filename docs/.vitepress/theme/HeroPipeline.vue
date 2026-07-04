<script setup lang="ts">
import { ref, onMounted, onUnmounted } from 'vue';
import { lsn, tickLsn } from './lsn';

// The hero "image": a live vertical pipeline — postgres (WAL position)
// → wallaby (stages + slot status) → rotating destination (delivered
// counter). Every 2.5s a packet flows through and the numbers update in
// step with it. Telemetry is fake, so the whole widget is hidden from
// assistive tech.
const delivered = ref(1184); // deterministic start — SSR hydration
const pulsingTop = ref(false);
const pulsingBottom = ref(false);
// 0 = idle, 1..3 = decode/transform/deliver lighting up in sequence
const stage = ref(0);
// end chips flash as the packet is emitted / arrives
const sourceActive = ref(false);
const destActive = ref(false);

// destination cycles with each batch; starts (and, under reduced
// motion, stays) on the catch-all
const destinations = ['anywhere', 'search index', 'vector db'];
const destIndex = ref(0);

let cycleTimer: ReturnType<typeof setInterval> | undefined;
let stepTimers: ReturnType<typeof setTimeout>[] = [];
let cycleCount = 0;

// One packet flowing through, with the numbers telling the same story:
// the WAL advances as the packet leaves postgres (0.6s travel per
// connector), wallaby "processes" it — the stage words light up in
// sequence and the slot line reads processing — and the delivered
// counter bumps when the packet lands.
function runPulseCycle() {
  stepTimers = ([
    [0, () => { tickLsn(); sourceActive.value = true; pulsingTop.value = true; }],
    [400, () => (sourceActive.value = false)],
    [600, () => (stage.value = 1)],
    [650, () => (pulsingTop.value = false)],
    [900, () => (stage.value = 2)],
    [1200, () => (stage.value = 3)],
    [1500, () => { stage.value = 0; pulsingBottom.value = true; }],
    [2100, () => {
      delivered.value += 6 + Math.floor(Math.random() * 34);
      destActive.value = true;
    }],
    [2150, () => (pulsingBottom.value = false)],
    [2450, () => (destActive.value = false)],
  ] as [number, () => void][]).map(([ms, fn]) => setTimeout(fn, ms));
}

function formatCount(n: number) {
  return n.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

onMounted(() => {
  if (!window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
    cycleTimer = setInterval(() => {
      // destination rotates every other cycle, before the packet launches,
      // so a delivery never lands on a mid-swap word
      cycleCount += 1;
      if (cycleCount % 2 === 0) {
        destIndex.value = (destIndex.value + 1) % destinations.length;
      }
      runPulseCycle();
      // the sequence ends at ~2.45s; the extra half second is a rest
      // beat before the next packet
    }, 3000);
  }
});

onUnmounted(() => {
  if (cycleTimer) clearInterval(cycleTimer);
  stepTimers.forEach(clearTimeout);
});
</script>

<template>
  <div class="wb-flow" aria-hidden="true">
    <div class="wb-flow-chip" :class="{ 'is-active': sourceActive }">
      <div class="wb-flow-title">postgres</div>
      <div class="wb-flow-sub">
        wal @ <span class="wb-flow-lsn">{{ lsn }}</span>
      </div>
    </div>

    <div class="wb-flow-link" :class="{ 'is-pulsing': pulsingTop }"></div>

    <div class="wb-flow-chip is-wallaby" :class="{ 'is-processing': stage > 0 }">
      <div class="wb-flow-title">
        <span class="wb-flow-name">wallaby</span>
      </div>
      <div class="wb-flow-sub">
        slot: wallaby_cdc <span class="wb-flow-state">► active</span>
      </div>
      <div class="wb-flow-sub">
        <span class="wb-flow-stage" :class="{ 'is-on': stage === 1 }">decode</span> ▸
        <span class="wb-flow-stage" :class="{ 'is-on': stage === 2 }">transform</span> ▸
        <span class="wb-flow-stage" :class="{ 'is-on': stage === 3 }">deliver</span>
      </div>
    </div>

    <div class="wb-flow-link" :class="{ 'is-pulsing': pulsingBottom }"></div>

    <div class="wb-flow-chip" :class="{ 'is-active': destActive }">
      <div class="wb-flow-title">
        <Transition name="wb-swipe" mode="out-in">
          <span :key="destIndex" class="wb-flow-dest">{{ destinations[destIndex] }}</span>
        </Transition>
      </div>
      <div class="wb-flow-sub">
        <span class="wb-flow-count">{{ formatCount(delivered) }}</span>
        changes delivered
      </div>
    </div>
  </div>
</template>

<style scoped>
.wb-flow {
  width: 100%;
  max-width: 400px;
  font-family: var(--vp-font-family-mono);
  text-align: left;
}

.wb-flow-chip {
  padding: 10px 16px 11px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 2px;
  background-color: var(--vp-code-block-bg);
  transition: border-color 0.4s;
}

/* end chips flash blue as the packet is emitted / arrives — matching
   their data values (LSN, counter), which update in the same instant */
.wb-flow-chip.is-active {
  border-color: var(--wb-accent-blue);
}

.wb-flow-chip.is-wallaby {
  border-color: var(--vp-c-brand-1);
}

/* processing: border steps up a shade while the stage words carry the
   story — no shadow; the light theme is paper, and paper doesn't glow */
.wb-flow-chip.is-wallaby.is-processing {
  border-color: var(--vp-c-brand-2);
}

.wb-flow-stage.is-on {
  color: var(--vp-c-brand-1);
}

.wb-flow-title {
  font-size: 14px;
  color: var(--vp-c-text-1);
}

.wb-flow-name {
  color: var(--vp-c-brand-1);
}

.wb-flow-sub {
  margin-top: 2px;
  font-size: 12px;
  line-height: 18px;
  color: var(--vp-c-text-3);
}

.wb-flow-lsn,
.wb-flow-count {
  color: var(--wb-accent-blue);
}

.wb-flow-state {
  color: var(--vp-c-brand-1);
}

/* connector: hairline between chips; a square amber dot travels down it
   when a batch lands */
.wb-flow-link {
  position: relative;
  width: 1px;
  height: 28px;
  margin: 0 auto;
  background-color: var(--vp-c-divider);
}

.wb-flow-link::before {
  content: '';
  position: absolute;
  top: 0;
  left: -2px;
  width: 5px;
  height: 5px;
  background-color: var(--vp-c-brand-1);
  opacity: 0;
}

@media (prefers-reduced-motion: no-preference) {
  .wb-flow-link.is-pulsing::before {
    animation: wb-flow-travel 0.6s linear;
  }
}

@keyframes wb-flow-travel {
  0% {
    top: 0;
    opacity: 1;
  }
  100% {
    top: calc(100% - 5px);
    opacity: 1;
  }
}

/* destination swap: the old word wipes out left-to-right, the new one
   wipes in behind it — steps() keeps the sweep chunky, in character.
   Only ever triggered by the batch timer, which reduced-motion skips. */
.wb-flow-dest {
  display: inline-block;
}

.wb-swipe-leave-active {
  animation: wb-swipe-out 0.22s steps(8);
}

.wb-swipe-enter-active {
  animation: wb-swipe-in 0.28s steps(8);
}

@keyframes wb-swipe-out {
  from {
    clip-path: inset(0 0 0 0);
  }
  to {
    clip-path: inset(0 0 0 100%);
  }
}

@keyframes wb-swipe-in {
  from {
    clip-path: inset(0 100% 0 0);
  }
  to {
    clip-path: inset(0 0 0 0);
  }
}

</style>
