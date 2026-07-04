<script setup lang="ts">
import { ref, onMounted, onUnmounted } from 'vue';
import { lsn, subscribeLsn, unsubscribeLsn } from './lsn';

// The hero "image": a live vertical pipeline — postgres (WAL position,
// shared ticker) → wallaby (stages + slot status) → anywhere (delivered
// counter). Every few seconds a batch lands: a pulse runs down the
// connectors and the counter jumps. Telemetry is fake, so the whole
// widget is hidden from assistive tech.
const delivered = ref(1184); // deterministic start — SSR hydration
const pulsing = ref(false);

// destination cycles with each batch; starts (and, under reduced
// motion, stays) on the catch-all
const destinations = ['anywhere', 'search index', 'vector db'];
const destIndex = ref(0);

let batchTimer: ReturnType<typeof setInterval> | undefined;
let pulseTimer: ReturnType<typeof setTimeout> | undefined;

function formatCount(n: number) {
  return n.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

onMounted(() => {
  subscribeLsn();
  if (!window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
    batchTimer = setInterval(() => {
      delivered.value += 12 + Math.floor(Math.random() * 68);
      destIndex.value = (destIndex.value + 1) % destinations.length;
      pulsing.value = true;
      pulseTimer = setTimeout(() => (pulsing.value = false), 700);
    }, 5000);
  }
});

onUnmounted(() => {
  unsubscribeLsn();
  if (batchTimer) clearInterval(batchTimer);
  if (pulseTimer) clearTimeout(pulseTimer);
});
</script>

<template>
  <div class="wb-flow" aria-hidden="true">
    <div class="wb-flow-chip">
      <div class="wb-flow-title">postgres</div>
      <div class="wb-flow-sub">
        wal @ <span class="wb-flow-lsn">{{ lsn }}</span>
      </div>
    </div>

    <div class="wb-flow-link" :class="{ 'is-pulsing': pulsing }"></div>

    <div class="wb-flow-chip is-wallaby">
      <div class="wb-flow-title">
        <span class="wb-flow-name">wallaby</span>
      </div>    
      <div class="wb-flow-sub">
        slot: wallaby_cdc <span class="wb-flow-state">► active</span>
      </div>
      <div class="wb-flow-sub">decode ▸ transform ▸ deliver</div>
    </div>

    <div class="wb-flow-link" :class="{ 'is-pulsing': pulsing }"></div>

    <div class="wb-flow-chip">
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
}

.wb-flow-chip.is-wallaby {
  border-color: var(--vp-c-brand-1);
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
