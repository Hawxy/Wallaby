<script setup lang="ts">
import { ref } from 'vue';
import { lsn, tickLsn } from './lsn';
import { formatCount } from './flow/format';
import { useFlowCycle, type FlowStep } from './flow/useFlowCycle';
import FlowChip from './flow/FlowChip.vue';
import FlowLink from './flow/FlowLink.vue';

// The hero "image": a live vertical pipeline - postgres (WAL position)
// → wallaby (stages + slot status) → rotating destination (delivered
// counter). Every 3s a packet flows through and the numbers update in
// step with it. Telemetry is fake, so the whole widget is hidden from
// assistive tech.
const delivered = ref(1184); // deterministic start - SSR hydration
const pulsingTop = ref(false);
const pulsingBottom = ref(false);
// 0 = idle, 1..3 = decode/transform/deliver lighting up in sequence
const stage = ref(0);
// end chips flash as the packet is emitted / arrives
const sourceActive = ref(false);
const destActive = ref(false);

// destination cycles with each batch; starts (and, under reduced
// motion, stays) on the catch-all
const destinations = ['anywhere', 'search index', 'vector db', 'http endpoint', 'event stream'];
const destIndex = ref(0);

let cycleCount = 0;

// One packet flowing through, with the numbers telling the same story:
// the WAL advances as the packet leaves postgres (0.6s travel per
// connector), wallaby "processes" it - the stage words light up in
// sequence - and the delivered counter bumps when the packet lands.
useFlowCycle({
  kick: 500,
  interval: 3000,
  cycle: (): FlowStep[] => {
    cycleCount += 1;
    // destination rotates every other cycle, before the packet
    // launches, so a delivery never lands on a mid-swap word
    if (cycleCount % 2 === 0) {
      destIndex.value = (destIndex.value + 1) % destinations.length;
    }
    return [
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
    ];
  },
});
</script>

<template>
  <div class="wb-flow" aria-hidden="true">
    <FlowChip :flash="sourceActive">
      <div class="wb-chip-title">postgres</div>
      <div class="wb-chip-sub">
        wal @ <span class="wb-chip-val">{{ lsn }}</span>
      </div>
    </FlowChip>

    <FlowLink class="wb-flow-link" :pulsing="pulsingTop" />

    <FlowChip class="is-wallaby" :class="{ 'is-processing': stage > 0 }">
      <div class="wb-chip-title">
        <span class="wb-flow-name">wallaby</span>
      </div>
      <div class="wb-chip-sub">
        slot: wallaby_cdc <span class="wb-flow-state">► active</span>
      </div>
      <div class="wb-chip-sub">
        <span class="wb-flow-stage" :class="{ 'is-on': stage === 1 }">decode</span> ▸
        <span class="wb-flow-stage" :class="{ 'is-on': stage === 2 }">transform</span> ▸
        <span class="wb-flow-stage" :class="{ 'is-on': stage === 3 }">deliver</span>
      </div>
    </FlowChip>

    <FlowLink class="wb-flow-link" :pulsing="pulsingBottom" />

    <FlowChip :flash="destActive">
      <div class="wb-chip-title">
        <Transition name="wb-swipe" mode="out-in">
          <span :key="destIndex" class="wb-flow-dest">{{ destinations[destIndex] }}</span>
        </Transition>
      </div>
      <div class="wb-chip-sub">
        <span class="wb-chip-val">{{ formatCount(delivered) }}</span>
        changes delivered
      </div>
    </FlowChip>
  </div>
</template>

<style scoped>
.wb-flow {
  width: 100%;
  max-width: 400px;
  font-family: var(--vp-font-family-mono);
  text-align: left;
}

.wb-chip.is-wallaby {
  border-color: var(--vp-c-brand-1);
}

/* processing: border steps up a shade while the stage words carry the
   story */
.wb-chip.is-wallaby.is-processing {
  border-color: var(--vp-c-brand-2);
}

.wb-flow-stage.is-on {
  color: var(--vp-c-brand-1);
}

.wb-flow-name {
  color: var(--vp-c-brand-1);
}

.wb-flow-state {
  color: var(--vp-c-brand-1);
}

.wb-flow-link {
  margin: 0 auto;
}

/* destination swap: the old word wipes out left-to-right, the new one
   wipes in behind it - steps() keeps the sweep chunky, in character.
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
