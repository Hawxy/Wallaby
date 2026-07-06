<script setup lang="ts">
import { ref } from 'vue';
import { lsn, tickLsn } from './lsn';
import { formatCount } from './flow/format';
import { useFlowCycle, type FlowStep } from './flow/useFlowCycle';
import FlowChip from './flow/FlowChip.vue';
import FlowLink from './flow/FlowLink.vue';

// Animated replacement for the "How It Works" flowchart: the full
// delivery loop. A packet leaves postgres, is read, transformed and
// delivered, then the acknowledgement rides the rail back up and the
// slot's flushed position catches up to the WAL — the at-least-once
// ordering, drawn. Every fourth cycle a backfill batch (blue) enters
// the same path through the read stage.
const delivered = ref(23481); // deterministic start — SSR hydration
const flushed = ref(lsn.value);

// which chip is processing (amber border)
const lit = ref('');
// which chip is flashing blue — its data value just updated
const flash = ref('');
// which connector the packet is on
const pulse = ref('');
// backfill cycles tint the packet blue
const isBackfill = ref(false);

// the WAL position this packet was read at; flushed catches up to it
// only once the acknowledgement makes it back
let inFlightLsn = '';
let cycleCount = 0;

function liveSteps(): FlowStep[] {
  return [
    [0, () => {
      tickLsn();
      inFlightLsn = lsn.value;
      flash.value = 'pg';
      pulse.value = 'l1';
    }],
    [400, () => (flash.value = '')],
    [600, () => { lit.value = 'read'; pulse.value = ''; }],
    [900, () => (pulse.value = 'l2')],
    [1500, () => { lit.value = 'xform'; pulse.value = ''; }],
    [1800, () => (pulse.value = 'l3')],
    [2400, () => {
      lit.value = '';
      pulse.value = '';
      delivered.value += 6 + Math.floor(Math.random() * 34);
      flash.value = 'sinks';
    }],
    // sinks stays lit while the packet rides to ack — matching the
    // ~1s hold of the processing chips, so its decay doesn't read as
    // faster than theirs
    [2700, () => (pulse.value = 'l4')],
    [3300, () => { lit.value = 'ack'; pulse.value = ''; }],
    [3400, () => (flash.value = '')],
    [3600, () => { lit.value = ''; pulse.value = 'rail'; }],
    [4500, () => {
      pulse.value = '';
      flushed.value = inFlightLsn;
      flash.value = 'pg';
    }],
    [4900, () => (flash.value = '')],
  ];
}

// backfill: snapshot rows join at read and flow through the same
// transform + sink path, but no WAL position rides along — the loop
// back to postgres stays quiet
function backfillSteps(): FlowStep[] {
  return [
    [0, () => { flash.value = 'backfill'; pulse.value = 'bf'; }],
    [400, () => { flash.value = ''; lit.value = 'read'; pulse.value = ''; }],
    [700, () => (pulse.value = 'l2')],
    [1300, () => { lit.value = 'xform'; pulse.value = ''; }],
    [1600, () => (pulse.value = 'l3')],
    [2200, () => {
      lit.value = '';
      pulse.value = '';
      delivered.value += 180 + Math.floor(Math.random() * 120);
      flash.value = 'sinks';
    }],
    [3200, () => (flash.value = '')],
  ];
}

useFlowCycle({
  kick: 600,
  // the live sequence ends at ~4.9s; the rest is a beat between
  // packets — same ~600ms breather as the homepage widget
  interval: 5500,
  cycle: () => {
    cycleCount += 1;
    isBackfill.value = cycleCount % 4 === 0;
    return isBackfill.value ? backfillSteps() : liveSteps();
  },
});
</script>

<template>
  <div
    class="wb-pipe"
    role="img"
    aria-label="Wallaby pipeline: Postgres logical replication streams committed transactions into read and materialize, then transform and route, then sinks. Once every sink accepts the batch, acknowledge and checkpoint advances the replication slot back in Postgres. Backfill snapshots tables in keyset chunks and feeds the rows into the same path."
  >
    <div class="wb-pipe-col">
      <FlowChip :flash="flash === 'pg'">
        <div class="wb-chip-title">postgres</div>
        <div class="wb-chip-sub">
          wal @ <span class="wb-chip-val">{{ lsn }}</span>
        </div>
        <div class="wb-chip-sub">
          flushed @ <span class="wb-chip-val">{{ flushed }}</span>
        </div>
      </FlowChip>

      <FlowLink :pulsing="pulse === 'l1'" :blue="isBackfill">
        <span class="wb-pipe-link-label">logical replication</span>
      </FlowLink>

      <div class="wb-pipe-branch">
        <FlowChip :lit="lit === 'read'">
          <div class="wb-chip-title">read + materialize</div>
          <div class="wb-chip-sub">committed tx ▸ change events</div>
        </FlowChip>
        <FlowChip class="is-backfill-chip" :flash="flash === 'backfill'">
          <div class="wb-chip-title">backfill</div>
          <div class="wb-chip-sub">keyset chunks</div>
          <div class="wb-chip-sub">snapshot rows</div>
        </FlowChip>
        <div class="wb-pipe-hlink" :class="{ 'is-pulsing': pulse === 'bf' }"></div>
      </div>

      <FlowLink :pulsing="pulse === 'l2'" :blue="isBackfill" />

      <FlowChip :lit="lit === 'xform'">
        <div class="wb-chip-title">transform + route</div>
        <div class="wb-chip-sub">shape documents ▸ batch</div>
      </FlowChip>

      <FlowLink :pulsing="pulse === 'l3'" :blue="isBackfill" />

      <FlowChip :flash="flash === 'sinks'">
        <div class="wb-chip-title">sinks</div>
        <div class="wb-chip-sub">
          <span class="wb-chip-val">{{ formatCount(delivered) }}</span>
          changes delivered
        </div>
      </FlowChip>

      <FlowLink :pulsing="pulse === 'l4'" :blue="isBackfill" />

      <FlowChip :lit="lit === 'ack'">
        <div class="wb-chip-title">acknowledge + checkpoint</div>
        <div class="wb-chip-sub">confirm delivery ▸ advance slot</div>
      </FlowChip>
    </div>

    <div class="wb-pipe-rail" :class="{ 'is-pulsing': pulse === 'rail' }">
      <span class="wb-pipe-rail-label">advance slot</span>
    </div>
  </div>
</template>

<style scoped>
.wb-pipe {
  position: relative;
  width: fit-content;
  margin: 32px auto;
  font-family: var(--vp-font-family-mono);
  /* processing chips decay back to gray lazily on this diagram */
  --wb-chip-decay: 0.8s;
}

.wb-pipe-col {
  display: flex;
  flex-direction: column;
  align-items: center;
}

.wb-pipe .wb-chip {
  width: 260px;
}

.wb-pipe-link-label {
  position: absolute;
  top: 50%;
  left: 10px;
  transform: translateY(-50%);
  font-size: 11px;
  white-space: nowrap;
  color: var(--vp-c-text-3);
}

/* backfill hangs off the read stage */
.wb-pipe-branch {
  position: relative;
}

.wb-pipe .wb-chip.is-backfill-chip {
  position: absolute;
  top: 50%;
  right: calc(100% + 20px);
  transform: translateY(-50%);
  width: 112px;
  padding: 7px 12px 8px;
}

.wb-chip.is-backfill-chip .wb-chip-title {
  font-size: 12px;
  line-height: 16px;
}

.wb-chip.is-backfill-chip .wb-chip-sub {
  font-size: 11px;
  line-height: 16px;
}

.wb-pipe-hlink {
  position: absolute;
  top: 50%;
  right: 100%;
  width: 20px;
  height: 1px;
  background-color: var(--vp-c-divider);
}

.wb-pipe-hlink::before {
  content: '';
  position: absolute;
  top: -2px;
  left: 0;
  width: 5px;
  height: 5px;
  background-color: var(--wb-accent-blue);
  opacity: 0;
}

/* the acknowledgement's ride home: a bracket from the checkpoint back
   up to postgres */
.wb-pipe-rail {
  position: absolute;
  /* pinned to the midpoints of the postgres and ack chips */
  top: 41px;
  bottom: 31px;
  left: 100%;
  width: 18px;
  border: 1px solid var(--vp-c-divider);
  border-left: none;
  border-radius: 0 2px 2px 0;
}

.wb-pipe-rail::after {
  content: '';
  position: absolute;
  top: 0;
  right: -3px;
  width: 5px;
  height: 5px;
  background-color: var(--vp-c-brand-1);
  opacity: 0;
}

.wb-pipe-rail-label {
  position: absolute;
  top: 50%;
  left: calc(100% + 8px);
  transform: translateY(-50%) rotate(180deg);
  writing-mode: vertical-rl;
  font-size: 11px;
  white-space: nowrap;
  color: var(--vp-c-text-3);
}

@media (prefers-reduced-motion: no-preference) {
  .wb-pipe-hlink.is-pulsing::before {
    animation: wb-pipe-cross 0.4s linear;
  }

  .wb-pipe-rail.is-pulsing::after {
    animation: wb-pipe-rise 0.9s linear;
  }
}

@keyframes wb-pipe-cross {
  0% {
    left: 0;
    opacity: 1;
  }
  100% {
    left: calc(100% - 5px);
    opacity: 1;
  }
}

@keyframes wb-pipe-rise {
  0% {
    top: calc(100% - 5px);
    opacity: 1;
  }
  100% {
    top: 0;
    opacity: 1;
  }
}

/* narrow screens: tighten everything and drop the labels so the
   backfill branch and rail still fit */
@media (max-width: 639px) {
  .wb-pipe {
    margin: 24px 0 24px 118px;
  }

  .wb-pipe .wb-chip {
    width: 200px;
    padding: 8px 12px 9px;
  }

  /* small enough that "acknowledge + checkpoint" doesn't wrap */
  .wb-pipe .wb-chip-title {
    font-size: 12px;
  }

  .wb-pipe .wb-chip-sub {
    font-size: 11px;
    line-height: 16px;
  }

  .wb-chip.is-backfill-chip {
    right: calc(100% + 14px);
  }

  .wb-pipe-hlink {
    width: 14px;
  }

  .wb-pipe-rail {
    /* chip mids shift: some sub-lines wrap at this width */
    top: 37px;
    bottom: 36px;
    width: 14px;
  }

  .wb-pipe-link-label,
  .wb-pipe-rail-label {
    display: none;
  }
}
</style>
