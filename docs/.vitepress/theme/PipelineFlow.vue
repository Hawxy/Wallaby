<script setup lang="ts">
import { ref, onMounted, onUnmounted } from 'vue';
import { lsn, tickLsn } from './lsn';

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
let kickTimer: ReturnType<typeof setTimeout> | undefined;
let cycleTimer: ReturnType<typeof setInterval> | undefined;
let stepTimers: ReturnType<typeof setTimeout>[] = [];
let cycleCount = 0;

function runLiveCycle() {
  isBackfill.value = false;
  stepTimers = ([
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
  ] as [number, () => void][]).map(([ms, fn]) => setTimeout(fn, ms));
}

// backfill: snapshot rows join at read and flow through the same
// transform + sink path, but no WAL position rides along — the loop
// back to postgres stays quiet
function runBackfillCycle() {
  isBackfill.value = true;
  stepTimers = ([
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
  ] as [number, () => void][]).map(([ms, fn]) => setTimeout(fn, ms));
}

function formatCount(n: number) {
  return n.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

onMounted(() => {
  if (!window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
    // first packet after a short settle instead of a 5.5s empty stare
    kickTimer = setTimeout(() => {
      cycleCount = 1;
      runLiveCycle();
    }, 600);
    cycleTimer = setInterval(() => {
      cycleCount += 1;
      if (cycleCount % 4 === 0) {
        runBackfillCycle();
      } else {
        runLiveCycle();
      }
      // the live sequence ends at ~4.9s; the rest is a beat between
      // packets — same ~600ms breather as the homepage widget
    }, 5500);
  }
});

onUnmounted(() => {
  if (kickTimer) clearTimeout(kickTimer);
  if (cycleTimer) clearInterval(cycleTimer);
  stepTimers.forEach(clearTimeout);
});
</script>

<template>
  <div
    class="wb-pipe"
    :class="{ 'is-backfill': isBackfill }"
    role="img"
    aria-label="Wallaby pipeline: Postgres logical replication streams committed transactions into read and materialize, then transform and route, then sinks. Once every sink accepts the batch, acknowledge and checkpoint advances the replication slot back in Postgres. Backfill snapshots tables in keyset chunks and feeds the rows into the same path."
  >
    <div class="wb-pipe-col">
      <div class="wb-pipe-chip" :class="{ 'is-flash': flash === 'pg' }">
        <div class="wb-pipe-title">postgres</div>
        <div class="wb-pipe-sub">
          wal @ <span class="wb-pipe-val">{{ lsn }}</span>
        </div>
        <div class="wb-pipe-sub">
          flushed @ <span class="wb-pipe-val">{{ flushed }}</span>
        </div>
      </div>

      <div class="wb-pipe-link" :class="{ 'is-pulsing': pulse === 'l1' }">
        <span class="wb-pipe-link-label">logical replication</span>
      </div>

      <div class="wb-pipe-branch">
        <div class="wb-pipe-chip" :class="{ 'is-lit': lit === 'read' }">
          <div class="wb-pipe-title">read + materialize</div>
          <div class="wb-pipe-sub">committed tx ▸ change events</div>
        </div>
        <div
          class="wb-pipe-chip is-backfill-chip"
          :class="{ 'is-flash': flash === 'backfill' }"
        >
          <div class="wb-pipe-title">backfill</div>
          <div class="wb-pipe-sub">keyset chunks</div>
          <div class="wb-pipe-sub">snapshot rows</div>
        </div>
        <div class="wb-pipe-hlink" :class="{ 'is-pulsing': pulse === 'bf' }"></div>
      </div>

      <div class="wb-pipe-link" :class="{ 'is-pulsing': pulse === 'l2' }"></div>

      <div class="wb-pipe-chip" :class="{ 'is-lit': lit === 'xform' }">
        <div class="wb-pipe-title">transform + route</div>
        <div class="wb-pipe-sub">shape documents ▸ batch</div>
      </div>

      <div class="wb-pipe-link" :class="{ 'is-pulsing': pulse === 'l3' }"></div>

      <div class="wb-pipe-chip" :class="{ 'is-flash': flash === 'sinks' }">
        <div class="wb-pipe-title">sinks</div>
        <div class="wb-pipe-sub">
          <span class="wb-pipe-val">{{ formatCount(delivered) }}</span>
          changes delivered
        </div>
      </div>

      <div class="wb-pipe-link" :class="{ 'is-pulsing': pulse === 'l4' }"></div>

      <div class="wb-pipe-chip" :class="{ 'is-lit': lit === 'ack' }">
        <div class="wb-pipe-title">acknowledge + checkpoint</div>
        <div class="wb-pipe-sub">confirm delivery ▸ advance slot</div>
      </div>
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
}

.wb-pipe-col {
  display: flex;
  flex-direction: column;
  align-items: center;
}

.wb-pipe-chip {
  width: 260px;
  padding: 10px 16px 11px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 2px;
  background-color: var(--vp-code-block-bg);
  /* asymmetric: light up fast (the active-state rules below), decay
     lazily on the way back to gray */
  transition: border-color 0.8s;
}

/* amber while the packet is being processed inside the chip */
.wb-pipe-chip.is-lit {
  border-color: var(--vp-c-brand-1);
  transition: border-color 0.2s;
}

/* blue when the chip's data value updates — LSNs, delivered counter */
.wb-pipe-chip.is-flash {
  border-color: var(--wb-accent-blue);
  transition: border-color 0.2s;
}

.wb-pipe-title {
  font-size: 14px;
  /* the .vp-doc 28px line-height bloats the chips and shifts the rail
     off the chip midpoints */
  line-height: 20px;
  color: var(--vp-c-text-1);
}

.wb-pipe-sub {
  margin-top: 2px;
  font-size: 12px;
  line-height: 18px;
  color: var(--vp-c-text-3);
}

.wb-pipe-val {
  color: var(--wb-accent-blue);
}

/* vertical connector: hairline; a square dot travels down it as the
   packet moves between stages */
.wb-pipe-link {
  position: relative;
  width: 1px;
  height: 28px;
  background-color: var(--vp-c-divider);
}

.wb-pipe-link::before {
  content: '';
  position: absolute;
  top: 0;
  left: -2px;
  width: 5px;
  height: 5px;
  background-color: var(--vp-c-brand-1);
  opacity: 0;
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

.wb-pipe-chip.is-backfill-chip {
  position: absolute;
  top: 50%;
  right: calc(100% + 20px);
  transform: translateY(-50%);
  width: 112px;
  padding: 7px 12px 8px;
}

.wb-pipe-chip.is-backfill-chip .wb-pipe-title {
  font-size: 12px;
  line-height: 16px;
}

.wb-pipe-chip.is-backfill-chip .wb-pipe-sub {
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

/* backfill packets are blue on the shared connectors too */
.wb-pipe.is-backfill .wb-pipe-link::before {
  background-color: var(--wb-accent-blue);
}

@media (prefers-reduced-motion: no-preference) {
  .wb-pipe-link.is-pulsing::before {
    animation: wb-pipe-drop 0.6s linear;
  }

  .wb-pipe-hlink.is-pulsing::before {
    animation: wb-pipe-cross 0.4s linear;
  }

  .wb-pipe-rail.is-pulsing::after {
    animation: wb-pipe-rise 0.9s linear;
  }
}

@keyframes wb-pipe-drop {
  0% {
    top: 0;
    opacity: 1;
  }
  100% {
    top: calc(100% - 5px);
    opacity: 1;
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

  .wb-pipe-chip {
    width: 200px;
    padding: 8px 12px 9px;
  }

  /* small enough that "acknowledge + checkpoint" doesn't wrap */
  .wb-pipe-title {
    font-size: 12px;
  }

  .wb-pipe-sub {
    font-size: 11px;
    line-height: 16px;
  }

  .wb-pipe-chip.is-backfill-chip {
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
