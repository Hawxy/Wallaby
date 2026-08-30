<script setup lang="ts">
import { ref } from 'vue';
import { withBase } from 'vitepress';
import { lsn, tickLsn } from './lsn';
import { useFlowCycle, type FlowStep } from './flow/useFlowCycle';
import FlowChip from './flow/FlowChip.vue';
import FlowLink from './flow/FlowLink.vue';

// "Choose your configuration" picker: postgres fans out over a bus into
// two lanes - the capture lane (providers box → sinks box) and the
// provision-only lane (external slots → pgoutput consumer) - with the
// setup buttons embedded in the chips. A packet flows down a different
// path each cycle.
const providers = [
  {
    title: 'efcore provider',
    sub: 'relational storage',
    label: 'EF Core Setup →',
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
    title: 'http',
    sub: 'POST to any endpoint',
    label: 'HTTP →',
    link: '/sinks/http',
  },
  {
    title: 'kafka',
    sub: 'event streaming',
    label: 'Kafka →',
    link: '/sinks/kafka',
  },
  {
    title: 'elasticsearch',
    sub: 'search + analytics',
    label: 'Elasticsearch →',
    link: '/sinks/elasticsearch',
  },
  {
    title: 'opensearch',
    sub: 'search + analytics',
    label: 'OpenSearch →',
    link: '/sinks/opensearch',
  },
  {
    title: 'custom',
    sub: 'your own target',
    label: 'Custom Sinks →',
    link: '/sinks/custom',
  },
];

// deliveries rotate across the sinks that have pages (placeholders are skipped)
const liveSinks = sinks.flatMap((s, i) => (s.link ? [i] : []));

// 0/1 = provider path, 2 = external slots path; rotates each cycle
const target = ref(0);
// chip processing the packet (amber): '' | 'p0' | 'p1' | 'ext'
const lit = ref('');
// chip whose data just updated (blue): '' | 'src' | 's<index>' | 'consumer'
const flash = ref('');
// which connector segment the packet is on
const pulse = ref('');

// providers alternate which sink they deliver to across rounds
let round = 0;
let started = false;

// stem (0.5s) → bus toward a lane (0.35s) → drop into the box (0.5s) →
// chip processes (amber ~0.75s) → drop to the lane's destination →
// destination flashes blue as the delivery lands
useFlowCycle({
  kick: 500,
  interval: 3600,
  cycle: (): FlowStep[] => {
    if (started) {
      target.value = (target.value + 1) % 3;
      if (target.value === 0) round += 1;
    }
    started = true;
    const t = target.value;
    const provider = t < 2;
    const sink = liveSinks[(t + round) % liveSinks.length];
    return [
      [0, () => { tickLsn(); flash.value = 'src'; pulse.value = 'stem'; }],
      [400, () => (flash.value = '')],
      [500, () => (pulse.value = provider ? 'bus-left' : 'bus-right')],
      [850, () => (pulse.value = provider ? 'drop-left' : 'drop-right')],
      [1350, () => { pulse.value = ''; lit.value = provider ? 'p' + t : 'ext'; }],
      [2100, () => { lit.value = ''; pulse.value = provider ? 'drop-sinks' : 'drop-consumer'; }],
      // destination holds blue as long as the provider held amber, so
      // the delivery doesn't read as more fleeting than the processing
      [2600, () => { pulse.value = ''; flash.value = provider ? 's' + sink : 'consumer'; }],
      [3350, () => (flash.value = '')],
    ];
  },
});
</script>

<template>
  <div class="wb-config">
    <!-- the source chip and connectors are decoration (fake telemetry);
         the boxes below carry the real content and links -->
    <div class="wb-config-top" aria-hidden="true">
      <FlowChip class="is-src" :flash="flash === 'src'">
        <div class="wb-chip-title">postgres</div>
        <div class="wb-chip-sub">
          wal @ <span class="wb-chip-val">{{ lsn }}</span>
        </div>
      </FlowChip>
      <FlowLink :pulsing="pulse === 'stem'" />
      <div
        class="wb-config-bus"
        :class="{ 'is-left': pulse === 'bus-left', 'is-right': pulse === 'bus-right' }"
      ></div>
    </div>

    <!-- one shared grid so lane rows stay the same height: drops on
         row 1 and 3, providers/external on row 2, sinks/consumer on
         row 4 -->
    <div class="wb-config-grid">
      <FlowLink class="wb-config-drop" :pulsing="pulse === 'drop-left'" aria-hidden="true" />
      <FlowLink class="wb-config-drop" :pulsing="pulse === 'drop-right'" aria-hidden="true" />

      <div class="wb-config-group is-providers">
        <div class="wb-config-group-label">providers</div>
        <div class="wb-config-group-grid">
          <FlowChip
            v-for="(p, i) in providers"
            :key="p.title"
            class="is-option"
            :lit="lit === 'p' + i"
          >
            <div class="wb-chip-title">{{ p.title }}</div>
            <div class="wb-chip-sub">{{ p.sub }}</div>
            <a class="wb-btn" :href="withBase(p.link)">{{ p.label }}</a>
          </FlowChip>
        </div>
      </div>

      <FlowChip class="is-option is-ext" :lit="lit === 'ext'">
        <div class="wb-chip-title">external slots</div>
        <div class="wb-chip-sub">provision publications + slots, no capture</div>
        <a class="wb-btn" :href="withBase('/external-slots')">External Slots →</a>
      </FlowChip>

      <FlowLink class="wb-config-drop" :pulsing="pulse === 'drop-sinks'" aria-hidden="true" />
      <FlowLink class="wb-config-drop" :pulsing="pulse === 'drop-consumer'" aria-hidden="true" />

      <div class="wb-config-group is-sinks">
        <div class="wb-config-group-label">sinks</div>
        <div class="wb-config-group-grid">
          <FlowChip
            v-for="(s, i) in sinks"
            :key="s.title"
            class="is-option"
            :class="{ 'is-soon': !s.link }"
            :flash="flash === 's' + i"
          >
            <div class="wb-chip-title">{{ s.title }}</div>
            <div class="wb-chip-sub">{{ s.sub }}</div>
            <a v-if="s.link" class="wb-btn" :href="withBase(s.link)">{{ s.label }}</a>
          </FlowChip>
        </div>
      </div>

      <FlowChip class="is-option is-consumer" :flash="flash === 'consumer'">
        <div class="wb-chip-title">pgoutput consumer</div>
        <div class="wb-chip-sub">Airbyte / Fivetran / etc</div>
        <div class="wb-chip-sub">reads the slot directly</div>
      </FlowChip>
    </div>
  </div>
</template>

<style scoped>
.wb-config {
  max-width: 680px;
  margin: 32px auto;
  font-family: var(--vp-font-family-mono);
  /* shorter hops than the vertical diagrams */
  --wb-link-travel: 0.5s;
}

.wb-config .wb-link {
  height: 24px;
}

.wb-config-top {
  display: flex;
  flex-direction: column;
  align-items: center;
}

.wb-chip.is-src {
  width: 200px;
}

.wb-chip.is-option {
  display: flex;
  flex-direction: column;
}

.wb-config .wb-chip-sub:last-of-type {
  margin-bottom: 12px;
}

/* embedded buttons: compact form of the global .wb-btn, pinned to the
   chip bottom so siblings line up */
.wb-chip .wb-btn {
  display: block;
  margin-top: auto;
  padding: 5px 10px;
  font-size: 12px;
  text-align: center;
}

/* placeholder chip: dashed outline, muted title, no button */
.wb-chip.is-soon {
  border-style: dashed;
}

.wb-chip.is-soon .wb-chip-title {
  color: var(--vp-c-text-3);
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

.wb-config-drop {
  justify-self: center;
}

@media (prefers-reduced-motion: no-preference) {
  .wb-config-bus.is-left::before {
    animation: wb-config-cross-left 0.35s linear;
  }

  .wb-config-bus.is-right::before {
    animation: wb-config-cross-right 0.35s linear;
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

/* narrow screens: the lanes don't fit side by side - stack the boxes
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

  .wb-chip.is-ext {
    order: 3;
  }

  .wb-chip.is-consumer {
    order: 4;
  }
}
</style>
