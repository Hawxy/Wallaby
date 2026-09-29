<script setup lang="ts">
import { computed, ref, watch } from 'vue';
import { withBase } from 'vitepress';
import { lsn, tickLsn } from './lsn';
import FlowChip from './flow/FlowChip.vue';
import FlowLink from './flow/FlowLink.vue';
import { highlight } from './flow/highlight';
import { installCommands, programCs, providers, sinks } from './flow/setup';
import { phaseTimeline } from './flow/timeline';
import { useTimelineLoop } from './flow/useTimelineLoop';

// "Choose your configuration" picker: postgres fans out over a bus into
// two lanes - the capture lane (providers box → sinks box) and the
// provision-only lane (external slot → pgoutput consumer). The reader
// picks a provider, any sinks and optionally an external slot. Until then
// a packet tours random paths; once something is picked it follows the
// choice (filling any gap at random), and the setup code below the
// diagram is generated from the same choice.

const providerId = ref('');
const sinkIds = ref<string[]>([]);
const externalSlot = ref(false);

// somewhere to deliver or provision
const hasTarget = computed(() => sinkIds.value.length > 0 || externalSlot.value);
const hasChoice = computed(() => !!providerId.value || hasTarget.value);
const provider = computed(() => providers.find(p => p.id === providerId.value));

// setup code needs a provider plus a target
const missing = computed(() => {
  if (!provider.value) return hasTarget.value ? 'a provider' : 'a provider and a sink';
  return hasTarget.value ? '' : 'a sink or the external slot';
});
const choice = computed(() => ({
  provider: provider.value!,
  sinks: sinks.filter(s => sinkIds.value.includes(s.id)),
  externalSlot: externalSlot.value,
}));
const codeBlocks = computed(() => (missing.value ? [] : [
  { label: 'install', lang: 'bash', html: highlight(installCommands(choice.value), 'bash') },
  { label: 'program.cs', lang: 'csharp', html: highlight(programCs(choice.value), 'csharp') },
]));

// --- animation ---------------------------------------------------------

// chip processing the packet (amber): '' | provider id | 'ext'
const lit = ref('');
// chips whose data just updated (blue): 'src', sink ids, 'consumer'
const flash = ref<string[]>([]);
// which connector segment the packet is on
const pulse = ref('');

const root = ref<HTMLElement>();
let round = 0;

// stem → bus toward a lane → drop into the box → chip processes (amber)
// → drop to the lane's destination → destination flashes blue as the
// delivery lands. With both lanes chosen, cycles alternate between them;
// with nothing chosen, one packet in four takes the external lane.
function cycle(first: boolean) {
  lit.value = '';
  flash.value = [];
  pulse.value = '';
  const pick = <T,>(list: T[]) => list[Math.floor(Math.random() * list.length)];
  const lanes = hasChoice.value
    ? [...(providerId.value || sinkIds.value.length ? ['capture'] : []), ...(externalSlot.value ? ['external'] : [])]
    : ['capture', 'capture', 'capture', 'external'];
  const capture = (hasChoice.value ? lanes[round++ % lanes.length] : pick(lanes)) === 'capture';
  const via = providerId.value || pick(providers).id;
  const to = sinkIds.value.length ? sinkIds.value : [pick(sinks).id];
  return phaseTimeline([
    { name: 'lead', ms: first ? 500 : 0 },
    {
      name: 'source',
      ms: 500,
      run: () => {
        tickLsn();
        flash.value = ['src'];
        pulse.value = 'stem';
      },
      at: [[400, () => (flash.value = [])]],
    },
    { name: 'bus', ms: 350, run: () => (pulse.value = capture ? 'bus-left' : 'bus-right') },
    { name: 'drop', ms: 500, run: () => (pulse.value = capture ? 'drop-left' : 'drop-right') },
    {
      name: 'process',
      ms: 750,
      run: () => {
        pulse.value = '';
        lit.value = capture ? via : 'ext';
      },
    },
    {
      name: 'deliver',
      ms: 500,
      run: () => {
        lit.value = '';
        pulse.value = capture ? 'drop-sinks' : 'drop-consumer';
      },
    },
    // destination holds blue as long as the provider held amber, so the
    // delivery doesn't read as more fleeting than the processing
    {
      name: 'land',
      ms: 750,
      run: () => {
        pulse.value = '';
        flash.value = capture ? to : ['consumer'];
      },
    },
    { name: 'rest', ms: 250, run: () => (flash.value = []) },
  ]);
}

const loop = useTimelineLoop(root, cycle);

// a new choice restarts the packet on the new path
watch([providerId, sinkIds, externalSlot], () => loop.reset());
</script>

<template>
  <div ref="root" class="wb-config">
    <!-- the source chip and connectors are decoration (fake telemetry);
         the options below carry the real content and links -->
    <div class="wb-config-top" aria-hidden="true">
      <FlowChip class="is-src" :flash="flash.includes('src')">
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

      <div class="wb-config-group is-providers" role="radiogroup" aria-label="storage provider">
        <div class="wb-config-group-label">providers · pick one</div>
        <div class="wb-config-group-grid">
          <FlowChip
            v-for="p in providers"
            :key="p.id"
            class="wb-pick"
            :class="{ 'is-selected': providerId === p.id, 'is-muted': providerId && providerId !== p.id }"
            :lit="lit === p.id"
          >
            <input
              v-model="providerId"
              class="wb-pick-input"
              type="radio"
              name="wb-config-provider"
              :value="p.id"
              :aria-label="`${p.title}: ${p.sub}`"
            >
            <div class="wb-pick-head">
              <span class="wb-chip-title">{{ p.title }}</span>
              <i class="wb-pick-box" aria-hidden="true" />
            </div>
            <div class="wb-chip-sub">{{ p.sub }}</div>
            <a class="wb-pick-link" :href="withBase(p.link)">docs →</a>
          </FlowChip>
        </div>
      </div>

      <FlowChip
        class="wb-pick is-ext"
        :class="{ 'is-selected': externalSlot, 'is-muted': hasChoice && !externalSlot }"
        :lit="lit === 'ext'"
      >
        <input
          v-model="externalSlot"
          class="wb-pick-input"
          type="checkbox"
          aria-label="external slot: provision publications and slots, no capture"
        >
        <div class="wb-pick-head">
          <span class="wb-chip-title">external slot</span>
          <i class="wb-pick-box" aria-hidden="true" />
        </div>
        <div class="wb-chip-sub">provision publications + slots, no capture</div>
        <a class="wb-pick-link" :href="withBase('/external-slots')">docs →</a>
      </FlowChip>

      <FlowLink class="wb-config-drop" :pulsing="pulse === 'drop-sinks'" aria-hidden="true" />
      <FlowLink class="wb-config-drop" :pulsing="pulse === 'drop-consumer'" aria-hidden="true" />

      <div class="wb-config-group is-sinks" role="group" aria-label="sinks">
        <div class="wb-config-group-label">sinks · pick any</div>
        <div class="wb-config-group-grid">
          <FlowChip
            v-for="s in sinks"
            :key="s.id"
            class="wb-pick"
            :class="{ 'is-selected': sinkIds.includes(s.id), 'is-muted': sinkIds.length && !sinkIds.includes(s.id) }"
            :flash="flash.includes(s.id)"
          >
            <input
              v-model="sinkIds"
              class="wb-pick-input"
              type="checkbox"
              :value="s.id"
              :aria-label="`${s.title}: ${s.sub}`"
            >
            <div class="wb-pick-head">
              <span class="wb-chip-title">{{ s.title }}</span>
              <i class="wb-pick-box" aria-hidden="true" />
            </div>
            <div class="wb-chip-sub">{{ s.sub }}</div>
            <a class="wb-pick-link" :href="withBase(s.link)">docs →</a>
          </FlowChip>
        </div>
      </div>

      <FlowChip
        class="is-consumer"
        :class="{ 'is-muted': hasChoice && !externalSlot }"
        :flash="flash.includes('consumer')"
      >
        <div class="wb-chip-title">pgoutput consumer</div>
        <div class="wb-chip-sub">Airbyte / Fivetran / etc</div>
        <div class="wb-chip-sub">reads the slot directly</div>
      </FlowChip>
    </div>

    <div v-if="missing" class="wb-config-hint">
      <span class="wb-config-prompt">$</span> pick {{ missing }} to generate the setup
    </div>
    <div v-else class="wb-config-code">
      <template v-for="b in codeBlocks" :key="b.label">
        <div class="wb-config-group-label">{{ b.label }}</div>
        <div :class="'language-' + b.lang">
          <button title="Copy code" data-copied="Copied" class="copy"></button>
          <span class="lang">{{ b.lang }}</span>
          <pre
            class="shiki shiki-themes github-light-high-contrast ayu-dark"
            style="--shiki-light:#0e1116;--shiki-dark:#bfbdb6;--shiki-light-bg:#ffffff;--shiki-dark-bg:#0d1017;"
            tabindex="0"
            dir="ltr"
          ><code v-html="b.html"></code></pre>
        </div>
      </template>
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

/* --- options -----------------------------------------------------------
   Each option is a chip with a transparent input stretched over it, so
   the whole chip toggles; the docs link sits above the input. */

.wb-pick {
  position: relative;
  display: flex;
  flex-direction: column;
  padding-inline: 12px;
}

.wb-pick,
.wb-chip.is-consumer {
  transition: border-color var(--wb-chip-decay, 0.4s), opacity 0.2s;
}

/* once a group has a pick, the rest of it steps back */
.wb-chip.is-muted {
  opacity: 0.6;
}

.wb-chip.is-muted:hover {
  opacity: 1;
}

.wb-pick.is-selected:not(.is-lit):not(.is-flash) {
  border-color: color-mix(in srgb, var(--vp-c-brand-1) 55%, var(--vp-c-divider));
}

.wb-pick:has(.wb-pick-input:focus-visible) {
  outline: 2px solid var(--vp-c-brand-1);
  outline-offset: 2px;
}

.wb-pick-input {
  position: absolute;
  inset: 0;
  width: 100%;
  height: 100%;
  margin: 0;
  opacity: 0;
  cursor: pointer;
}

.wb-pick-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 8px;
}

/* the checkbox: an outlined square that fills with a packet when chosen */
.wb-pick-box {
  flex-shrink: 0;
  display: grid;
  place-items: center;
  width: 11px;
  height: 11px;
  border: 1px solid var(--vp-c-border);
}

.wb-pick.is-selected .wb-pick-box {
  border-color: var(--vp-c-brand-1);
}

.wb-pick.is-selected .wb-pick-box::after {
  content: '';
  width: 5px;
  height: 5px;
  background-color: var(--vp-c-brand-1);
}

.wb-pick-link {
  position: relative;
  z-index: 1;
  align-self: flex-start;
  margin-top: auto;
  padding-top: 10px;
  font-size: 12px;
  line-height: 18px;
  color: var(--wb-accent-blue);
  text-decoration: none;
}

.wb-pick-link:hover {
  text-decoration: underline;
}

/* the external slot's reader, not an option itself */
.wb-chip.is-consumer {
  align-self: start;
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
  line-height: 16px;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--vp-c-text-3);
}

.wb-config-group-grid {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 12px;
}

.is-providers .wb-config-group-grid {
  grid-template-columns: repeat(3, 1fr);
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

/* --- generated setup ------------------------------------------------- */

.wb-config-code {
  margin-top: 24px;
}

.wb-config-hint {
  margin-top: 24px;
  padding: 12px 16px;
  border: 1px dashed var(--vp-c-divider);
  border-radius: 2px;
  font-size: 12px;
  line-height: 18px;
  color: var(--vp-c-text-3);
}

.wb-config-prompt {
  margin-right: 6px;
  color: var(--vp-c-brand-1);
}

.wb-config-code .wb-config-group-label {
  margin: 16px 0 -8px;
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

  .wb-config-grid {
    grid-template-columns: 1fr;
    gap: 12px;
  }

  .wb-config-group-grid,
  .is-providers .wb-config-group-grid {
    grid-template-columns: 1fr;
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
