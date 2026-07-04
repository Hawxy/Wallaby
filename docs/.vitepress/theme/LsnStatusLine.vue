<script setup lang="ts">
import { ref, onMounted, onUnmounted } from 'vue';

// A fake replication-slot readout under the hero tagline. The LSN ticks
// upward in irregular bursts so it reads as a live WAL position rather
// than a static string. Purely decorative — hidden from assistive tech.
const hi = ref(0x16);
const lo = ref(0xb374d848);
const lsn = ref(format());
let timer: ReturnType<typeof setInterval> | undefined;

function format() {
  return `${hi.value.toString(16).toUpperCase()}/${lo.value
    .toString(16)
    .toUpperCase()
    .padStart(8, '0')}`;
}

function tick() {
  lo.value += 0x100 + Math.floor(Math.random() * 0x4000);
  if (lo.value > 0xffffffff) {
    lo.value -= 0x100000000;
    hi.value += 1;
  }
  lsn.value = format();
}

onMounted(() => {
  if (!window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
    timer = setInterval(tick, 1200);
  }
});

onUnmounted(() => {
  if (timer) clearInterval(timer);
});
</script>

<template>
  <p class="wb-lsn" aria-hidden="true">
    <span class="wb-lsn-marker">►</span> streaming @ LSN
    <span class="wb-lsn-value">{{ lsn }}</span> · slot: wallaby_cdc ·
    <span class="wb-lsn-state">active</span>
  </p>
</template>

<style scoped>
.wb-lsn {
  margin-top: 16px;
  font-family: var(--vp-font-family-mono);
  font-size: 13px;
  line-height: 20px;
  letter-spacing: 0.02em;
  color: var(--vp-c-text-3);
  white-space: nowrap;
}

.wb-lsn-marker {
  color: var(--vp-c-brand-1);
}

/* padStart keeps the hex at a fixed 8 digits, and the mono font keeps
   digit widths equal, so the ticking value never shifts the layout */
.wb-lsn-value {
  color: var(--wb-accent-blue);
}

.wb-lsn-state {
  color: var(--vp-c-brand-1);
}

@media (max-width: 480px) {
  .wb-lsn {
    white-space: normal;
    font-size: 12px;
  }
}
</style>
