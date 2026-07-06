<script setup lang="ts">
// Shared chip for the pipeline diagrams: a bordered panel that lights
// amber while it "processes" a packet (lit) and flashes blue when its
// data updates (flash). Content is slotted; use .wb-chip-title /
// .wb-chip-sub / .wb-chip-val for the shared typography.
defineProps<{
  lit?: boolean;
  flash?: boolean;
}>();
</script>

<template>
  <div class="wb-chip" :class="{ 'is-lit': lit, 'is-flash': flash }">
    <slot />
  </div>
</template>

<style>
.wb-chip {
  padding: 10px 16px 11px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 2px;
  background-color: var(--vp-code-block-bg);
  /* light up fast (the state rules below), decay lazily on the way
     back to gray; --wb-chip-decay lets a diagram slow the decay */
  transition: border-color var(--wb-chip-decay, 0.4s);
}

.wb-chip.is-lit {
  border-color: var(--vp-c-brand-1);
  transition: border-color 0.2s;
}

.wb-chip.is-flash {
  border-color: var(--wb-accent-blue);
  transition: border-color 0.2s;
}

.wb-chip-title {
  font-size: 14px;
  /* explicit: the .vp-doc 28px line-height would bloat the chips */
  line-height: 20px;
  color: var(--vp-c-text-1);
}

.wb-chip-sub {
  margin-top: 2px;
  font-size: 12px;
  line-height: 18px;
  color: var(--vp-c-text-3);
}

.wb-chip-val {
  color: var(--wb-accent-blue);
}
</style>
