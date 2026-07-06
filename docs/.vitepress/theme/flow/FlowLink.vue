<script setup lang="ts">
// Shared vertical connector: a hairline with a square packet dot that
// travels down it while pulsing. Amber by default, blue when the
// packet isn't a live WAL change (backfill). Height is overridden
// per diagram; travel speed via --wb-link-travel.
defineProps<{
  pulsing?: boolean;
  blue?: boolean;
}>();
</script>

<template>
  <div class="wb-link" :class="{ 'is-pulsing': pulsing, 'is-blue': blue }">
    <slot />
  </div>
</template>

<style>
.wb-link {
  position: relative;
  width: 1px;
  height: 28px;
  background-color: var(--vp-c-divider);
}

.wb-link::before {
  content: '';
  position: absolute;
  top: 0;
  left: -2px;
  width: 5px;
  height: 5px;
  background-color: var(--vp-c-brand-1);
  opacity: 0;
}

.wb-link.is-blue::before {
  background-color: var(--wb-accent-blue);
}

@media (prefers-reduced-motion: no-preference) {
  .wb-link.is-pulsing::before {
    animation: wb-link-drop var(--wb-link-travel, 0.6s) linear;
  }
}

@keyframes wb-link-drop {
  0% {
    top: 0;
    opacity: 1;
  }
  100% {
    top: calc(100% - 5px);
    opacity: 1;
  }
}
</style>
