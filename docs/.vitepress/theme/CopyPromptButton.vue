<script setup lang="ts">
// Agent setup panel: copies the setup prompt to the clipboard and swaps the
// copy icon for a check while the confirmation shows.
import { ref } from 'vue';
import prompt from './agent-prompt.txt?raw';

const lines = prompt.trimEnd().split('\n').length;
const state = ref<'idle' | 'copied' | 'failed'>('idle');
let reset: ReturnType<typeof setTimeout> | undefined;

async function copy() {
  try {
    await navigator.clipboard.writeText(prompt);
    state.value = 'copied';
  } catch {
    state.value = 'failed';
  }
  clearTimeout(reset);
  reset = setTimeout(() => (state.value = 'idle'), 2000);
}
</script>

<template>
  <div class="wb-agent">
    <div class="wb-agent-body">
      <div class="wb-agent-label">agent setup</div>
      <div class="wb-agent-title">Using an AI coding agent?</div>
      <div class="wb-agent-sub">
        Copy the setup prompt and paste it into your agent from inside your project. It reads your
        model, and wires Wallaby in.
      </div>
    </div>
    <button
      type="button"
      class="wb-agent-btn"
      :class="{ 'is-copied': state === 'copied' }"
      :aria-label="state === 'copied' ? 'Prompt copied' : 'Copy agent prompt'"
      @click="copy"
    >
      <svg v-if="state === 'copied'" class="wb-agent-icon" viewBox="0 0 16 16" aria-hidden="true">
        <path d="M2.5 8.5l3.5 3.5 7.5-8" />
      </svg>
      <svg v-else class="wb-agent-icon" viewBox="0 0 16 16" aria-hidden="true">
        <rect x="5.5" y="5.5" width="8" height="8" rx="1" />
        <path d="M10.5 5.5v-2a1 1 0 0 0-1-1h-6a1 1 0 0 0-1 1v6a1 1 0 0 0 1 1h2" />
      </svg>
      <span class="wb-agent-btn-text">
        <span :class="{ 'is-hidden': state !== 'idle' }">copy prompt</span>
        <span :class="{ 'is-hidden': state !== 'copied' }">copied</span>
        <span :class="{ 'is-hidden': state !== 'failed' }">copy failed</span>
      </span>
      <span class="wb-agent-meta">{{ lines }} lines</span>
    </button>
  </div>
</template>

<style>
.wb-agent {
  display: flex;
  align-items: center;
  gap: 16px 24px;
  margin: 20px 0 28px;
  padding: 14px 18px 15px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 2px;
  background-color: var(--vp-code-block-bg);
}

.wb-agent-body {
  flex: 1 1 auto;
  min-width: 0;
}

.wb-agent-label {
  margin-bottom: 4px;
  font-family: var(--vp-font-family-mono);
  font-size: 11px;
  line-height: 16px;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--vp-c-text-3);
}

.wb-agent-title {
  font-family: var(--vp-font-family-mono);
  font-size: 15px;
  font-weight: 600;
  line-height: 22px;
  color: var(--vp-c-text-1);
}

.wb-agent-sub {
  margin-top: 4px;
  font-size: 13px;
  line-height: 20px;
  color: var(--vp-c-text-2);
}

.vp-doc .wb-agent-btn {
  display: inline-flex;
  flex: 0 0 auto;
  align-items: center;
  gap: 10px;
  padding: 9px 14px 9px 12px;
  border: 1px solid var(--vp-c-border);
  border-radius: 2px;
  background-color: var(--vp-c-bg-soft);
  color: var(--vp-c-text-1);
  font-family: var(--vp-font-family-mono);
  font-size: 13px;
  font-weight: 600;
  line-height: 18px;
  white-space: nowrap;
  cursor: pointer;
  transition: color 0.25s, border-color 0.25s, box-shadow 0.25s;
}

.vp-doc .wb-agent-btn:hover,
.vp-doc .wb-agent-btn:focus-visible,
.vp-doc .wb-agent-btn.is-copied {
  border-color: var(--vp-c-brand-1);
  color: var(--vp-c-brand-1);
  box-shadow: var(--wb-glow-amber);
}

.wb-agent-icon {
  flex: 0 0 auto;
  width: 16px;
  height: 16px;
  fill: none;
  stroke: currentColor;
  stroke-width: 1.5;
  stroke-linecap: round;
  stroke-linejoin: round;
}

/* Every label occupies the same cell so the button keeps the widest
   label's width and the surrounding text never shifts. */
.wb-agent-btn-text {
  display: grid;
}

.wb-agent-btn-text > span {
  grid-area: 1 / 1;
}

.wb-agent-btn-text > .is-hidden {
  visibility: hidden;
}

.wb-agent-meta {
  padding-left: 10px;
  border-left: 1px solid var(--vp-c-divider);
  font-weight: 400;
  color: var(--vp-c-text-3);
}

.wb-agent-btn.is-copied .wb-agent-meta {
  color: var(--wb-accent-blue);
}

@media (max-width: 640px) {
  .wb-agent {
    flex-direction: column;
    align-items: stretch;
  }

  .vp-doc .wb-agent-btn {
    justify-content: center;
  }
}
</style>
