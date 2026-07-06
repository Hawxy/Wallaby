import { onMounted, onUnmounted } from 'vue';

export type FlowStep = [ms: number, action: () => void];

// Shared timer scaffolding for the pipeline diagrams: a short kick so
// the first packet doesn't wait out a full interval, a steady interval
// after, per-cycle step timeouts, and cleanup on unmount. Under reduced
// motion no timers start, so the diagrams hold their static state.
export function useFlowCycle(opts: {
  kick: number;
  interval: number;
  cycle: () => FlowStep[];
}) {
  let kickTimer: ReturnType<typeof setTimeout> | undefined;
  let cycleTimer: ReturnType<typeof setInterval> | undefined;
  let stepTimers: ReturnType<typeof setTimeout>[] = [];

  function run() {
    stepTimers = opts.cycle().map(([ms, fn]) => setTimeout(fn, ms));
  }

  onMounted(() => {
    if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) return;
    kickTimer = setTimeout(run, opts.kick);
    cycleTimer = setInterval(run, opts.interval);
  });

  onUnmounted(() => {
    if (kickTimer) clearTimeout(kickTimer);
    if (cycleTimer) clearInterval(cycleTimer);
    stepTimers.forEach(clearTimeout);
  });
}
